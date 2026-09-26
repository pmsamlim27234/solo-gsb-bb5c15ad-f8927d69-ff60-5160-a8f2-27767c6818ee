"""SSE 事件订阅端到端验证（真实 uvicorn 服务器 + httpx 网络流）。"""
import asyncio
import contextlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time

import httpx

PASS = 0
FAIL = 0


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS {name}", flush=True)
    else:
        FAIL += 1
        print(f"  FAIL {name} {detail}", flush=True)


def parse_frames(lines):
    frames = []
    cur = {}
    for line in lines:
        if line == "":
            if cur:
                frames.append(cur)
                cur = {}
            continue
        if line.startswith(":"):
            frames.append({"comment": line[1:].strip()})
        elif line.startswith("id:"):
            cur["id"] = line[3:].strip()
        elif line.startswith("event:"):
            cur["event"] = line[6:].strip()
        elif line.startswith("data:"):
            cur["data"] = json.loads(line[5:].strip())
    if cur:
        frames.append(cur)
    return frames


class StreamReader:
    def __init__(self, client, path):
        self.client = client
        self.path = path
        self.cm = None
        self.resp = None
        self.task = None
        self.queue: asyncio.Queue = asyncio.Queue()

    async def start(self):
        self.cm = self.client.stream("GET", self.path)
        self.resp = await self.cm.__aenter__()
        self.task = asyncio.create_task(self._pump())

    async def _pump(self):
        try:
            async for line in self.resp.aiter_lines():
                await self.queue.put(line)
        except Exception as exc:  # noqa: BLE001
            await self.queue.put(("__error__", repr(exc)))

    async def collect(self, predicate, timeout=8.0):
        lines: list[str] = []
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            try:
                item = await asyncio.wait_for(self.queue.get(),
                                              timeout=max(remaining, 0.05))
            except asyncio.TimeoutError:
                raise AssertionError("等待 SSE 帧超时") from None
            if isinstance(item, tuple) and item[0] == "__error__":
                raise AssertionError(f"流读取异常: {item[1]}")
            lines.append(item)
            frames = parse_frames(lines)
            if predicate(frames):
                return frames

    async def close(self):
        if self.cm is not None:
            with contextlib.suppress(Exception):
                await self.cm.__aexit__(None, None, None)
        if self.task is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.task, timeout=2)


def is_live(frames):
    return any(f.get("comment", "").startswith("live") for f in frames)


async def post_order(client, token):
    r = await client.post(
        "/orders",
        headers={"sim-session-token": token, "Content-Type": "application/json"},
        json={"sku": "A-1", "quantity": 2})
    assert r.status_code in (200, 201), r.text
    return r


async def global_max_event_id(client):
    mx = 0
    for tok in ["t1", "t2", "t3", "t4", "t5", "t6"]:
        rows = (await client.get(
            f"/__sessions/{tok}/events?limit=500")).json().get("events", [])
        if rows:
            mx = max(mx, max(r["id"] for r in rows))
    return mx


async def main(base):
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        # 等待健康检查
        for _ in range(50):
            try:
                if (await client.get("/__health")).status_code == 200:
                    break
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2)

        # 预置 5 个事件：t1 下单/支付/发货/确认 + t2 下单
        await post_order(client, "t1")
        await client.post("/orders/1000/pay",
                          headers={"sim-session-token": "t1",
                                   "Content-Type": "application/json"},
                          json={"channel": "alipay"})
        await client.post("/orders/1000/ship",
                          headers={"sim-session-token": "t1",
                                   "Content-Type": "application/json"},
                          json={"carrier": "sf"})
        await client.post("/orders/1000/confirm",
                          headers={"sim-session-token": "t1",
                                   "Content-Type": "application/json"})
        await post_order(client, "t2")

        history = (await client.get("/__sessions/t1/events?limit=50")).json()["events"]
        check("预置事件落库", len(history) == 4, history)
        id_first, id_last = history[-1]["id"], history[0]["id"]
        check("事件 id 全局递增", id_last == id_first + 3, (id_first, id_last))

        # 1) after_id=0：全量分批补发（batch=2），升序、不重不漏
        reader = StreamReader(client, "/__events/stream?after_id=0")
        await reader.start()
        check("SSE 内容类型",
              reader.resp.headers["content-type"].startswith("text/event-stream"))
        frames = await reader.collect(is_live)
        events = [f for f in frames if f.get("event") == "sim-event"]
        check("补发全部 5 个事件", len(events) == 5, len(events))
        ids = [f["data"]["id"] for f in events]
        check("补发按 id 升序且无重复", ids == sorted(ids) and len(set(ids)) == 5, ids)
        e = events[0]["data"]
        required = {"id", "token", "scenario", "revision", "method", "path", "route",
                    "rule", "from_state", "to_state", "status_code", "created_at"}
        check("事件字段完整", required <= set(e), sorted(set(e)))
        check("首个事件 init->created",
              e["from_state"] == "init" and e["to_state"] == "created", e)
        check("SSE id 与 data.id 一致",
              all(int(f["id"]) == f["data"]["id"] for f in events))
        live_cursor = int(
            [f for f in frames if f.get("comment", "").startswith("live")][0]
            ["comment"].split("=")[1])
        check("live 游标为最新 id", live_cursor == ids[-1], (live_cursor, ids[-1]))

        # 2) 补发追平后新提交的业务请求被实时推送，且与补发末尾 id 连续
        await post_order(client, "t3")
        frames = await reader.collect(
            lambda fs: any(f.get("data", {}).get("token") == "t3" for f in fs))
        new_events = [f["data"] for f in frames if f.get("event") == "sim-event"]
        check("实时收到 t3 事件",
              len(new_events) == 1 and new_events[0]["token"] == "t3", new_events)
        check("实时事件 id 紧接补发末尾",
              new_events[0]["id"] == live_cursor + 1,
              (new_events[0]["id"], live_cursor))

        # 3) 超时迁移事件也推送
        await client.post("/__sessions/t2/advance",
                          headers={"Content-Type": "application/json"},
                          json={"delta_ms": 1800000})
        frames = await reader.collect(
            lambda fs: any(f.get("data", {}).get("method") == "ADVANCE" for f in fs))
        adv = [f["data"] for f in frames if f.get("event") == "sim-event"
               and f["data"]["method"] == "ADVANCE"]
        check("收到超时迁移事件", len(adv) == 1, adv)
        check("超时事件字段",
              adv[0]["route"] == "@timeout"
              and adv[0]["rule"] == "timeout:created"
              and adv[0]["from_state"] == "created"
              and adv[0]["to_state"] == "cancelled"
              and adv[0]["status_code"] is None, adv[0])

        # 4) 空闲心跳
        frames = await reader.collect(
            lambda fs: any(f.get("comment") == "heartbeat" for f in fs), timeout=5.0)
        check("空闲发送心跳", any(f.get("comment") == "heartbeat" for f in frames))
        await reader.close()

        # 5) token 过滤：历史只收 t1，实时只收 t1
        reader = StreamReader(client, "/__events/stream?after_id=0&token=t1")
        await reader.start()
        frames = await reader.collect(is_live)
        events = [f["data"] for f in frames if f.get("event") == "sim-event"]
        check("token 过滤只收 t1 历史（4 条）",
              len(events) == 4 and all(x["token"] == "t1" for x in events),
              [(x["id"], x["token"]) for x in events])
        await post_order(client, "t6")  # 其他 token 的新事件，应被过滤
        await client.get("/orders/1", headers={"sim-session-token": "t1"})  # 状态不变也产生事件
        frames = await reader.collect(
            lambda fs: any(f.get("data", {}).get("route") == "get-order"
                          for f in fs if f.get("event") == "sim-event"))
        live_evs = [f["data"] for f in frames if f.get("event") == "sim-event"]
        check("token 过滤实时只收 t1", all(x["token"] == "t1" for x in live_evs),
              [(x["id"], x["token"]) for x in live_evs])
        await reader.close()

        # 6) Last-Event-ID 头优先于 after_id；非法/越界游标 422
        #    （头=0 合法时返回的是不关闭的 SSE 流，需以流式方式打开后立即关闭）
        async with client.stream("GET", "/__events/stream?after_id=999999",
                                 headers={"Last-Event-ID": "0"}) as r:
            check("头优先于 query（不报 422）", r.status_code == 200, r.status_code)
        r = await client.get("/__events/stream", headers={"Last-Event-ID": "999999"})
        check("头游标越界 422", r.status_code == 422, r.status_code)
        check("422 错误体", r.json()["error"] == "invalid_cursor")
        for bad in ["abc", "-1", "1.5", "0x1"]:
            r = await client.get(f"/__events/stream?after_id={bad}")
            check(f"非法游标 422: {bad!r}", r.status_code == 422, r.status_code)

        # 7) 不传游标：不补发历史，只收连接之后的新事件
        reader = StreamReader(client, "/__events/stream")
        await reader.start()
        frames = await reader.collect(is_live)
        check("无游标连接不补发历史",
              not [f for f in frames if f.get("event") == "sim-event"])
        await post_order(client, "t4")
        frames = await reader.collect(
            lambda fs: any(f.get("data", {}).get("token") == "t4" for f in fs))
        evs = [f["data"] for f in frames if f.get("event") == "sim-event"]
        check("无游标连接收到之后的新事件", len(evs) == 1 and evs[0]["token"] == "t4", evs)
        await reader.close()

        # 8) 断点续传：补发严格大于游标，实时紧接，不漏不重
        all_rows = (await client.get("/__sessions/t1/events?limit=500")).json()["events"]
        t1_ids = sorted(r["id"] for r in all_rows)
        mid = t1_ids[2]
        global_max = await global_max_event_id(client)
        reader = StreamReader(client, f"/__events/stream?after_id={mid}")
        await reader.start()
        frames = await reader.collect(is_live)
        replay_ids = [f["data"]["id"] for f in frames if f.get("event") == "sim-event"]
        expected = list(range(mid + 1, global_max + 1))
        check("续传补发严格大于游标、不漏不重", replay_ids == expected,
              (mid, replay_ids, expected))
        await post_order(client, "t5")
        frames2 = await reader.collect(
            lambda fs: any(f.get("data", {}).get("token") == "t5" for f in fs))
        live_ids = [f["data"]["id"] for f in frames2 if f.get("event") == "sim-event"]
        check("续传后实时事件紧接补发末尾",
              live_ids and live_ids[0] == max(replay_ids) + 1,
              (live_ids[:3], max(replay_ids)))
        await reader.close()

        # 9) 路由不匹配（404，事务整体回滚）不产生事件；
        #    命中兜底规则的 409 属于正常匹配，仍会记录事件（既有语义）
        before_max = await global_max_event_id(client)
        r = await client.get("/ghost/42", headers={"sim-session-token": "t1"})
        check("无路由 404", r.status_code == 404, r.status_code)
        check("404 回滚不产生事件", await global_max_event_id(client) == before_max)
        r = await client.post("/orders/1/pay",
                              headers={"sim-session-token": "t1",
                                       "Content-Type": "application/json"},
                              json={"channel": "alipay"})
        check("已完成会话命中兜底规则 409", r.status_code == 409, r.status_code)
        check("命中规则（含 409）仍记录事件",
              await global_max_event_id(client) == before_max + 1)

        # 10) 补发/实时衔接竞态：连接后立刻并发提交 12 个事件，
        #     全部事件必须恰好收到一次、id 无空洞
        burst_tokens = [f"burst-{i}" for i in range(12)]

        async def open_burst_stream():
            r = StreamReader(client, "/__events/stream?after_id=0")
            await r.start()
            return r

        burst_reader = await open_burst_stream()
        await asyncio.gather(*(post_order(client, tok) for tok in burst_tokens))

        wanted = set(burst_tokens)
        frames = await burst_reader.collect(
            lambda fs: {f.get("data", {}).get("token")
                        for f in fs if f.get("event") == "sim-event"} >= wanted,
            timeout=10)
        burst_ids = sorted(f["data"]["id"] for f in frames
                           if f.get("event") == "sim-event"
                           and f["data"]["token"] in wanted)
        check("突发 12 个事件全部恰好收到一次",
              len(burst_ids) == 12 and burst_ids == list(range(min(burst_ids),
                                                               min(burst_ids) + 12)),
              burst_ids)
        await burst_reader.close()

        # 11) 断连即取消订阅：多次开关后订阅计数归零，服务端不再读取该连接
        for _ in range(3):
            r = StreamReader(client, "/__events/stream?after_id=0")
            await r.start()
            await r.collect(is_live)
            await r.close()
        for _ in range(20):
            subs = (await client.get("/__health")).json().get("event_subscribers")
            if subs == 0:
                break
            await asyncio.sleep(0.1)
        check("断连后订阅计数归零", subs == 0, subs)

    print(f"\n{PASS} passed, {FAIL} failed", flush=True)
    return FAIL


if __name__ == "__main__":
    tmpdir = tempfile.mkdtemp(prefix="sse-test-")
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "SIM_DB_PATH": os.path.join(tmpdir, "test.db"),
        "SIM_SCENARIOS_DIR": "/workspace/scenarios",
        "SIM_SSE_HEARTBEAT_SECONDS": "0.8",
        "SIM_SSE_BATCH_SIZE": "2",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--workers", "1",
         "--log-level", "warning"],
        cwd="/workspace",
        env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        rc = asyncio.run(asyncio.wait_for(main(base), timeout=60))
    finally:
        proc.terminate()
        try:
            out, _ = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
    if rc or "Traceback" in (out or ""):
        print("--- server log ---")
        print(out)
    sys.exit(1 if rc else 0)
