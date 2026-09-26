"""会话事件订阅（Server-Sent Events）：GET /__events/stream。

按 ``events`` 表的全局递增 ``id`` 工作：

- 客户端用 ``Last-Event-ID`` 请求头或 ``after_id`` 查询参数指定游标，
  两者同传时以请求头为准；游标语义为“只收 id 严格大于游标的事件”；
- 未传游标时，游标取**连接建立时**的最大事件 id（先发请求后建连接，
  只看已提交事件），即从连接时的最新事件之后开始；
- 传入游标后先**分批补发**历史事件，再无缝转入实时推送：补发与实时
  共用同一条 ``id > cursor`` 的推进逻辑（在登记订阅之后才开始查），
  补发结束与实时唤醒之间提交的事件由首轮实时查询兜底，不漏发；全程
  以单条游标严格大于推进，不重复；
- ``token`` 可重复传入以同时订阅多个会话，未传则订阅全部会话；
- 每个 SSE 事件的 ``id`` 即事件行 id，浏览器/EventSource 断连重连时会
  自动回传为 ``Last-Event-ID``；空闲时定时发送心跳；服务端检测到断连
  即停止读取数据库并注销订阅；
- 游标不是非负整数（含负数、小数、非数字文本）时返回 422。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from ..db import get_conn
from ..event_bus import event_bus

logger = logging.getLogger("simulator")

router = APIRouter(prefix="/__events", tags=["events"])

# 每批从 events 表补读的行数；批间主动让出事件循环，避免长补发占满循环
BATCH_LIMIT = 1000
# 实时阶段无事件时的查询/唤醒等待间隔；有提交时由事件总线立即唤醒
POLL_INTERVAL_S = 1.0
# 距上次写出超过该时长且无事件时发送心跳注释
HEARTBEAT_INTERVAL_S = 15.0


def _parse_cursor(raw: str) -> int:
    """游标只接受非负整数文本（拒绝小数、负数、非数字）；失败抛 ValueError。"""
    text = raw.strip()
    if not text:
        raise ValueError("游标不能为空")
    # isdigit 天然拒绝负号、小数点、空白、1e3 等
    if not text.isdigit():
        raise ValueError("游标必须是非负整数")
    return int(text)


def _max_event_id() -> int:
    row = get_conn().execute("SELECT COALESCE(MAX(id), 0) AS m FROM events").fetchone()
    return int(row["m"])


def _fetch_batch(cursor: int, tokens: list[str]) -> list[dict[str, Any]]:
    """读取 id 严格大于 cursor 的下一批事件（按全局 id 升序），可选 token 过滤。"""
    if tokens:
        placeholders = ",".join("?" for _ in tokens)
        sql = (
            "SELECT id, token, scenario, revision, method, path, route, rule,"
            " from_state, to_state, status_code, created_at FROM events"
            f" WHERE id > ? AND token IN ({placeholders}) ORDER BY id ASC LIMIT ?"
        )
        params: tuple[Any, ...] = (cursor, *tokens, BATCH_LIMIT)
    else:
        sql = (
            "SELECT id, token, scenario, revision, method, path, route, rule,"
            " from_state, to_state, status_code, created_at FROM events"
            " WHERE id > ? ORDER BY id ASC LIMIT ?"
        )
        params = (cursor, BATCH_LIMIT)
    return [dict(row) for row in get_conn().execute(sql, params).fetchall()]


@router.get("/stream")
async def stream_events(
    request: Request,
    token: list[str] | None = Query(
        default=None,
        description="只订阅指定会话令牌的事件；可重复传入以订阅多个；不传则订阅全部会话",
    ),
    after_id: str | None = Query(
        default=None,
        description="游标：只补发/推送 id 严格大于该值的事件；与 Last-Event-ID 同传时以请求头为准",
    ),
):
    # 1) 游标：请求头优先；空文本视同未传
    header_cursor = request.headers.get("last-event-id")
    raw_cursor = header_cursor if header_cursor is not None and header_cursor.strip() \
        else (after_id if after_id is not None and after_id.strip() else None)
    if raw_cursor is None:
        # 未传游标：以连接建立时的最新事件为界，之后提交的事件才会推送
        start_cursor = _max_event_id()
    else:
        try:
            start_cursor = _parse_cursor(raw_cursor)
        except ValueError:
            return JSONResponse(status_code=422, content={
                "error": "invalid_after_id",
                "detail": "after_id（或 Last-Event-ID 请求头）必须是非负整数",
                "after_id": raw_cursor,
            })

    tokens = token or []

    async def generate(cursor: int = start_cursor):
        # 2) 先登记订阅，再开始查库：登记之后提交的事件一定能被唤醒，
        #    即使唤醒发生在首轮查询之前，信号只是合并为一次提前唤醒，
        #    查询以 id > cursor 严格推进，衔接处不漏发、不重复。
        queue = event_bus.subscribe()
        tokens_desc = "全部会话" if not tokens else f"{len(tokens)} 个会话"
        try:
            yield (
                f": connected after_id={cursor} scope={tokens_desc}\n"
                f": 补发历史事件后将持续推送新事件；空闲 {int(HEARTBEAT_INTERVAL_S)}s 发送一次心跳\n\n"
            )
            last_write = time.monotonic()

            # 3) 补发与实时循环共用同一条：不断读取 id > cursor 的下一批，
            #    查不到完整一批说明已追平，转入等待（总线唤醒/轮询/心跳）。
            while True:
                rows = _fetch_batch(cursor, tokens)
                if rows:
                    frames: list[str] = []
                    for event in rows:
                        cursor = int(event["id"])
                        # ensure_ascii=False：中文状态/令牌直接可读；id 行让客户端
                        # 重连时可自动通过 Last-Event-ID 续传
                        payload = json.dumps(event, ensure_ascii=False)
                        frames.append(f"id: {cursor}\ndata: {payload}\n\n")
                    last_write = time.monotonic()
                    yield "".join(frames)
                    # 还有疑似未读完的历史批次：立即读下一批，但先让出事件循环，
                    # 避免大数据量补发占满循环导致心跳/断连检测长期不触发
                    if len(rows) == BATCH_LIMIT:
                        await asyncio.sleep(0)
                        continue

                # 4) 已追平：等待新提交事件，顺带处理心跳与断连
                if await request.is_disconnected():
                    break
                timeout = max(0.05, HEARTBEAT_INTERVAL_S - (time.monotonic() - last_write))
                try:
                    await asyncio.wait_for(queue.get(), timeout=min(POLL_INTERVAL_S, timeout))
                except TimeoutError:
                    pass
                # 突发提交可能积压多个唤醒信号：一次性排空，避免逐个信号空查
                while not queue.empty():
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                if await request.is_disconnected():
                    break
                if time.monotonic() - last_write >= HEARTBEAT_INTERVAL_S:
                    last_write = time.monotonic()
                    # SSE 注释行：EventSource 不向应用暴露，仅用于保活
                    yield f": heartbeat cursor={cursor}\n\n"
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except Exception:
            # 写出过程中客户端断开会抛 ConnectionError 等，按正常结束处理；
            # 其他异常记 warning，避免静默吞掉真正的实现错误
            if await request.is_disconnected():
                logger.debug("事件流因客户端断连结束（cursor=%d）", cursor)
            else:
                logger.warning("事件流异常结束", exc_info=True)
        finally:
            event_bus.unsubscribe(queue)

    # 代理缓冲会合并心跳：提示中间节点不要缓冲本流
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
