"""会话事件订阅：``GET /__events/stream``（Server-Sent Events）。

游标语义（以 ``events`` 表全局自增 id 为准）：

- 请求头 ``Last-Event-ID`` 与查询参数 ``after_id`` 同传时**以请求头为准**；
- 显式游标：补发 id 严格大于游标的全部已提交事件（分批），随后持续推送
  新提交的事件；
- 未传游标：只推送**连接建立之后**提交的新事件（即从连接时刻的最新事件
  之后开始）；
- 游标非法（非正整数语法、小于 0、大于当前最大 id）返回 422。

补发与实时阶段使用同一条“按游标拉取”的循环：先订阅事件总线，再以连接时
游标为起点读库，每次被提交信号唤醒后继续按游标拉取。订阅登记先于任何
读取，故衔接处不会漏发；读取以严格大于游标的 id 为界，故不会重复。
"""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from ..config import settings
from ..db import get_conn
from ..events import event_bus

router = APIRouter(prefix="/__events", tags=["events"])

# 仅接受十进制非负整数（拒绝 -1、1.5、0x1、空串等）
_CURSOR_RE = re.compile(r"^(0|[1-9][0-9]*)$")

_EVENT_COLUMNS = (
    "id", "token", "scenario", "revision", "method", "path",
    "route", "rule", "from_state", "to_state", "status_code", "created_at",
)


def _max_event_id() -> int:
    row = get_conn().execute("SELECT MAX(id) AS m FROM events").fetchone()
    return int(row["m"] or 0)


def _fetch_events(after_id: int, token: str | None, limit: int) -> list[dict[str, Any]]:
    """按全局 id 升序拉取严格大于 after_id 的一批事件（含 token 过滤）。"""
    sql = (
        "SELECT id, token, scenario, revision, method, path, route, rule,"
        " from_state, to_state, status_code, created_at FROM events WHERE id > ?"
    )
    params: list[Any] = [after_id]
    if token is not None:
        sql += " AND token = ?"
        params.append(token)
    sql += " ORDER BY id ASC LIMIT ?"
    params.append(limit)
    return [dict(row) for row in get_conn().execute(sql, params).fetchall()]


def _sse_event(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"id: {payload['id']}\nevent: sim-event\ndata: {body}\n\n"


@router.get("/stream")
async def stream_events(request: Request) -> StreamingResponse:
    # 1) 解析游标：Last-Event-ID 头优先；after_id 查询参数兜底；
    #    都未传则以连接时刻的最新事件为起点（只收实时新事件）。
    header_cursor = request.headers.get("last-event-id")
    query_cursor = request.query_params.get("after_id")
    raw_cursor = header_cursor if header_cursor is not None else query_cursor

    cursor: int | None
    if raw_cursor is None:
        cursor = None
    else:
        if not _CURSOR_RE.match(raw_cursor.strip() or " "):
            return JSONResponse(status_code=422, content={
                "error": "invalid_cursor",
                "detail": "游标必须是十进制非负整数（Last-Event-ID 请求头或 after_id 查询参数）",
                "cursor": raw_cursor,
            })
        cursor = int(raw_cursor)
        max_id = await asyncio.to_thread(_max_event_id)
        if cursor > max_id:
            return JSONResponse(status_code=422, content={
                "error": "invalid_cursor",
                "detail": f"游标 {cursor} 大于当前最新事件 id {max_id}，无法续传",
                "cursor": cursor,
                "max_event_id": max_id,
            })

    token_raw = request.query_params.get("token")
    token = token_raw if token_raw else None
    batch_size = max(1, settings.sse_batch_size)
    heartbeat = settings.sse_heartbeat_seconds

    async def generate() -> Any:
        # 订阅必须先于首次读库：登记之后提交的事务都会留下唤醒信号
        queue = event_bus.subscribe()
        start_id = cursor
        if start_id is None:
            start_id = await asyncio.to_thread(_max_event_id)
        current = start_id
        try:
            yield ": subscribed\nretry: 3000\n\n"
            catching_up = True
            while True:
                if await request.is_disconnected():
                    break

                # 2) 按游标分批读取已提交事件，读到“本批不足一批”即追平
                rows = await asyncio.to_thread(_fetch_events, current, token, batch_size)
                if rows:
                    for event in rows:
                        yield _sse_event(event)
                    current = rows[-1]["id"]

                if len(rows) == batch_size:
                    continue  # 可能还有历史事件，立即读下一批

                if catching_up:
                    # 3) 补发追平：排空等待期间积累的全部唤醒信号，再做一次
                    #    确认性读取。确认期间若有新提交，其信号尚未被排空
                    #    （FIFO：先排空后提交），不会漏发。
                    drained = False
                    while not queue.empty():
                        queue.get_nowait()
                        drained = True
                    if drained:
                        continue
                    catching_up = False
                    yield f": live cursor={current}\n\n"

                # 4) 实时阶段：等待新提交信号；空闲到点发送心跳
                try:
                    await asyncio.wait_for(queue.get(), timeout=heartbeat)
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            event_bus.unsubscribe(queue)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # 提示反向代理不要缓冲 SSE
    }
    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers=headers)
