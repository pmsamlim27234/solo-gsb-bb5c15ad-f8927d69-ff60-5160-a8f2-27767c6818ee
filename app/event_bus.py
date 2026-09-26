"""进程内事件总线：写事务提交后唤醒 SSE 订阅者。

部署固定单 worker（见 Dockerfile 的 ``--workers 1``），所有写请求与事件流
连接都在同一进程、同一事件循环内，因此无需跨进程发布：引擎在写事务**提交
之后**调用 :meth:`EventBus.publish` 投一个唤醒信号，订阅者收到后重新查询
``events`` 表即可看到全部已提交事件。信号只负责“及时唤醒”，不携带数据——
即使信号丢失，订阅者也有定时轮询兜底，因此队列采用无界队列、发布永不阻塞
写路径（极端积压时多占的也只是一次性的小整数信号）。
"""
from __future__ import annotations

import asyncio
from typing import Any


class EventBus:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[Any]] = set()

    def subscribe(self) -> asyncio.Queue[Any]:
        """登记一个订阅者队列；调用方负责在断连时 :meth:`unsubscribe`。"""
        queue: asyncio.Queue[Any] = asyncio.Queue()
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[Any]) -> None:
        self._subscribers.discard(queue)

    def publish(self) -> None:
        """向全部在线订阅者投唤醒信号（非阻塞；信号可合并，丢失也有轮询兜底）。"""
        for queue in self._subscribers:
            queue.put_nowait(1)


event_bus = EventBus()
