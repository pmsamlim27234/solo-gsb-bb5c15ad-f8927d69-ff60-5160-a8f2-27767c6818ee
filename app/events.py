"""进程内事件总线：写事务提交后唤醒 SSE 订阅者。

单 worker 部署（容器固定 ``--workers 1``）下，发布与订阅同进程即可满足
实时推送。订阅者拿到的只是“有新提交事件”的唤醒信号，事件内容统一按
``events.id`` 全局游标从表中分批拉取——补发与实时推送因此收敛为同一条
基于游标的读取循环，天然不漏发、不重复。

发布时机严格位于写事务**提交之后**：未提交（或回滚）的事件不会被广播，
订阅者绝不会读到尚不存在的 id。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger("simulator.events")


class EventBus:
    def __init__(self, queue_maxsize: int = 1024) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscribers: set[asyncio.Queue[Any]] = set()
        self._queue_maxsize = queue_maxsize

    def subscribe(self) -> asyncio.Queue[Any]:
        """在事件循环线程中登记订阅队列并返回它。

        必须在任何历史/新事件读取**之前**登记：登记之后提交的事务一定有
        唤醒信号入队，衔接处只可能重复确认，不可能漏发。
        """
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            # 正常运行期只有一个事件循环；切换只发生在测试环境中，
            # 旧循环上的队列已随旧循环失效，直接清空
            self._subscribers.clear()
            self._loop = loop
        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self._queue_maxsize)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[Any]) -> None:
        self._subscribers.discard(queue)

    def publish(self, max_id: int) -> None:
        """写事务提交成功后调用（线程安全）。

        投递的载荷是当前 ``events`` 表的**最大 id**（而非事件内容）：订阅者
        各自按自己的游标拉取差异。``call_soon_threadsafe`` 保证唤醒按调用
        顺序（即事务提交顺序）FIFO 投递；满队时丢弃本次信号也不影响正确性
        ——慢消费者会在下一次唤醒或心跳后按 id 重新拉取追平，且写事务
        永远不会被慢订阅者阻塞。
        """
        loop = self._loop
        if loop is None:
            return  # 尚无 SSE 订阅者，无需唤醒
        loop.call_soon_threadsafe(self._dispatch, max_id)

    def _dispatch(self, max_id: int) -> None:
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(max_id)
            except asyncio.QueueFull:
                logger.warning(
                    "SSE 订阅者队列已满（%d），丢弃一次唤醒；"
                    "该订阅者将在下次唤醒或心跳时按游标追平",
                    self._queue_maxsize,
                )

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)


event_bus = EventBus()
