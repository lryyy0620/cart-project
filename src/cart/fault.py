"""故障注入：把"网络会出什么问题"变成显式、可复现的策略对象。

支持作业要求的九类故障里与网络相关的部分：

============  ==========================================================
延迟          ``latency_ms`` + ``jitter_ms``
乱序          ``reorder_window_ms``：额外随机拖延，打乱到达顺序
重复           ``duplicate``：同一条消息投递 N 份
丢弃          ``drop_rate``
分区          ``block(a, b)``：双向隔离
响应丢失      ``drop_response_rate``：请求到了、改了、但回包丢了
============  ==========================================================

注入点放在**传输层**而不是业务逻辑里，好处是同一套注入代码可以同时驱动
进程内直连和真实 HTTP 两种模式，实验结论不会因为换了传输方式而失效。

说明：使用应用层模拟而非 ``tc netem``，是为了让实验在 Windows / Docker Desktop
上都能一条命令复现，不需要特权容器。真实分区可以在 docker 演示脚本里用
``docker network disconnect`` 复现，两条路径的结果应当一致。
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field

__all__ = ["FaultPolicy", "FaultInjector", "DeliveryPlan"]


@dataclass
class FaultPolicy:
    latency_ms: float = 0.0
    jitter_ms: float = 0.0
    duplicate: int = 1
    drop_rate: float = 0.0
    reorder_window_ms: float = 0.0
    drop_response_rate: float = 0.0
    blocks: set[frozenset[str]] = field(default_factory=set)

    # -- 分区 -------------------------------------------------------------- #

    def block(self, a: str, b: str) -> None:
        self.blocks.add(frozenset((a, b)))

    def unblock(self, a: str, b: str) -> None:
        self.blocks.discard(frozenset((a, b)))

    def heal(self) -> None:
        self.blocks.clear()

    def blocks_link(self, a: str, b: str) -> bool:
        return frozenset((a, b)) in self.blocks

    # -- 展示 -------------------------------------------------------------- #

    def describe(self) -> str:
        bits = []
        if self.latency_ms:
            bits.append(f"延迟 {self.latency_ms:.0f}ms")
        if self.jitter_ms:
            bits.append(f"抖动 ±{self.jitter_ms:.0f}ms")
        if self.reorder_window_ms:
            bits.append(f"乱序窗口 {self.reorder_window_ms:.0f}ms")
        if self.duplicate > 1:
            bits.append(f"每条消息重复 {self.duplicate} 份")
        if self.drop_rate:
            bits.append(f"丢包率 {self.drop_rate:.0%}")
        if self.drop_response_rate:
            bits.append(f"响应丢失率 {self.drop_response_rate:.0%}")
        if self.blocks:
            bits.append(f"{len(self.blocks)} 条链路已断开")
        return "，".join(bits) or "无故障"


@dataclass
class DeliveryPlan:
    copies: int
    delay_ms: float
    dropped: bool

    @property
    def delivered(self) -> bool:
        return not self.dropped and self.copies > 0


class FaultInjector:
    """根据策略给每条消息算一个投递计划，并维护统计。"""

    def __init__(self, policy: FaultPolicy | None = None, seed: int | None = None) -> None:
        self.policy = policy or FaultPolicy()
        self.rng = random.Random(seed)
        self.stats: dict[str, int] = {
            "sent": 0,
            "dropped": 0,
            "duplicated": 0,
            "delayed": 0,
            "responses_lost": 0,
        }

    def plan(self, sender: str, receiver: str) -> DeliveryPlan:
        policy = self.policy
        self.stats["sent"] += 1

        if policy.blocks_link(sender, receiver):
            self.stats["dropped"] += 1
            return DeliveryPlan(copies=0, delay_ms=0.0, dropped=True)

        if policy.drop_rate and self.rng.random() < policy.drop_rate:
            self.stats["dropped"] += 1
            return DeliveryPlan(copies=0, delay_ms=0.0, dropped=True)

        delay = policy.latency_ms
        if policy.jitter_ms:
            delay += self.rng.uniform(-policy.jitter_ms, policy.jitter_ms)
        if policy.reorder_window_ms:
            delay += self.rng.uniform(0, policy.reorder_window_ms)
        delay = max(0.0, delay)
        if delay:
            self.stats["delayed"] += 1

        copies = max(1, policy.duplicate)
        if copies > 1:
            self.stats["duplicated"] += copies - 1

        return DeliveryPlan(copies=copies, delay_ms=delay, dropped=False)

    def should_lose_response(self, sender: str, receiver: str) -> bool:
        """请求已处理，但回包在途中丢失 —— 客户端会重试，考验幂等。"""
        if not self.policy.drop_response_rate:
            return False
        if self.rng.random() < self.policy.drop_response_rate:
            self.stats["responses_lost"] += 1
            return True
        return False

    # -- 等待（同步 / 异步两种用法） --------------------------------------- #

    @staticmethod
    def sleep(plan: DeliveryPlan) -> None:
        if plan.delay_ms:
            time.sleep(plan.delay_ms / 1000)

    @staticmethod
    async def asleep(plan: DeliveryPlan) -> None:
        if plan.delay_ms:
            await asyncio.sleep(plan.delay_ms / 1000)

    def __str__(self) -> str:
        return f"FaultInjector({self.policy.describe()})"
