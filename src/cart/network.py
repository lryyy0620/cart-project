"""网络层：拓扑（谁和谁断了）+ 投递计划（延迟、重复、丢弃、乱序）。

这里刻意把"拓扑"和"投递行为"分开：

- **拓扑**决定能不能通（分区）；
- **投递计划**决定通的时候长什么样（慢、重、丢、乱）。

两者都委托给 :class:`cart.fault.FaultInjector`，所以进程内模式与 HTTP 模式
共用同一套注入逻辑，实验结论不会因为换了传输方式而失效。
"""

from __future__ import annotations

from itertools import combinations
from typing import Iterable

from .fault import DeliveryPlan, FaultInjector, FaultPolicy

__all__ = ["Network"]


class Network:
    def __init__(self, copies: int = 1, injector: FaultInjector | None = None,
                 seed: int | None = None) -> None:
        if injector is None:
            injector = FaultInjector(FaultPolicy(duplicate=copies), seed=seed)
        self.injector = injector

    # -- 兼容旧接口：copies ------------------------------------------------ #

    @property
    def copies(self) -> int:
        return self.injector.policy.duplicate

    @copies.setter
    def copies(self, value: int) -> None:
        self.injector.policy.duplicate = max(1, int(value))

    # -- 兼容旧接口：拓扑 -------------------------------------------------- #

    @staticmethod
    def _key(a: str, b: str) -> frozenset[str]:
        return frozenset((a, b))

    def block(self, a: str, b: str) -> None:
        """切断 a 与 b 之间的双向链路。"""
        self.injector.policy.block(a, b)

    def unblock(self, a: str, b: str) -> None:
        self.injector.policy.unblock(a, b)

    def partition(self, *groups: Iterable[str]) -> None:
        """按分组制造网络分区：组内互通，跨组断开。"""
        groups = [set(group) for group in groups]
        for left, right in combinations(groups, 2):
            for a in left:
                for b in right:
                    self.block(a, b)

    def heal_all(self) -> None:
        self.injector.policy.heal()

    def partitioned(self, a: str, b: str) -> bool:
        return self.injector.policy.blocks_link(a, b)

    # -- 投递 -------------------------------------------------------------- #

    def plan(self, sender: str, receiver: str) -> DeliveryPlan:
        return self.injector.plan(sender, receiver)

    # -- 统计 -------------------------------------------------------------- #

    @property
    def dropped(self) -> int:
        return self.injector.stats["dropped"]

    @property
    def delivered(self) -> int:
        return (
            self.injector.stats["sent"]
            - self.injector.stats["dropped"]
            - self.injector.stats["responses_lost"]
        )

    def __str__(self) -> str:
        return (
            f"Network({self.injector.policy.describe()}, "
            f"dropped={self.dropped}, delivered={self.delivered})"
        )
