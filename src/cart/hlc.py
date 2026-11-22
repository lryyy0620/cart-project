"""混合逻辑时钟（Hybrid Logical Clock, HLC）。

为什么不能只用物理时钟：
- 各机器的 NTP 同步存在漂移，误差可达数十毫秒甚至更多；
- 运维改时间、虚拟机迁移、闰秒、NTP 回拨都会让物理时钟**倒退**；
- 物理时钟无法表达因果：A 在 B 之前发生，但 A 的时钟可能读数更大。

HLC 把物理时间和逻辑计数结合起来：物理部分尽量贴近真实时间（便于人读），
逻辑部分保证单调不回退，从而在任何时钟环境下都保持
``a happens-before b => a.timestamp < b.timestamp``。

作业里"系统时间突然倒退一分钟"这个故障，就是用来暴露 LWW 直接依赖物理时钟的缺陷。
"""

from __future__ import annotations

import time
from dataclasses import dataclass

__all__ = ["Timestamp", "Clock"]


@dataclass(frozen=True, order=True)
class Timestamp:
    """(物理毫秒, 逻辑计数, 节点名)，字典序比较即因果序的一个保守近似。"""

    physical: int
    logical: int
    node: str

    def __str__(self) -> str:
        return f"{self.physical}.{self.logical}@{self.node}"

    def to_dict(self) -> dict[str, object]:
        return {"p": self.physical, "l": self.logical, "n": self.node}

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "Timestamp":
        return cls(int(data["p"]), int(data["l"]), str(data["n"]))


class Clock:
    """带可注入偏移量的 HLC，方便在实验里模拟"时钟倒退"。"""

    def __init__(self, node: str, offset_ms: int = 0) -> None:
        self.node = node
        self.offset_ms = offset_ms
        self._physical = 0
        self._logical = 0

    # -- 可注入的时间源 ---------------------------------------------------- #

    def set_offset(self, offset_ms: int) -> None:
        """把本地时钟整体拨快/拨慢（负数即倒退）。"""
        self.offset_ms = offset_ms

    def wall_ms(self) -> int:
        return int(time.time() * 1000) + self.offset_ms

    # -- HLC 算法 ---------------------------------------------------------- #

    def now(self) -> Timestamp:
        wall = self.wall_ms()
        if wall > self._physical:
            self._physical, self._logical = wall, 0
        else:
            # 物理时间没前进（或倒退了）：靠逻辑计数维持单调
            self._logical += 1
        return Timestamp(self._physical, self._logical, self.node)

    def observe(self, remote: Timestamp) -> Timestamp:
        """收到远端时间戳后推进本地时钟，保证之后产生的时间戳一定更大。"""
        wall = self.wall_ms()
        physical = max(self._physical, remote.physical, wall)
        if physical == self._physical == remote.physical:
            logical = max(self._logical, remote.logical) + 1
        elif physical == self._physical:
            logical = self._logical + 1
        elif physical == remote.physical:
            logical = remote.logical + 1
        else:
            logical = 0
        self._physical, self._logical = physical, logical
        return Timestamp(physical, logical, self.node)

    def __str__(self) -> str:
        return f"Clock({self.node}, offset={self.offset_ms}ms)"
