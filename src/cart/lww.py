"""LWW（最后写入者胜）购物车：**反例**，用来对照 CRDT 的行为。

它不是作业要求的方案，而是"如果我们偷懒只用时间戳覆盖会怎样"的对照组。
在「系统时间倒退一分钟」和「两端并发添加」两个场景里，LWW 会直接丢掉
用户在另一端加的商品——这正是作业禁止的"简单覆盖掉其中一端"。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .hlc import Timestamp

__all__ = ["LwwItem", "LwwCart"]


@dataclass
class LwwItem:
    qty: int
    ts: Timestamp

    def to_dict(self) -> dict[str, object]:
        return {"qty": self.qty, "ts": self.ts.to_dict()}


@dataclass
class LwwCart:
    user_id: str
    items: dict[str, LwwItem] = field(default_factory=dict)

    def put(self, sku: str, qty: int, ts: Timestamp) -> None:
        current = self.items.get(sku)
        if current is None or ts > current.ts:
            self.items[sku] = LwwItem(qty, ts)

    def remove(self, sku: str, ts: Timestamp) -> None:
        self.put(sku, 0, ts)

    def contents(self) -> dict[str, int]:
        return {sku: item.qty for sku, item in sorted(self.items.items()) if item.qty > 0}

    def merge_with(self, other: "LwwCart") -> "LwwCart":
        merged = LwwCart(self.user_id, dict(self.items))
        for sku, item in other.items.items():
            current = merged.items.get(sku)
            if current is None or item.ts > current.ts:
                merged.items[sku] = item
        return merged

    def __str__(self) -> str:
        pairs = ", ".join(f"{sku}×{qty}" for sku, qty in self.contents().items())
        return f"<LWW {self.user_id}: {pairs or '空'}>"
