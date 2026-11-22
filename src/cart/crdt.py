"""购物车 CRDT 核心：版本向量 + OR-Set（observed-remove）+ 合并。

对应作业场景 1 的三个关键词：

- CRDT      : 合并算子构成半格（交换、结合、幂等），各副本无需协调即可收敛；
- 版本向量  : 记录"我见过哪些副本的哪些操作"，用来区分并发与先后（happens-before）；
- 冲突合并  : 并发 add 全部保留（并集），remove 只删除它**确实看见过**的 dot。

为什么不用"最后写入者胜"（LWW）：
LWW 在分区期间会让手机端和电脑端的两次添加互相覆盖，直接违反
"不能简单覆盖掉其中一端添加的商品"这一要求。详见 docs/design.md。

数据模型
--------
每个 ``add`` 生成一个全局唯一的 dot ``(副本名, 序号)``，并在 ``adds`` 里记下
该 dot 对应的数量。``remove`` 不删除数据，而是把"当时看得见的 dot"记进
``removed``（墓碑）。于是：

    某 SKU 的可见数量 = Σ{qty | dot ∈ adds 且 dot ∉ removed}

并发场景自然成立：另一端新产生的 dot 不在我的 ``removed`` 里，所以它的添加
不会被我的删除吃掉（add-wins）。重复投递同一个 op 也不会重复计数，因为
dot 相同，合并是幂等的。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Iterator, Literal, Mapping

from .hlc import Timestamp

__all__ = [
    "Dot",
    "VersionVector",
    "SkuEntry",
    "Cart",
    "Op",
    "OpKind",
    "apply_op",
]


# --------------------------------------------------------------------------- #
# 基础类型
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, order=True)
class Dot:
    """一次写操作的全局唯一标识：``副本名`` + 该副本内单调递增的序号。"""

    replica: str
    seq: int

    def __str__(self) -> str:  # 日志里更易读
        return f"{self.replica}#{self.seq}"


@dataclass(frozen=True)
class VersionVector:
    """版本向量：记录"每个副本的最大已知序号"。

    用排序后的元组保存，因此不可变、可哈希、可比较，方便在测试里当作值使用。
    """

    _entries: tuple[tuple[str, int], ...] = ()

    # -- 构造 ------------------------------------------------------------- #

    @classmethod
    def empty(cls) -> "VersionVector":
        return cls(())

    @classmethod
    def from_mapping(cls, data: Mapping[str, int]) -> "VersionVector":
        return cls(tuple(sorted((str(k), int(v)) for k, v in data.items() if int(v) > 0)))

    # -- 读取 ------------------------------------------------------------- #

    def to_dict(self) -> dict[str, int]:
        return dict(self._entries)

    def get(self, replica: str) -> int:
        for name, seq in self._entries:
            if name == replica:
                return seq
        return 0

    def includes(self, dot: Dot) -> bool:
        """该 dot 是否已被本向量覆盖（即"我见过它"）。"""
        return self.get(dot.replica) >= dot.seq

    def dominates(self, other: "VersionVector") -> bool:
        """self 是否不旧于 other（逐副本取 >=）。"""
        return all(self.get(name) >= seq for name, seq in other._entries)

    # -- 更新 ------------------------------------------------------------- #

    def observe(self, dot: Dot) -> "VersionVector":
        """把 dot 记入向量（逐副本取 max）。"""
        if self.includes(dot):
            return self
        data = self.to_dict()
        data[dot.replica] = dot.seq
        return VersionVector.from_mapping(data)

    def merge(self, other: "VersionVector") -> "VersionVector":
        """逐副本取 max，即版本向量的 join。"""
        data = self.to_dict()
        for name, seq in other._entries:
            if seq > data.get(name, 0):
                data[name] = seq
        return VersionVector.from_mapping(data)

    # -- 杂项 ------------------------------------------------------------- #

    def __iter__(self) -> Iterator[tuple[str, int]]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __str__(self) -> str:
        return "{" + ", ".join(f"{name}:{seq}" for name, seq in self._entries) + "}"


# --------------------------------------------------------------------------- #
# 单个 SKU 的 OR-Set
# --------------------------------------------------------------------------- #


@dataclass
class SkuEntry:
    """单个 SKU 的 observed-remove set。

    ``adds``     : dot -> 数量（每次 add 一个 dot）
    ``removed``  : 已被删除的 dot 集合（墓碑，不可再复活）
    """

    adds: dict[Dot, int] = field(default_factory=dict)
    removed: set[Dot] = field(default_factory=set)

    # -- 写入 ------------------------------------------------------------- #

    def add(self, dot: Dot, qty: int) -> None:
        if qty <= 0:
            raise ValueError(f"数量必须为正数，收到 {qty}")
        existing = self.adds.get(dot)
        if existing is not None and existing != qty:
            raise ValueError(f"dot {dot} 已被使用且数量不同：{existing} vs {qty}")
        # 若 dot 已在 removed 里，说明它早就被删了：照常收下，但不可见（乱序到达的正确行为）
        self.adds[dot] = qty

    def remove_dots(self, dots: Iterable[Dot]) -> None:
        """记录墓碑。

        客户端只会带上"它看见过的" dot，所以并发新增（新 dot）不会被误删——
        这就是 add-wins 的来源。这里刻意**不**检查该 dot 是否已在本地 ``adds`` 中：
        真实网络不保证因果投递，删除消息完全可能先于它对应的添加到达。把墓碑
        做成与到达顺序无关，合并才真正在任意顺序下收敛（见 test_convergence.py）。
        """
        self.removed.update(dots)

    # -- 读取 ------------------------------------------------------------- #

    def effective(self) -> dict[Dot, int]:
        """仍然生效的 (dot, 数量)。"""
        return {dot: qty for dot, qty in self.adds.items() if dot not in self.removed}

    def quantity(self) -> int:
        return sum(self.effective().values())

    def visible_dots(self) -> tuple[Dot, ...]:
        """客户端删除时应当携带的 dot：它此刻看得见的那些。"""
        return tuple(sorted(self.effective()))

    # -- 合并 ------------------------------------------------------------- #

    def merge_with(self, other: "SkuEntry") -> "SkuEntry":
        """半格 join：adds 按 dot 求并、removed 求并，纯函数、不改动入参。"""
        merged_adds = dict(self.adds)
        for dot, qty in other.adds.items():
            if dot in merged_adds and merged_adds[dot] != qty:
                # 同一个 dot 必须永远携带同样的内容，否则是系统 bug（dot 被复用了）
                raise ValueError(f"dot {dot} 出现冲突的数量：{merged_adds[dot]} vs {qty}")
            merged_adds[dot] = qty
        return SkuEntry(merged_adds, self.removed | other.removed)

    def copy(self) -> "SkuEntry":
        return SkuEntry(dict(self.adds), set(self.removed))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SkuEntry):
            return NotImplemented
        return self.adds == other.adds and self.removed == other.removed

    def __str__(self) -> str:
        live = ", ".join(f"{dot}={qty}" for dot, qty in sorted(self.effective().items()))
        return f"qty={self.quantity()} [{live or '空'}]"


# --------------------------------------------------------------------------- #
# 一个用户的购物车
# --------------------------------------------------------------------------- #


@dataclass
class Cart:
    """一个用户的购物车：``sku -> SkuEntry``，整体是一个 join-semilattice。"""

    user_id: str
    items: dict[str, SkuEntry] = field(default_factory=dict)
    # 结算版本号：只有"结算主副本"会推进它，用于 CAS（见 Replica.checkout）。
    # 它自身按 max 合并，因此副本之间传播之后取到的一定是已知的最大版本。
    checkout_version: int = 0

    # -- 写入 ------------------------------------------------------------- #

    def add(self, sku: str, qty: int, dot: Dot) -> None:
        self._entry(sku, create=True).add(dot, qty)

    def remove(self, sku: str, dots: Iterable[Dot]) -> None:
        dots = tuple(dots)
        if not dots:
            return
        if sku not in self.items:
            self.items[sku] = SkuEntry()      # 只为保存墓碑，可见数量仍然为 0
        self.items[sku].remove_dots(dots)

    # -- 读取 ------------------------------------------------------------- #

    def quantity(self, sku: str) -> int:
        entry = self.items.get(sku)
        return entry.quantity() if entry else 0

    def visible_dots(self, sku: str) -> tuple[Dot, ...]:
        entry = self.items.get(sku)
        return entry.visible_dots() if entry else ()

    def contents(self) -> dict[str, int]:
        """可读视图：只列出数量 > 0 的 SKU。"""
        return {
            sku: qty
            for sku in sorted(self.items)
            if (qty := self.items[sku].quantity()) > 0
        }

    def total_quantity(self) -> int:
        return sum(self.contents().values())

    def copy(self) -> "Cart":
        return Cart(
            self.user_id,
            {sku: entry.copy() for sku, entry in self.items.items()},
            self.checkout_version,
        )

    # -- 合并 ------------------------------------------------------------- #

    def merge_with(self, other: "Cart") -> "Cart":
        """半格 join：逐 SKU 合并，纯函数。"""
        if self.user_id != other.user_id:
            raise ValueError(f"不能合并不同用户的购物车：{self.user_id} vs {other.user_id}")
        merged = Cart(self.user_id)
        merged.checkout_version = max(self.checkout_version, other.checkout_version)
        for sku in set(self.items) | set(other.items):
            left = self.items.get(sku)
            right = other.items.get(sku)
            if left is None:
                merged.items[sku] = right.copy()  # type: ignore[union-attr]
            elif right is None:
                merged.items[sku] = left.copy()
            else:
                merged.items[sku] = left.merge_with(right)
        return merged

    # -- 杂项 ------------------------------------------------------------- #

    def _entry(self, sku: str, create: bool = False) -> SkuEntry:
        entry = self.items.get(sku)
        if entry is None:
            if not create:
                raise KeyError(sku)
            entry = SkuEntry()
            self.items[sku] = entry
        return entry

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Cart):
            return NotImplemented
        return self.user_id == other.user_id and self.items == other.items

    def __str__(self) -> str:
        pairs = ", ".join(f"{sku}×{qty}" for sku, qty in self.contents().items())
        return f"<{self.user_id}: {pairs or '空购物车'}>"


# --------------------------------------------------------------------------- #
# 操作（可复制、可重放的写请求）
# --------------------------------------------------------------------------- #

OpKind = Literal["add", "remove", "checkout"]


@dataclass(frozen=True)
class Op:
    """一条可重放的写操作。

    携带 ``ctx``（发起方生成该操作时的因果上下文）的目的是能回答
    "这两次写是并发还是有先后"——即 happens-before 关系。
    """

    user_id: str
    kind: OpKind
    sku: str
    qty: int
    dots: tuple[Dot, ...]
    origin: Dot
    ctx: VersionVector = VersionVector.empty()
    request_id: str = ""
    # 发起方的 HLC 时间戳。CRDT 合并**不用**它，只是留给 LWW 影子视图做对照：
    # 同一个操作序列，换成"按时间戳覆盖"会丢什么，页面上能直接看出来。
    ts: Timestamp = Timestamp(0, 0, "")

    # -- 因果 ------------------------------------------------------------- #

    def happens_before(self, other: "Op") -> bool:
        """self 是否因果先于 other（other 的上下文里包含 self 的 origin）。"""
        return other.origin != self.origin and other.ctx.includes(self.origin)

    def concurrent_with(self, other: "Op") -> bool:
        """两次写互不相干（并发），正是需要靠 CRDT 合并而非覆盖的场景。"""
        return not self.happens_before(other) and not other.happens_before(self)

    def __str__(self) -> str:
        return f"{self.kind}({self.sku}×{self.qty})@{self.origin}"


def apply_op(cart: Cart, op: Op) -> None:
    """把一条操作作用到购物车上（幂等：同一 op 重复应用结果不变）。"""
    if op.kind == "add":
        cart.add(op.sku, op.qty, op.origin)
    elif op.kind == "remove":
        cart.remove(op.sku, op.dots)
    elif op.kind == "checkout":
        # 结算版本号按 max 合并：任何副本收到这次结算，之后都会认为版本已经推进
        cart.checkout_version = max(cart.checkout_version, op.qty)
    else:  # pragma: no cover - 类型系统已排除
        raise ValueError(f"未知操作类型：{op.kind}")
