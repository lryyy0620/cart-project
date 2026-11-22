"""基于 hypothesis 的性质测试：用随机历史"暴力验证"收敛性。

这类测试比手写用例更有说服力：它随机生成「写 / 删 / 分区 / 恢复 / 同步 / 重复投递」
的任意交错顺序，然后检查两件事——

1. 网络恢复后所有副本收敛到同一状态；
2. 该状态与"按操作集合直接算出来的期望值"完全一致（不丢添加、不重复计数）。

报告里可以把这个思路写成"用性质测试代替人工枚举故障组合"。
"""

from __future__ import annotations

import random

from hypothesis import HealthCheck, given, settings, strategies as st

from cart.cluster import Cluster
from cart.crdt import Cart, Dot, SkuEntry
from cart.network import Network
from cart.replica import Replica

NAMES = ["r1", "r2", "r3"]
SKUS = ["apple", "milk", "coffee", "tea"]
USER = "alice"

FAST = settings(
    deadline=None,
    max_examples=50,
    suppress_health_check=[HealthCheck.too_slow],
)


# --------------------------------------------------------------------------- #
# 期望值预言机：不经过 CRDT，直接从操作集合算答案
# --------------------------------------------------------------------------- #


def expected_contents(ops: list, user: str) -> dict[str, int]:
    removed: set[Dot] = set()
    for op in ops:
        if op.kind == "remove":
            removed |= set(op.dots)

    totals: dict[str, int] = {}
    for op in ops:
        if op.kind == "add" and op.user_id == user and op.origin not in removed:
            totals[op.sku] = totals.get(op.sku, 0) + op.qty
    return {sku: qty for sku, qty in sorted(totals.items()) if qty > 0}


# --------------------------------------------------------------------------- #
# 随机命令
# --------------------------------------------------------------------------- #


@st.composite
def commands(draw):
    kind = draw(st.sampled_from(["add", "remove", "sync", "block", "heal", "duplicate"]))
    if kind == "add":
        return {
            "kind": "add",
            "replica": draw(st.sampled_from(NAMES)),
            "sku": draw(st.sampled_from(SKUS)),
            "qty": draw(st.integers(1, 3)),
        }
    if kind == "remove":
        return {"kind": "remove", "replica": draw(st.sampled_from(NAMES)),
                "sku": draw(st.sampled_from(SKUS))}
    if kind == "block":
        a, b = draw(st.lists(st.sampled_from(NAMES), min_size=2, max_size=2, unique=True))
        return {"kind": "block", "a": a, "b": b}
    if kind == "duplicate":
        return {"kind": "duplicate", "copies": draw(st.integers(1, 3))}
    return {"kind": kind}


def fresh_cluster() -> Cluster:
    return Cluster([Replica(name) for name in NAMES], network=Network(copies=1))


def run(cluster: Cluster, cmd: dict) -> None:
    kind = cmd["kind"]
    if kind == "add":
        cluster.add(cmd["replica"], USER, cmd["sku"], cmd["qty"])
    elif kind == "remove":
        cluster.remove(cmd["replica"], USER, cmd["sku"])
    elif kind == "sync":
        cluster.converge(rounds=2)
    elif kind == "block":
        cluster.network.block(cmd["a"], cmd["b"])
    elif kind == "heal":
        cluster.network.heal_all()
    elif kind == "duplicate":
        cluster.network.copies = cmd["copies"]
    else:  # pragma: no cover
        raise AssertionError(kind)


# --------------------------------------------------------------------------- #
# 性质 1：任意故障交错后都能收敛，且结果等于预言机
# --------------------------------------------------------------------------- #


@given(st.lists(commands(), max_size=20))
@FAST
def test_converges_to_oracle_after_healing(history: list[dict]) -> None:
    cluster = fresh_cluster()
    for cmd in history:
        run(cluster, cmd)

    # 修复网络，并让消息继续重复投递（收敛性不应受重复影响）
    cluster.network.heal_all()
    cluster.converge(rounds=10)

    assert cluster.is_converged(USER), f"副本未收敛：{cluster.views()}"
    assert cluster.read(USER) == expected_contents(cluster.all_ops(), USER)


# --------------------------------------------------------------------------- #
# 性质 2：合并算子满足交换律 / 结合律 / 幂等律
# --------------------------------------------------------------------------- #


@st.composite
def carts(draw):
    cart = Cart(USER)
    for _ in range(draw(st.integers(0, 8))):
        sku = draw(st.sampled_from(SKUS))
        dot = Dot(draw(st.sampled_from(NAMES)), draw(st.integers(1, 5)))
        entry = cart.items.setdefault(sku, SkuEntry())
        # 同一个 dot 必须永远携带同样的内容，否则本身就违反 dot 唯一性约定
        entry.adds.setdefault(dot, dot.seq % 3 + 1)

    for entry in cart.items.values():
        for dot in list(entry.adds):
            if draw(st.booleans()):
                entry.removed.add(dot)
    return cart


@given(carts(), carts())
@FAST
def test_merge_is_commutative_and_idempotent(a: Cart, b: Cart) -> None:
    assert a.merge_with(b) == b.merge_with(a), "交换律不成立"
    assert a.merge_with(a) == a, "幂等律不成立"


@given(carts(), carts(), carts())
@FAST
def test_merge_is_associative(a: Cart, b: Cart, c: Cart) -> None:
    left = a.merge_with(b).merge_with(c)
    right = a.merge_with(b.merge_with(c))
    assert left == right, "结合律不成立"


# --------------------------------------------------------------------------- #
# 性质 3：投递顺序与重复次数都不影响最终结果
# --------------------------------------------------------------------------- #


@given(st.lists(commands(), min_size=1, max_size=10), st.integers(0, 2**31 - 1))
@FAST
def test_apply_order_does_not_change_final_state(history: list[dict], seed: int) -> None:
    """同一批固定操作，打乱"到达顺序"并附带重复投递，最终状态必须相同。

    注意不能反过来打乱"命令历史"——那会改变语义（先加后删 ≠ 先删后加）。
    这里固定操作集合，只打乱它们的到达顺序，才是真正应该成立的收敛性质。
    """
    cluster = fresh_cluster()
    for cmd in history:
        run(cluster, cmd)
    cluster.network.heal_all()
    cluster.converge(rounds=10)

    ops = cluster.all_ops()
    oracle = expected_contents(ops, USER)

    rng = random.Random(seed)
    for _ in range(3):
        arrival = list(ops)
        rng.shuffle(arrival)
        replica = Replica("fresh")
        for op in arrival:
            replica.apply(op)
            if rng.random() < 0.3:
                replica.apply(op)        # 顺便重复投递一份
        assert replica.read(USER) == oracle, f"顺序 {[str(o.origin) for o in arrival]} 下结果不符"
