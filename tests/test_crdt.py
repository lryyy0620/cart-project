"""CRDT 语义单元测试：每条都对应作业要求里的一个具体承诺。"""

from __future__ import annotations

from cart.crdt import Cart, Dot, SkuEntry, VersionVector
from cart.replica import Replica


def dot(replica: str, seq: int) -> Dot:
    return Dot(replica, seq)


# --------------------------------------------------------------------------- #
# 基本增删
# --------------------------------------------------------------------------- #


def test_add_then_remove_single_replica() -> None:
    cart = Cart("alice")
    cart.add("apple", 2, dot("r1", 1))
    assert cart.quantity("apple") == 2
    assert cart.contents() == {"apple": 2}

    cart.remove("apple", [dot("r1", 1)])
    assert cart.quantity("apple") == 0
    assert cart.contents() == {}


def test_remove_is_tombstone_and_survives_merge() -> None:
    """删除信息必须留下来，否则会和还没看到删除的副本合并后"复活"。"""
    cart = Cart("alice")
    cart.add("apple", 1, dot("r1", 1))
    cart.remove("apple", [dot("r1", 1)])

    stale = Cart("alice")
    stale.add("apple", 1, dot("r1", 1))   # 还没看到删除的旧副本

    merged = cart.merge_with(stale)
    assert merged.quantity("apple") == 0, "墓碑丢了，删除被旧副本复活"


# --------------------------------------------------------------------------- #
# 并发合并（作业核心）
# --------------------------------------------------------------------------- #


def test_concurrent_adds_on_two_devices_both_survive() -> None:
    """手机加咖啡豆、电脑加燕麦奶 → 合并后两件都在。"""
    phone = Cart("alice")
    phone.add("咖啡豆", 2, dot("phone", 1))

    pc = Cart("alice")
    pc.add("燕麦奶", 1, dot("pc", 1))

    merged = phone.merge_with(pc)
    assert merged.contents() == {"咖啡豆": 2, "燕麦奶": 1}


def test_same_sku_added_on_both_sides_sums() -> None:
    """同一商品两端各加：数量相加，而不是取其中一个。"""
    phone = Cart("alice")
    phone.add("apple", 1, dot("phone", 1))

    pc = Cart("alice")
    pc.add("apple", 2, dot("pc", 1))

    assert phone.merge_with(pc).quantity("apple") == 3


def test_concurrent_add_wins_over_remove() -> None:
    """A 删掉它看见的那一条，B 并发新增一条 —— 新增的那条必须活下来。"""
    phone = Replica("phone")
    pc = Replica("pc")

    first = phone.add("alice", "apple", 1)
    pc.apply(first)                                    # 电脑看到了手机的第一条

    removal = pc.remove("alice", "apple")              # 电脑删掉它看见的那条
    assert removal.dots == (dot("phone", 1),)

    concurrent_add = phone.add("alice", "apple", 2)    # 手机并发又加了一条

    merged = phone.cart("alice").merge_with(pc.cart("alice"))
    assert merged.quantity("apple") == 2, "并发新增被并发删除吃掉了"


# --------------------------------------------------------------------------- #
# 幂等 / 纯函数
# --------------------------------------------------------------------------- #


def test_duplicate_op_application_is_idempotent() -> None:
    replica = Replica("r1")
    op = replica.add("alice", "apple", 1)
    for _ in range(3):
        assert replica.apply(op) is False, "重复投递不应带来新信息"
    assert replica.cart("alice").quantity("apple") == 1
    assert replica.duplicates_ignored == 3


def test_merge_does_not_mutate_operands() -> None:
    left = Cart("alice")
    left.add("apple", 1, dot("r1", 1))
    right = Cart("alice")
    right.add("milk", 2, dot("r2", 1))

    left_before, right_before = left.copy(), right.copy()
    left.merge_with(right)

    assert left == left_before and right == right_before


# --------------------------------------------------------------------------- #
# 版本向量与因果
# --------------------------------------------------------------------------- #


def test_version_vector_merge_and_dominance() -> None:
    a = VersionVector.from_mapping({"r1": 3, "r2": 1})
    b = VersionVector.from_mapping({"r2": 5, "r3": 2})

    merged = a.merge(b)
    assert merged.to_dict() == {"r1": 3, "r2": 5, "r3": 2}
    assert merged.dominates(a) and merged.dominates(b)
    assert not a.dominates(merged)
    assert merged.includes(dot("r3", 2)) and not merged.includes(dot("r3", 3))


def test_happens_before_and_concurrent() -> None:
    """连续两次写是因果相关；两台设备上的写是并发的。"""
    phone = Replica("phone")
    pc = Replica("pc")

    first = phone.add("alice", "apple", 1)
    second = phone.add("alice", "milk", 1)
    remote = pc.add("alice", "coffee", 1)

    assert first.happens_before(second), "同副本的先后写必须有因果序"
    assert not second.happens_before(first)
    assert first.concurrent_with(remote), "两端各自第一次写应当判定为并发"
    assert remote.concurrent_with(second)


def test_sku_entry_rejects_dot_reuse_with_different_content() -> None:
    """同一个 dot 携带不同数量是真正的错误（dot 被复用）；相同内容则只是重复投递。"""
    entry = SkuEntry()
    entry.add(dot("r1", 1), 2)
    entry.add(dot("r1", 1), 2)          # 重复投递，幂等
    assert entry.quantity() == 2
    try:
        entry.add(dot("r1", 1), 5)
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("同一个 dot 携带不同数量应当报错")
