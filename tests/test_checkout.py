"""结算 CAS 测试：与 CRDT 的 AP 路径形成对照。"""

from __future__ import annotations

import pytest

from cart.cluster import Cluster, owner_name
from cart.replica import CheckoutUnavailable, Replica, VersionConflict


def make_cluster() -> Cluster:
    return Cluster([Replica(name) for name in ("r1", "r2", "r3")])


def test_owner_is_deterministic() -> None:
    assert owner_name("alice", ["r1", "r2", "r3"]) == owner_name("alice", ["r3", "r1", "r2"])
    owners = {owner_name(f"user{i}", ["r1", "r2", "r3"]) for i in range(64)}
    assert owners == {"r1", "r2", "r3"}, "一致性哈希应该把用户分散到所有副本"


def test_checkout_increments_version() -> None:
    cluster = make_cluster()
    cluster.add("r1", "alice", "苹果", 2)
    receipt = cluster.checkout("alice", 0)
    assert receipt.version == 1
    assert receipt.items == {"苹果": 2}, "结算前应做读修复，把购物车拉全"
    assert cluster.read("alice") == {"苹果": 2}


def test_second_checkout_with_stale_version_conflicts() -> None:
    cluster = make_cluster()
    cluster.add("r1", "alice", "苹果", 2)
    cluster.checkout("alice", 0)
    with pytest.raises(VersionConflict) as info:
        cluster.checkout("alice", 0)
    assert info.value.current == 1


def test_checkout_version_propagates_to_all_replicas() -> None:
    cluster = make_cluster()
    cluster.add("r1", "alice", "苹果", 1)
    cluster.checkout("alice", 0)
    cluster.converge()
    versions = {r.cart("alice").checkout_version for r in cluster.replicas.values()}
    assert versions == {1}


def test_checkout_refused_when_owner_is_in_minority() -> None:
    cluster = make_cluster()
    owner = cluster.owner_of("alice").name
    others = [name for name in cluster.names() if name != owner]
    cluster.network.partition({owner}, set(others))

    with pytest.raises(CheckoutUnavailable):
        cluster.checkout("alice", 0)

    # 同一时刻购物车写入仍然可用（AP）
    cluster.add(others[0], "alice", "苹果", 1)
    assert cluster.replica(others[0]).read("alice") == {"苹果": 1}
