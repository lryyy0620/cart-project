"""集群编排：把若干副本 + 一条网络组织起来，供演示、场景与测试驱动。

两条路径刻意分开：

- **购物车读写 → AP**：分区期间两端都能写，恢复后靠反熵合并（CRDT）。
- **结算 → CP**：只有该用户的结算主副本能处理，且它必须还能联系到多数派，
  否则宁可拒绝服务也不产生"重复扣款"。
"""

from __future__ import annotations

import hashlib
from typing import Iterable

from .crdt import Cart, Op
from .fault import FaultInjector
from .network import Network
from .replica import CheckoutUnavailable, Receipt, Replica

__all__ = ["Cluster", "owner_name"]


def owner_name(user_id: str, names: list[str]) -> str:
    """一致性哈希：给定成员列表，算出某个用户的结算主副本名。"""
    ordered = sorted(names)
    digest = hashlib.sha1(user_id.encode("utf-8")).hexdigest()
    return ordered[int(digest, 16) % len(ordered)]


class Cluster:
    def __init__(self, replicas: Iterable[Replica], network: Network | None = None) -> None:
        self.replicas: dict[str, Replica] = {r.name: r for r in replicas}
        self.network = network or Network()

    # -- 便捷访问 ---------------------------------------------------------- #

    def replica(self, name: str) -> Replica:
        return self.replicas[name]

    def names(self) -> list[str]:
        return sorted(self.replicas)

    @property
    def quorum(self) -> int:
        return len(self.replicas) // 2 + 1

    def owner_of(self, user_id: str) -> Replica:
        """按一致性哈希选结算主副本；成员变化时会有一部分用户换主（触发迁移）。"""
        return self.replicas[owner_name(user_id, list(self.replicas))]

    # -- 客户端写（打到指定副本，模拟"就近接入"） ---------------------------- #

    def add(self, replica: str, user_id: str, sku: str, qty: int = 1,
            request_id: str | None = None) -> Op | None:
        return self.replicas[replica].add(user_id, sku, qty, request_id=request_id)

    def remove(self, replica: str, user_id: str, sku: str,
               request_id: str | None = None) -> Op | None:
        return self.replicas[replica].remove(user_id, sku, request_id=request_id)

    # -- 结算（CP） -------------------------------------------------------- #

    def reachable_from(self, replica: Replica) -> list[str]:
        """该副本当前还能联系到谁（含自己）。"""
        if replica.down:
            return []
        reachable = [replica.name]
        for name, peer in self.replicas.items():
            if name == replica.name or peer.down:
                continue
            if not self.network.partitioned(replica.name, name):
                reachable.append(name)
        return reachable

    def refresh_from_peers(self, replica: Replica) -> int:
        """读修复：结算前先把该副本缺的操作从能连上的对端拉过来。

        这样"结算凭据"里的商品清单才是完整的——真实系统在收银台前也会做一次
        读修复，否则用户可能为一份不完整的购物车付钱。
        """
        applied = 0
        for name, peer in self.replicas.items():
            if name == replica.name or peer.down or replica.down:
                continue
            if self.network.partitioned(replica.name, name):
                continue
            applied += replica.merge_payload(peer.pull_payload(replica.vv))
        return applied

    def checkout(self, user_id: str, expected_version: int,
                 request_id: str | None = None) -> Receipt:
        owner = self.owner_of(user_id)
        if owner.down:
            raise CheckoutUnavailable(
                f"结算主副本 {owner.name} 不可用；重新选主需要共识，本实现未做（见设计文档）"
            )
        reachable = self.reachable_from(owner)
        if len(reachable) < self.quorum:
            raise CheckoutUnavailable(
                f"主副本 {owner.name} 只能联系到 {len(reachable)}/{len(self.replicas)} 个副本，"
                f"低于多数派 {self.quorum}；为保证『不重复扣款』拒绝结算"
            )
        self.refresh_from_peers(owner)
        return owner.checkout(user_id, expected_version, request_id=request_id)

    # -- 反熵 -------------------------------------------------------------- #

    def sync(self) -> int:
        """一轮反熵：每个可用副本向其它可用副本补发对方缺失的操作。

        这是**进程内驱动**（场景与测试用，同步、确定性强）；
        真实服务里每个副本各自跑 ``api.py`` 的拉模式循环，逻辑一一对应。
        """
        delivered = 0
        for src in self.replicas.values():
            for dst in self.replicas.values():
                if src is dst or src.down or dst.down:
                    continue
                plan = self.network.plan(src.name, dst.name)
                if not plan.delivered:
                    continue
                pending = src.ops_for(dst.vv)
                if not pending:
                    continue
                if plan.delay_ms:
                    FaultInjector.sleep(plan)   # 整批延迟一次，避免演示时无限变慢
                if self.network.injector.policy.reorder_window_ms:
                    self.network.injector.rng.shuffle(pending)   # 打乱到达顺序
                for op in pending:
                    for _ in range(plan.copies):
                        dst.apply(op)
                        delivered += 1
        return delivered

    def converge(self, rounds: int = 6) -> int:
        """反复反熵直到静默（分区仍在的话永远收敛不了——这正是要观察的现象）。"""
        total = 0
        for _ in range(rounds):
            moved = self.sync()
            total += moved
            if moved == 0:
                break
        return total

    # -- 读 ---------------------------------------------------------------- #

    def views(self) -> dict[str, dict[str, dict[str, int]]]:
        return {name: replica.state() for name, replica in sorted(self.replicas.items())}

    def read(self, user_id: str) -> dict[str, int]:
        merged = Cart(user_id)
        for replica in self.replicas.values():
            merged = merged.merge_with(replica.cart(user_id))
        return merged.contents()

    def all_ops(self) -> list[Op]:
        seen: dict[tuple[str, int], Op] = {}
        for replica in self.replicas.values():
            for op in replica.log:
                seen.setdefault((op.origin.replica, op.origin.seq), op)
        return sorted(seen.values(), key=lambda op: (op.origin.replica, op.origin.seq))

    def is_converged(self, user_id: str) -> bool:
        states = {
            name: tuple(sorted(r.read(user_id).items()))
            for name, r in self.replicas.items()
            if not r.down
        }
        return len(set(states.values())) <= 1
