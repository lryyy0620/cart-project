"""副本：持有购物车状态、WAL、逻辑时钟与结算版本。

一个副本承担四件事：

1. **本地写**：分配 dot、先写 WAL（fsync）、再改内存、最后才回 ack；
2. **幂等应用**：同一个 ``origin``（或同一个客户端 ``request_id``）来多少次都只生效一次；
3. **反熵**：向对端拉取自己缺的操作，分页进行因此天然可断点续传；
4. **结算 CAS**：作为某个用户的"结算主副本"时，用版本号做 compare-and-swap。

第四条要特别注意：购物车读写走 CRDT（AP，分区期间照常可写），但**下单结算不能**
这么干，因为"余额/库存只扣一次"需要真正的互斥。所以结算路径刻意做成 CP：
只有主副本能结算，且要求它还能联系到多数派。这个对比正是作业"典型提问"里
"为什么不能只用 CRDT 解决一切"的答案。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from .codec import cart_from_json, op_from_json
from .crdt import Cart, Dot, Op, VersionVector, apply_op
from .hlc import Clock, Timestamp
from .lww import LwwCart
from .wal import WriteAheadLog

__all__ = [
    "Replica",
    "Receipt",
    "ReplicaDown",
    "NotOwner",
    "VersionConflict",
    "CheckoutUnavailable",
    "MigrationInterrupted",
]


# --------------------------------------------------------------------------- #
# 异常
# --------------------------------------------------------------------------- #


class ReplicaDown(RuntimeError):
    """目标副本已宕机（或被分区隔离），请求根本到不了。"""


class NotOwner(RuntimeError):
    """结算必须发给该用户的主副本。"""


class CheckoutUnavailable(RuntimeError):
    """主副本联系不到多数派，为了保证结算的强一致性只能拒绝服务（选 CP）。"""


class MigrationInterrupted(RuntimeError):
    """迁移被中途打断（用来模拟"新节点迁到一半宕机"）。"""


class VersionConflict(RuntimeError):
    """CAS 失败：别人先改了。"""

    def __init__(self, expected: int, current: int) -> None:
        super().__init__(f"版本冲突：期望 {expected}，实际 {current}")
        self.expected = expected
        self.current = current


# --------------------------------------------------------------------------- #
# 结算凭据
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Receipt:
    user_id: str
    version: int
    items: dict[str, int]
    total_qty: int
    replica: str
    timestamp: Timestamp

    def to_dict(self) -> dict[str, object]:
        return {
            "user": self.user_id,
            "version": self.version,
            "items": self.items,
            "total_qty": self.total_qty,
            "replica": self.replica,
            "timestamp": str(self.timestamp),
        }


# --------------------------------------------------------------------------- #
# 副本
# --------------------------------------------------------------------------- #


class Replica:
    def __init__(self, name: str, wal: WriteAheadLog | str | None = None,
                 clock: Clock | None = None, recover: bool = True) -> None:
        self.name = name
        self.clock = clock or Clock(name)
        self.wal = wal if isinstance(wal, WriteAheadLog) else (
            WriteAheadLog(wal) if wal is not None else None
        )

        self.carts: dict[str, Cart] = {}
        self.vv = VersionVector.empty()
        self.counter = 0
        self.log: list[Op] = []

        self._applied: set[Dot] = set()
        self._by_request: dict[str, Op] = {}
        # LWW 影子视图：同一批操作按"时间戳覆盖"会得到什么。只为对照，不参与真实状态。
        self.shadow: dict[str, LwwCart] = {}

        self.down = False
        self.duplicates_ignored = 0
        self.rejected_while_down = 0
        self.recovered_ops = 0
        self.checkpoint_vv: VersionVector | None = None

        if recover and self.wal is not None:
            self.recover()

    # -- 恢复 -------------------------------------------------------------- #

    def recover(self) -> int:
        """从 WAL 恢复：先装检查点，再重放其后的操作。"""
        assert self.wal is not None
        loaded = self.wal.scan()
        carts, vv, counter, requests, applied = loaded.base_state()
        self.carts = carts
        self.vv = vv
        self.counter = counter
        self._applied = applied
        self._by_request = {}
        for op in loaded.ops:
            self._apply_only(op)
        self.recovered_ops = len(loaded.ops)
        self.checkpoint_vv = vv if loaded.has_checkpoint else None
        return self.recovered_ops

    # -- 读 ---------------------------------------------------------------- #

    def cart(self, user_id: str) -> Cart:
        cart = self.carts.get(user_id)
        if cart is None:
            cart = Cart(user_id)
            self.carts[user_id] = cart
        return cart

    def read(self, user_id: str) -> dict[str, int]:
        return self.cart(user_id).contents()

    def state(self) -> dict[str, dict[str, int]]:
        return {user: cart.contents() for user, cart in sorted(self.carts.items())}

    # -- 本地写 ------------------------------------------------------------ #

    def set_down(self, value: bool) -> None:
        """模拟进程被 kill 或被网络隔离：之后的写和同步都会被拒绝。"""
        self.down = value

    def _make_op(self, user_id: str, kind: str, sku: str, qty: int,
                 dots: tuple[Dot, ...] = (), request_id: str | None = None) -> Op:
        self.counter += 1
        return Op(
            user_id=user_id,
            kind=kind,  # type: ignore[arg-type]
            sku=sku,
            qty=qty,
            dots=dots,
            origin=Dot(self.name, self.counter),
            ctx=self.vv,
            request_id=request_id or uuid.uuid4().hex,
            ts=self.clock.now(),
        )

    def add(self, user_id: str, sku: str, qty: int = 1,
            request_id: str | None = None) -> Op | None:
        if self.down:
            self.rejected_while_down += 1
            raise ReplicaDown(self.name)
        if request_id and request_id in self._by_request:
            self.duplicates_ignored += 1
            return self._by_request[request_id]
        op = self._make_op(user_id, "add", sku, qty, request_id=request_id)
        self.apply(op)
        return op

    def remove(self, user_id: str, sku: str,
               request_id: str | None = None) -> Op | None:
        if self.down:
            self.rejected_while_down += 1
            raise ReplicaDown(self.name)
        if request_id and request_id in self._by_request:
            self.duplicates_ignored += 1
            return self._by_request[request_id]
        dots = self.cart(user_id).visible_dots(sku)
        op = self._make_op(user_id, "remove", sku, 0, dots, request_id=request_id)
        self.apply(op)
        return op

    def checkout(self, user_id: str, expected_version: int,
                 request_id: str | None = None) -> Receipt:
        """CAS 结算：只有版本号仍然是 ``expected_version`` 时才成交。"""
        if self.down:
            self.rejected_while_down += 1
            raise ReplicaDown(self.name)
        cart = self.cart(user_id)
        current = cart.checkout_version
        if expected_version != current:
            raise VersionConflict(expected_version, current)
        version = current + 1
        op = self._make_op(user_id, "checkout", "", version, request_id=request_id)
        self.apply(op)
        return Receipt(
            user_id=user_id,
            version=version,
            items=cart.contents(),
            total_qty=cart.total_quantity(),
            replica=self.name,
            timestamp=self.clock.now(),
        )

    # -- 应用（本地与远端共用） -------------------------------------------- #

    def apply(self, op: Op, persist: bool = True) -> bool:
        """幂等应用一条操作；返回 True 表示确实带来了新信息。"""
        if op.request_id and op.request_id in self._by_request:
            self.duplicates_ignored += 1
            return False
        if op.origin in self._applied:
            self.duplicates_ignored += 1
            return False
        if self.down:
            self.rejected_while_down += 1
            return False
        if persist and self.wal is not None:
            self.wal.append_op(op)      # 先落盘，再改内存
        return self._apply_only(op)

    def _apply_only(self, op: Op) -> bool:
        if op.origin in self._applied:
            return False
        self._applied.add(op.origin)
        self.log.append(op)
        apply_op(self.cart(op.user_id), op)
        self._apply_shadow(op)
        self.vv = self.vv.merge(op.ctx).observe(op.origin)
        # 恢复时也要把本地计数器推到已用过的最大序号，否则重启后会重复使用 dot
        if op.origin.replica == self.name and op.origin.seq > self.counter:
            self.counter = op.origin.seq
        if op.request_id:
            self._by_request.setdefault(op.request_id, op)
        return True

    # -- 反熵 -------------------------------------------------------------- #

    def ops_for(self, peer_vv: VersionVector, limit: int | None = None) -> list[Op]:
        missing = [op for op in self.log if not peer_vv.includes(op.origin)]
        return missing[:limit] if limit is not None else missing

    def pull_payload(self, peer_vv: VersionVector, limit: int | None = None) -> dict:
        """给对端准备它缺的东西：优先发操作，日志已覆盖不到时改发全量状态。"""
        if self.down:
            raise ReplicaDown(self.name)

        truncated = (
            self.checkpoint_vv is not None
            and self.checkpoint_vv.dominates(peer_vv)
            and not peer_vv.dominates(self.checkpoint_vv)
        )
        if truncated:
            from .codec import cart_to_json

            return {
                "type": "state",
                "vv": self.vv.to_dict(),
                "carts": {user: cart_to_json(cart) for user, cart in self.carts.items()},
                "more": False,
            }

        missing = self.ops_for(peer_vv, limit)
        from .codec import op_to_json

        return {
            "type": "ops",
            "ops": [op_to_json(op) for op in missing],
            "more": limit is not None and len(self.ops_for(peer_vv)) > limit,
        }

    def merge_payload(self, payload: dict) -> int:
        """合并对端发来的操作或状态；返回新应用的操作数。"""
        applied = 0
        if payload.get("type") == "state":
            for user, payload_cart in payload.get("carts", {}).items():
                remote = cart_from_json(payload_cart)
                self.carts[user] = self.cart(user).merge_with(remote)
            self.vv = self.vv.merge(VersionVector.from_mapping(payload.get("vv", {})))
            return 1
        for raw in payload.get("ops", []):
            if self.apply(op_from_json(raw)):
                applied += 1
        return applied

    def join_from(self, peer: "Replica", page_size: int = 10,
                  pages: int | None = None) -> int:
        """从对端分页拉取（新节点加入 / 数据迁移）。

        ``pages`` 用来故意在第 N 页之后中断，模拟"迁移到一半宕机"：
        已经应用的操作已经进了 WAL，重启后 ``vv`` 会告诉它从哪里继续。
        """
        total = 0
        page_no = 0
        while True:
            payload = peer.pull_payload(self.vv, limit=page_size)
            applied = self.merge_payload(payload)
            total += applied
            page_no += 1
            if pages is not None and page_no >= pages:
                raise MigrationInterrupted(f"在第 {page_no} 页后中断，已应用 {total} 条")
            if not payload.get("more"):
                break
            if applied == 0 and not payload.get("more"):
                break
        return total

    # -- LWW 影子视图（只用于对照展示） ------------------------------------ #

    def _shadow(self, user_id: str) -> LwwCart:
        cart = self.shadow.get(user_id)
        if cart is None:
            cart = LwwCart(user_id)
            self.shadow[user_id] = cart
        return cart

    def _apply_shadow(self, op: Op) -> None:
        """把同一个操作喂给"按时间戳覆盖"的对照组。

        它不参与真实状态，纯粹是为了让页面能并排显示：
        同一个序列，CRDT 得到什么、LWW 丢掉了什么。
        """
        if op.kind == "add":
            self._shadow(op.user_id).put(op.sku, op.qty, op.ts)
        elif op.kind == "remove":
            self._shadow(op.user_id).remove(op.sku, op.ts)

    def lww_view(self) -> dict[str, dict[str, int]]:
        return {user: cart.contents() for user, cart in sorted(self.shadow.items())}

    # -- 维护 -------------------------------------------------------------- #

    def checkpoint(self) -> None:
        if self.wal is None:
            return
        self.wal.append_checkpoint(
            self.carts,
            self.vv,
            self.counter,
            set(self._by_request),
            self._applied,
        )
        self.checkpoint_vv = self.vv

    def close(self) -> None:
        if self.wal is not None:
            self.wal.close()

    def __str__(self) -> str:
        status = "down" if self.down else "up"
        return f"Replica({self.name}, {status}, vv={self.vv}, ops={len(self.log)})"
