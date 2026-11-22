"""CRDT 对象的 JSON 编解码：WAL、HTTP 接口、快照共用同一套格式。"""

from __future__ import annotations

from .crdt import Cart, Dot, Op, SkuEntry, VersionVector

__all__ = [
    "dot_to_json",
    "dot_from_json",
    "op_to_json",
    "op_from_json",
    "cart_to_json",
    "cart_from_json",
]


def dot_to_json(dot: Dot) -> list[object]:
    return [dot.replica, dot.seq]


def dot_from_json(data: object) -> Dot:
    replica, seq = data  # type: ignore[misc]
    return Dot(str(replica), int(seq))


def op_to_json(op: Op) -> dict[str, object]:
    return {
        "user": op.user_id,
        "kind": op.kind,
        "sku": op.sku,
        "qty": op.qty,
        "dots": [dot_to_json(d) for d in op.dots],
        "origin": dot_to_json(op.origin),
        "ctx": op.ctx.to_dict(),
        "rid": op.request_id,
        "ts": op.ts.to_dict(),
    }


def op_from_json(data: dict[str, object]) -> Op:
    from .hlc import Timestamp

    raw_ts = data.get("ts")
    return Op(
        user_id=str(data["user"]),
        kind=str(data["kind"]),  # type: ignore[arg-type]
        sku=str(data["sku"]),
        qty=int(data["qty"]),  # type: ignore[arg-type]
        dots=tuple(dot_from_json(d) for d in data.get("dots", [])),  # type: ignore[union-attr]
        origin=dot_from_json(data["origin"]),
        ctx=VersionVector.from_mapping(data.get("ctx", {})),  # type: ignore[arg-type]
        request_id=str(data.get("rid", "")),
        ts=Timestamp.from_dict(raw_ts) if isinstance(raw_ts, dict) else Timestamp(0, 0, ""),
    )


def _entry_to_json(entry: SkuEntry) -> dict[str, object]:
    return {
        "adds": [[d.replica, d.seq, qty] for d, qty in entry.adds.items()],
        "removed": [[d.replica, d.seq] for d in entry.removed],
    }


def _entry_from_json(data: dict[str, object]) -> SkuEntry:
    entry = SkuEntry()
    for replica, seq, qty in data.get("adds", []):  # type: ignore[union-attr]
        entry.adds[Dot(str(replica), int(seq))] = int(qty)
    for replica, seq in data.get("removed", []):  # type: ignore[union-attr]
        entry.removed.add(Dot(str(replica), int(seq)))
    return entry


def cart_to_json(cart: Cart) -> dict[str, object]:
    return {
        "user_id": cart.user_id,
        "checkout_version": cart.checkout_version,
        "items": {sku: _entry_to_json(entry) for sku, entry in cart.items.items()},
    }


def cart_from_json(data: dict[str, object]) -> Cart:
    cart = Cart(str(data["user_id"]))
    cart.checkout_version = int(data.get("checkout_version", 0))  # type: ignore[arg-type]
    for sku, payload in dict(data.get("items", {})).items():  # type: ignore[arg-type]
        cart.items[str(sku)] = _entry_from_json(payload)
    return cart
