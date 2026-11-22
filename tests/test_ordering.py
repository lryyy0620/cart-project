"""删除先于添加到达：墓碑必须与到达顺序无关（对应作业主题“消息延迟后乱序到达”）。"""

from __future__ import annotations

from cart.crdt import Cart, Dot
from cart.replica import Replica


def dot(replica: str, seq: int) -> Dot:
    return Dot(replica, seq)


def test_remove_arriving_before_add_still_takes_effect() -> None:
    # 因果上：先 add，再被另一副本 remove；但网络上 remove 先到
    removal_op = None
    pc = Replica("pc")
    phone = Replica("phone")
    add_op = phone.add("alice", "apple", 1)
    pc.apply(add_op)
    removal_op = pc.remove("alice", "apple")

    fresh = Replica("r9")
    assert fresh.apply(removal_op) is True      # 删除先到
    assert fresh.read("alice") == {}
    fresh.apply(add_op)                          # 添加后到，必须被墓碑吃掉
    assert fresh.read("alice") == {}, "乱序到达让已删除的商品复活了"


def test_out_of_order_delivery_converges_with_in_order() -> None:
    phone = Replica("phone")
    add_op = phone.add("alice", "apple", 2)
    pc = Replica("pc")
    pc.apply(add_op)
    removal_op = pc.remove("alice", "apple")
    pc.add("alice", "milk", 1)

    in_order = Replica("a")
    for op in (add_op, removal_op):
        in_order.apply(op)

    out_of_order = Replica("b")
    for op in (removal_op, add_op):
        out_of_order.apply(op)

    assert in_order.read("alice") == out_of_order.read("alice") == {}
