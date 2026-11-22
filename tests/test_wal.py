"""持久化与恢复测试：WAL、检查点、乱序与半行容忍。"""

from __future__ import annotations

import json

from cart.replica import Replica
from cart.wal import WriteAheadLog


def test_wal_replay_without_checkpoint(tmp_path) -> None:
    path = tmp_path / "r1.wal"
    replica = Replica("r1", wal=path)
    replica.add("alice", "苹果", 2)
    replica.add("alice", "牛奶", 1)
    replica.close()

    revived = Replica("r1", wal=path)
    assert revived.read("alice") == {"苹果": 2, "牛奶": 1}
    assert revived.recovered_ops == 2


def test_checkpoint_replaces_history(tmp_path) -> None:
    path = tmp_path / "r1.wal"
    replica = Replica("r1", wal=path)
    replica.add("alice", "苹果", 2)
    replica.checkpoint()
    replica.add("alice", "牛奶", 1)
    replica.close()

    revived = Replica("r1", wal=path)
    assert revived.read("alice") == {"苹果": 2, "牛奶": 1}
    assert revived.recovered_ops == 1, "检查点之前的历史不应该再被重放"


def test_counter_survives_restart_to_avoid_dot_reuse(tmp_path) -> None:
    path = tmp_path / "r1.wal"
    replica = Replica("r1", wal=path)
    for _ in range(3):
        replica.add("alice", "苹果", 1)
    replica.close()

    revived = Replica("r1", wal=path)
    op = revived.add("alice", "苹果", 1)
    assert op is not None
    assert op.origin.seq == 4, "重启后如果计数器回退，就会与旧 dot 撞车"


def test_torn_last_line_is_ignored(tmp_path) -> None:
    path = tmp_path / "r1.wal"
    replica = Replica("r1", wal=path)
    replica.add("alice", "苹果", 2)
    replica.close()

    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"type":"op","op":{"user":"ali')     # 掉电留下的半行

    revived = Replica("r1", wal=path)
    assert revived.read("alice") == {"苹果": 2}
    assert revived.wal is not None and revived.wal.scan().torn_lines == 1


def test_checkpoint_payload_is_valid_json(tmp_path) -> None:
    path = tmp_path / "r1.wal"
    replica = Replica("r1", wal=path)
    replica.add("alice", "苹果", 2)
    replica.checkpoint()
    replica.close()

    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    kinds = [line["type"] for line in lines]
    assert "checkpoint" in kinds
    assert kinds[-1] == "checkpoint"


def test_state_transfer_when_log_is_truncated(tmp_path) -> None:
    """日志被检查点覆盖后，落后太多的副本应该收到全量状态而不是操作。"""
    from cart.crdt import VersionVector

    path = tmp_path / "r1.wal"
    source = Replica("r1", wal=path)
    source.add("alice", "苹果", 2)
    source.checkpoint()
    source.add("alice", "牛奶", 1)

    payload = source.pull_payload(VersionVector.empty())
    assert payload["type"] == "state"
    assert payload["carts"]["alice"]["items"], "全量状态里应该带购物车内容"


def test_replica_never_serves_from_a_down_state(tmp_path) -> None:
    replica = Replica("r1", wal=tmp_path / "r1.wal")
    replica.set_down(True)
    try:
        replica.add("alice", "苹果", 1)
    except Exception as exc:  # noqa: BLE001
        assert "r1" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("宕机副本不应该接受写入")
    assert replica.rejected_while_down == 1


def test_op_timestamp_survives_json_roundtrip() -> None:
    """LWW 影子视图依赖操作里的时间戳，序列化后不能丢。"""
    from cart.codec import op_from_json, op_to_json
    from cart.crdt import Dot, Op
    from cart.hlc import Timestamp

    op = Op("alice", "add", "苹果", 2, (), Dot("r1", 1), ts=Timestamp(123, 4, "r1"))
    again = op_from_json(op_to_json(op))
    assert again.ts == op.ts
    assert again.request_id == op.request_id


def test_lww_shadow_diverges_from_crdt_under_clock_skew(tmp_path) -> None:
    """同一批操作：CRDT 累加，LWW 只留最后一条——用来演示"覆盖会丢数据"。"""
    fast = Replica("r1", wal=tmp_path / "a.wal")
    slow = Replica("r2", wal=tmp_path / "b.wal")
    slow.clock.set_offset(-60_000)

    fast.add("alice", "苹果", 1)
    slow.add("alice", "苹果", 2)          # 时间戳更早

    fast.merge_payload(slow.pull_payload(fast.vv))
    assert fast.cart("alice").quantity("苹果") == 3
    assert fast.shadow["alice"].contents().get("苹果") == 1
