"""HLC 与 LWW 对照测试：对应「系统时间突然倒退一分钟」。"""

from __future__ import annotations

from cart.hlc import Clock, Timestamp
from cart.lww import LwwCart


def test_hlc_is_monotonic_within_same_millisecond() -> None:
    clock = Clock("r1")
    stamps = [clock.now() for _ in range(50)]
    assert stamps == sorted(stamps)
    assert len(set(stamps)) == len(stamps)


def test_hlc_survives_clock_going_backwards() -> None:
    clock = Clock("r3")
    first = clock.now()
    clock.set_offset(-60_000)          # 时钟倒退一分钟
    second = clock.now()
    third = clock.now()
    assert first < second < third, "HLC 必须在物理时钟倒退后仍然单调"


def test_hlc_observe_makes_remote_ordering_possible() -> None:
    a = Clock("a")
    b = Clock("b")
    stamp_a = a.now()
    stamp_b = b.observe(stamp_a)
    assert stamp_b > stamp_a
    assert b.now() > stamp_b


def test_lww_loses_concurrent_add_when_clocks_disagree() -> None:
    base = 1_700_000_000_000
    fast = LwwCart("alice")
    slow = LwwCart("alice")
    fast.put("苹果", 1, Timestamp(base, 0, "r1"))
    slow.put("苹果", 2, Timestamp(base - 60_000, 0, "r3"))

    assert fast.merge_with(slow).contents() == {"苹果": 1}, "LWW 会吞掉慢时钟一侧的写入"


def test_lww_is_commutative_but_lossy() -> None:
    a = LwwCart("alice")
    b = LwwCart("alice")
    a.put("苹果", 1, Timestamp(10, 0, "r1"))
    b.put("苹果", 2, Timestamp(20, 0, "r2"))
    assert a.merge_with(b).contents() == b.merge_with(a).contents() == {"苹果": 2}
