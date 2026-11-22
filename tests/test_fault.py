"""故障注入器测试：注入方式本身必须可预测，否则实验结果不可信。"""

from __future__ import annotations

from cart.fault import FaultInjector, FaultPolicy


def test_partition_drops_everything() -> None:
    injector = FaultInjector(FaultPolicy())
    injector.policy.block("r1", "r3")
    assert not injector.plan("r1", "r3").delivered
    assert injector.plan("r1", "r2").delivered
    injector.policy.heal()
    assert injector.plan("r1", "r3").delivered


def test_duplicate_produces_multiple_copies() -> None:
    injector = FaultInjector(FaultPolicy(duplicate=3))
    plan = injector.plan("r1", "r2")
    assert plan.copies == 3
    assert injector.stats["duplicated"] == 2


def test_latency_and_reorder_apply_to_plan() -> None:
    injector = FaultInjector(FaultPolicy(latency_ms=100, reorder_window_ms=50), seed=7)
    delays = [injector.plan("r1", "r2").delay_ms for _ in range(20)]
    assert all(delay >= 100 for delay in delays)
    assert len(set(delays)) > 1, "乱序窗口应产生不同的延迟"


def test_drop_rate_is_reproducible_with_seed() -> None:
    def dropped(seed: int) -> int:
        injector = FaultInjector(FaultPolicy(drop_rate=0.5), seed=seed)
        return sum(1 for _ in range(200) if injector.plan("r1", "r2").dropped)

    assert dropped(42) == dropped(42)
    assert 50 < dropped(42) < 150


def test_response_loss_is_separate_from_request_loss() -> None:
    injector = FaultInjector(FaultPolicy(drop_response_rate=1.0))
    assert injector.plan("r1", "r2").delivered, "请求本身应该照常送达"
    assert injector.should_lose_response("client", "r1")
