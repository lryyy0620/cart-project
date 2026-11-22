"""九个故障场景必须全部通过——这是项目对外的验收线。"""

from __future__ import annotations

import pytest

from cart.scenarios import ALL_IDS, run_scenario


@pytest.mark.parametrize("sid", ALL_IDS)
def test_scenario_passes(sid: int) -> None:
    result = run_scenario(sid)
    failures = [c for c in result.checks if not c.ok]
    assert not failures, f"场景 {sid} 未通过：{[(c.name, c.expected, c.actual) for c in failures]}"
