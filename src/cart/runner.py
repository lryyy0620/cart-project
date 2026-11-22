"""一键运行九个故障场景，输出对照表。

    python -m cart.runner              # 全部场景
    python -m cart.runner --scenario 3 --scenario 7
    python -m cart.runner --slow       # 场景 3 使用真实的 10 秒延迟
"""

from __future__ import annotations

import argparse
import sys
import traceback

from .scenarios import ALL_IDS, ScenarioResult, run_scenario
from .ui import hr, table

__all__ = ["main", "render"]


def render(result: ScenarioResult) -> None:
    hr(f"场景 {result.sid}/9：{result.title}")
    print(f"作业主题：{result.topic}")

    print("\n【预测】看到结果之前先写下预期：")
    for line in result.prediction:
        print(f"  · {line}")

    print("\n【对照】预测 vs 实际：")
    rows = [
        [c.name, str(c.expected), str(c.actual), "PASS" if c.ok else "FAIL"]
        for c in result.checks
    ]
    table(["检查项", "预期", "实际", ""], rows, [36, 28, 28])

    if result.notes:
        print("\n【解释】")
        for note in result.notes:
            print(f"  · {note}")
    print(f"\n本场景 {result.passed}/{len(result.checks)} 通过，耗时 {result.duration_ms:.0f}ms")


def summary(results: list[ScenarioResult]) -> None:
    hr("总览")
    rows = [
        [
            str(r.sid),
            r.title,
            f"{r.passed}/{len(r.checks)}",
            f"{r.duration_ms:.0f}ms",
            "PASS" if r.ok else "FAIL",
        ]
        for r in results
    ]
    table(["编号", "场景", "检查", "耗时", ""], rows, [6, 36, 8, 10])

    passed = sum(1 for r in results if r.ok)
    print(f"场景通过 {passed}/{len(results)}")
    failed = [r.sid for r in results if not r.ok]
    if failed:
        print("失败场景：" + "、".join(str(sid) for sid in failed))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cart.runner", description="运行故障场景")
    parser.add_argument("--scenario", type=int, action="append",
                        help="只跑指定编号（可重复）")
    parser.add_argument("--slow", action="store_true",
                        help="场景 3 使用真实的 10 秒延迟")
    parser.add_argument("--quiet", action="store_true", help="只打印总览")
    args = parser.parse_args(argv)

    ids = args.scenario or ALL_IDS
    unknown = [sid for sid in ids if sid not in ALL_IDS]
    if unknown:
        print(f"未知场景编号：{unknown}，可用：{ALL_IDS}")
        return 2

    results: list[ScenarioResult] = []
    for sid in ids:
        try:
            result = run_scenario(sid, slow=args.slow)
        except Exception as exc:  # noqa: BLE001 - 一个场景炸了不该拖垮整套
            print(f"场景 {sid} 执行失败：{exc}")
            traceback.print_exc()
            return 2
        results.append(result)
        if not args.quiet:
            render(result)

    summary(results)
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
