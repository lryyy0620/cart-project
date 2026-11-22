"""演示脚本：严格按作业要求的四步组织。

    1. 预测系统行为      —— 先把预期写在屏幕上
    2. 运行故障注入      —— 分区 / 两端并发写 / 消息重复三次
    3. 对照预测和实际结果 —— 逐条断言，PASS / FAIL
    4. 解释              —— 违反了哪项保证，为什么仍然正确

运行：``python -m cart.demo``（需 PYTHONPATH=src）
"""

from __future__ import annotations

from .cluster import Cluster
from .network import Network
from .replica import Replica
from .ui import clip, hr, pad

USER = "alice"


# --------------------------------------------------------------------------- #
# 展示辅助
# --------------------------------------------------------------------------- #


class Checker:
    """收集断言，最后一次性打印成对照表。"""

    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str, bool]] = []

    def check(self, name: str, actual: object, expected: object) -> bool:
        ok = actual == expected
        self.rows.append((name, str(expected), str(actual), ok))
        return ok

    def report(self) -> bool:
        print(f"{pad('检查项', 28)}{pad('预期', 26)}{pad('实际', 26)}结果")
        print("-" * 86)
        for name, expected, actual, ok in self.rows:
            print(
                f"{pad(name, 28)}{pad(clip(expected, 26), 26)}"
                f"{pad(clip(actual, 26), 26)}{'PASS' if ok else 'FAIL'}"
            )
        print("-" * 86)
        passed = sum(1 for row in self.rows if row[3])
        print(f"通过 {passed}/{len(self.rows)}")
        return passed == len(self.rows)


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #


def main() -> int:
    checker = Checker()

    hr("[1/4] 预测系统行为（在看到结果之前先写下预期）")
    print("分区：{r1, r2} | {r3}，用户 alice 的手机打到 r1、电脑打到 r3，两端互不可见。")
    print("预期 1：分区期间两台设备都能写入成功（选 AP，不拒绝写）。")
    print("预期 2：分区期间两边读到的购物车不一样（各自只有自己加的商品）。")
    print("预期 3：恢复后合并结果为并集——两端加的商品都在，没有谁被覆盖。")
    print("预期 4：同一商品两端各加，数量相加（1 + 2 = 3），而不是取其中一个。")
    print("预期 5：每条消息重复发送三次，数量不会变成三倍。")

    hr("[2/4] 运行故障注入")
    cluster = Cluster([Replica(name) for name in ("r1", "r2", "r3")])
    cluster.network.partition({"r1", "r2"}, {"r3"})
    print(f"已切断 r3 与 r1/r2 之间的双向链路：{cluster.network}")

    phone_items = [("咖啡豆", 2), ("苹果", 1)]
    pc_items = [("燕麦奶", 1), ("苹果", 2)]
    phone_ops = [cluster.add("r1", USER, sku, qty) for sku, qty in phone_items]
    pc_ops = [cluster.add("r3", USER, sku, qty) for sku, qty in pc_items]
    print(f"手机端 → r1 写入：{phone_items}")
    print(f"电脑端 → r3 写入：{pc_items}")

    # 分区期间仍做一轮反熵：组内能同步，跨组一个字节都过不去
    cluster.network.copies = 1
    cluster.converge(rounds=2)
    before_heal = cluster.views()
    print("\n分区期间的三个副本视图（注意 r1/r2 与 r3 不一致，这正是可观察到的旧值）：")
    for name, view in before_heal.items():
        print(f"  {name}: {view.get(USER, {})}")
    dropped = cluster.network.dropped
    print(f"  跨分区被丢弃的消息数：{dropped}")

    hr("[3/4] 恢复网络 + 消息重复三次，对照预测与实际")
    cluster.network.heal_all()
    cluster.network.copies = 3          # 每条 op 重复投递三次
    print("网络已恢复；接下来每条操作都会被重复投递 3 次（模拟消息被重复发送）。")
    cluster.converge(rounds=6)

    print("\n恢复同步后的三个副本视图：")
    for name, view in cluster.views().items():
        print(f"  {name}: {view.get(USER, {})}")

    final = cluster.read(USER)
    expected = {"咖啡豆": 2, "燕麦奶": 1, "苹果": 3}
    print(f"\n合并读取：{final}")
    print(f"重复投递被幂等拦下的次数：{sum(r.duplicates_ignored for r in cluster.replicas.values())}")

    checker.check("分区期间两端都能写", all(op is not None for op in phone_ops + pc_ops), True)
    checker.check("分区期间视图不一致", len({str(v.get(USER, {})) for v in before_heal.values()}) > 1, True)
    checker.check("恢复后三副本一致", cluster.is_converged(USER), True)
    checker.check("两端加的商品都在（并集）", final, expected)
    checker.check("同一商品数量相加", final.get("苹果"), 3)
    checker.check("重复三次没有重复计数", final.get("咖啡豆"), 2)

    hr("[4/4] 解释：违反了哪项保证，为什么仍然正确")
    print("违反：线性一致性。分区期间 r3 读不到 r1 的写入，客户端必然读到旧值；")
    print("      同一时刻两个副本对同一购物车给出不同答案，全局不存在单一线性化顺序。")
    print("满足：最终一致性——网络恢复且消息可送达后，所有副本收敛到同一状态，")
    print("      且收敛值与消息顺序、重复次数无关。")
    print("满足：因果一致性——版本向量保证了'看到过的写'不会丢，")
    print("      而并发的写按 OR-Set 语义求并集，两端添加的商品都保留。")
    print()
    print("为什么不是 LWW：若按时间戳取最大，手机在 r1 加的咖啡豆会被电脑在 r3 的写入")
    print("直接覆盖，用户加的商品凭空消失——正是作业里禁止的'简单覆盖掉其中一端'。")

    ok = checker.report()
    print()
    print("结果：" + ("全部通过 ✅" if ok else "存在失败项 ❌"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
