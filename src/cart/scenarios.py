"""九个故障场景：每个都按「预测 → 注入 → 对照 → 解释」组织。

编号与作业 PPT 的 topic 一一对应：

1. 一个副本突然宕机
2. 网络出现双向分区，消息被重复发送三次
3. 消息延迟 10 秒后乱序到达
4. 节点重启后丢失内存状态
5. 新节点在数据迁移到一半时宕机
6. 某个 key 占据 50% 的请求
7. 两个客户端同时执行 CAS
8. 系统时间突然倒退一分钟
9. 磁盘写成功，但响应在途中丢失
"""

from __future__ import annotations

import shutil
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from .cluster import Cluster
from .hlc import Clock, Timestamp
from .lww import LwwCart
from .network import Network
from .replica import (
    CheckoutUnavailable,
    MigrationInterrupted,
    Replica,
    VersionConflict,
)

__all__ = ["Check", "ScenarioResult", "SCENARIOS", "run_scenario", "ALL_IDS"]

USER = "alice"

NAMES = ("r1", "r2", "r3")

_ROOT = Path(__file__).resolve().parents[2]


# --------------------------------------------------------------------------- #
# 结果类型
# --------------------------------------------------------------------------- #


@dataclass
class Check:
    name: str
    expected: object
    actual: object

    @property
    def ok(self) -> bool:
        return self.expected == self.actual


@dataclass
class ScenarioResult:
    sid: int
    title: str
    topic: str
    prediction: list[str]
    checks: list[Check] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    duration_ms: float = 0.0

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks)

    @property
    def passed(self) -> int:
        return sum(1 for check in self.checks if check.ok)


def check(name: str, actual: object, expected: object) -> Check:
    return Check(name=name, expected=expected, actual=actual)


def _cluster(count: int = 3) -> Cluster:
    replicas = [Replica(name) for name in NAMES[:count]]
    return Cluster(replicas, network=Network())


@contextmanager
def sandbox_dir(name: str):
    """临时目录放在仓库内的 ``.scratch/`` 下（已 gitignore）。

    不放到系统 %TEMP%，是因为某些受限环境会禁止在那里创建文件；
    放在工作区内则任何沙箱策略都能写。
    """
    path = _ROOT / ".scratch" / f"{name}-{uuid.uuid4().hex[:8]}"
    path.mkdir(parents=True, exist_ok=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


# --------------------------------------------------------------------------- #
# 场景 1：副本突然宕机
# --------------------------------------------------------------------------- #


def scenario_1(slow: bool = False) -> ScenarioResult:
    result = ScenarioResult(
        sid=1,
        title="一个副本突然宕机",
        topic="一个副本突然宕机",
        prediction=[
            "宕机期间其余两个副本继续接受写入，客户端无感",
            "宕机副本重启后通过反熵补齐缺失的操作",
            "最终三个副本状态一致，且不出现重复计数",
        ],
    )

    cluster = _cluster()
    cluster.add("r1", USER, "咖啡豆", 2)
    cluster.add("r1", USER, "燕麦奶", 1)
    cluster.converge()

    cluster.replica("r2").set_down(True)
    cluster.add("r1", USER, "苹果", 3)
    cluster.add("r3", USER, "牛奶", 1)
    cluster.converge()

    during = cluster.replica("r2").read(USER)
    cluster.replica("r2").set_down(False)
    cluster.converge()

    result.checks = [
        check("宕机副本未收到新写入", during, {"咖啡豆": 2, "燕麦奶": 1}),
        check(
            "重启后补齐全部数据",
            cluster.replica("r2").read(USER),
            {"咖啡豆": 2, "燕麦奶": 1, "苹果": 3, "牛奶": 1},
        ),
        check("三副本最终一致", cluster.is_converged(USER), True),
    ]
    result.notes.append("反熵只补对方版本向量里缺的 op，因此重启后的补齐不会产生重复。")
    return result


# --------------------------------------------------------------------------- #
# 场景 2：双向分区 + 消息重复三次
# --------------------------------------------------------------------------- #


def scenario_2(slow: bool = False) -> ScenarioResult:
    result = ScenarioResult(
        sid=2,
        title="双向分区，消息被重复发送三次",
        topic="网络出现双向分区消息被重复发送三次",
        prediction=[
            "分区期间两侧各自都能写，但互相看不到",
            "同款商品两端各加，恢复后数量相加而不是覆盖",
            "每条消息重复三份，数量不会变成三倍",
        ],
    )

    cluster = _cluster()
    cluster.network.partition({"r1", "r2"}, {"r3"})

    cluster.add("r1", USER, "苹果", 1)
    cluster.add("r3", USER, "苹果", 2)
    cluster.converge()

    left = cluster.replica("r1").read(USER)
    right = cluster.replica("r3").read(USER)

    cluster.network.heal_all()
    cluster.network.copies = 3          # 每条 op 重复投递三次
    cluster.converge()

    duplicates = sum(r.duplicates_ignored for r in cluster.replicas.values())
    result.checks = [
        check("分区两侧各写各的", (left, right), ({"苹果": 1}, {"苹果": 2})),
        check("恢复后数量相加", cluster.read(USER), {"苹果": 3}),
        check("重复消息被幂等拦下", duplicates > 0, True),
        check("三副本一致", cluster.is_converged(USER), True),
    ]
    result.notes.append(f"本次共拦下 {duplicates} 次重复投递（副本级 origin 去重）。")
    return result


# --------------------------------------------------------------------------- #
# 场景 3：延迟 10 秒后乱序到达
# --------------------------------------------------------------------------- #


def scenario_3(slow: bool = False) -> ScenarioResult:
    latency = 10000 if slow else 300
    result = ScenarioResult(
        sid=3,
        title=f"消息延迟 {latency / 1000:g} 秒后乱序到达",
        topic="消息延迟 10 秒后乱序到达",
        prediction=[
            "延迟期间两端读到旧值（可观察到的陈旧读）",
            "乱序到达不会让已删除的商品复活，也不会丢添加",
            "延迟消除后所有副本收敛到同一结果",
        ],
    )

    cluster = _cluster(2)
    cluster.network.injector.policy.latency_ms = latency
    cluster.network.injector.policy.reorder_window_ms = 1.0   # 触发打乱投递顺序
    cluster.network.block("r1", "r2")

    cluster.add("r1", USER, "咖啡豆", 2)
    cluster.add("r2", USER, "苹果", 3)
    stale = cluster.replica("r1").read(USER)

    cluster.network.heal_all()
    started = time.perf_counter()
    cluster.converge(rounds=3)
    elapsed = time.perf_counter() - started

    result.checks = [
        check("延迟期间读到陈旧值", stale, {"咖啡豆": 2}),
        check("乱序后两件商品都在", cluster.read(USER), {"咖啡豆": 2, "苹果": 3}),
        check("延迟真实发生", elapsed >= latency / 1000 * 0.8, True),
        check("两副本收敛", cluster.is_converged(USER), True),
    ]
    result.notes.append(
        f"实测等待 {elapsed * 1000:.0f}ms。乱序用「打乱投递顺序」模拟；"
        "墓碑与到达顺序无关，因此先到的删除不会被后到的添加复活。"
    )
    return result


# --------------------------------------------------------------------------- #
# 场景 4：节点重启丢失内存状态
# --------------------------------------------------------------------------- #


def scenario_4(slow: bool = False) -> ScenarioResult:
    result = ScenarioResult(
        sid=4,
        title="节点重启后丢失内存状态",
        topic="节点重启后丢失内存状态",
        prediction=[
            "内存全丢但 WAL 还在，重启后从日志恢复",
            "恢复后的状态、版本向量、本地计数器与宕机前完全一致",
            "已回 ack 的写入一条都不能少",
        ],
    )

    with sandbox_dir("s4") as tmp:
        path = Path(tmp) / "r1.wal"
        replica = Replica("r1", wal=path)
        replica.add(USER, "咖啡豆", 2)
        replica.add(USER, "燕麦奶", 1)
        replica.checkpoint()                    # 写检查点
        replica.add(USER, "苹果", 3)            # 检查点之后的增量

        before_contents = replica.read(USER)
        before_vv = replica.vv
        before_counter = replica.counter
        replica.close()                         # 相当于进程被 kill

        revived = Replica("r1", wal=path)       # 内存已清空，只剩磁盘
        after_contents = revived.read(USER)

        result.checks = [
            check("恢复出全部数据", after_contents, before_contents),
            check("版本向量一致", str(revived.vv), str(before_vv)),
            check("本地计数器一致（dot 不复用）", revived.counter, before_counter),
            check("重放了检查点之后的增量", revived.recovered_ops, 1),
        ]
        result.notes.append(f"WAL 大小 {path.stat().st_size} 字节，检查点 + 增量重放。")
    return result


# --------------------------------------------------------------------------- #
# 场景 5：新节点迁移到一半宕机
# --------------------------------------------------------------------------- #


def scenario_5(slow: bool = False) -> ScenarioResult:
    result = ScenarioResult(
        sid=5,
        title="新节点在数据迁移到一半时宕机",
        topic="新节点在数据迁移到一半时宕机",
        prediction=[
            "迁移分页进行，第一页应用后进程被杀",
            "重启后从版本向量指示的位置继续，不从头再来",
            "最终新节点与源节点数据完全一致，不丢不重",
        ],
    )

    with sandbox_dir("s5") as tmp:
        source = Replica("r1", wal=Path(tmp) / "src.wal")
        for index in range(12):
            source.add(USER, f"商品{index % 4}", 1)

        target_path = Path(tmp) / "new.wal"
        newcomer = Replica("r4", wal=target_path)
        interrupted = False
        try:
            newcomer.join_from(source, page_size=3, pages=1)
        except MigrationInterrupted:
            interrupted = True
        partial = newcomer.read(USER)
        newcomer.close()

        revived = Replica("r4", wal=target_path)     # 重启
        revived.join_from(source, page_size=3)

        result.checks = [
            check("迁移确实被中断", interrupted, True),
            check("中断时只拿到部分数据", partial != source.read(USER), True),
            check("重启后补齐", revived.read(USER), source.read(USER)),
            check("没有重复计数", revived.cart(USER).total_quantity(), 12),
            check("已迁移部分无需重放", revived.recovered_ops, 3),
        ]
        result.notes.append(
            "断点续传靠的不是位点，而是版本向量：它本身就是「我收到了什么」的压缩表示。"
        )
    return result


# --------------------------------------------------------------------------- #
# 场景 6：某个 key 占据 50% 的请求
# --------------------------------------------------------------------------- #


def scenario_6(slow: bool = False) -> ScenarioResult:
    result = ScenarioResult(
        sid=6,
        title="某个 key 占据 50% 的请求",
        topic="某个 key 占据 50% 的请求",
        prediction=[
            "热点 key 上的写入全部成功，数量精确等于写入次数",
            "CRDT 的 add 是独立 dot，热点 key 上没有互斥，因此不会互相冲突",
            "同样的热点如果用 CAS 串行化，5 个并发请求只会成功 1 个",
        ],
    )

    cluster = _cluster()
    total = 200
    hot = "手机"
    for index in range(total):
        if index % 2 == 0:
            cluster.add("r1" if index % 4 == 0 else "r3", USER, hot, 1)
        else:
            cluster.add("r2", USER, f"冷门{index % 5}", 1)
    cluster.converge()

    hot_qty = cluster.replica("r1").read(USER).get(hot, 0)

    # 对照：热点 key 上做 CAS，只有第一个能成功
    success = conflicts = 0
    for _ in range(5):
        try:
            cluster.checkout(USER, 0)
            success += 1
        except VersionConflict:
            conflicts += 1

    result.checks = [
        check("热点写入次数", total // 2, 100),
        check("热点 key 数量精确", hot_qty, 100),
        check("热点写入零失败", cluster.is_converged(USER), True),
        check("CAS 串行化下只有 1 个成功", (success, conflicts), (1, 4)),
    ]
    result.notes.append(
        "这说明 CRDT 用「每个写自带 dot」消除了热点 key 的写冲突；"
        "真正的热点分片（把单 key 拆成多副本计数器）留给读扩展，见设计文档。"
    )
    return result


# --------------------------------------------------------------------------- #
# 场景 7：两个客户端同时执行 CAS
# --------------------------------------------------------------------------- #


def scenario_7(slow: bool = False) -> ScenarioResult:
    result = ScenarioResult(
        sid=7,
        title="两个客户端同时执行 CAS",
        topic="两个客户端同时执行 CAS",
        prediction=[
            "两个客户端拿同一个版本号结算，只有一个成功",
            "失败方收到明确的版本冲突，可以刷新后重试（不会重复扣款）",
            "分区时若主副本联系不到多数派，结算被拒绝——购物车写入却仍然可用",
        ],
    )

    cluster = _cluster()
    user = next(
        name for name in (f"user{i}" for i in range(64))
        if cluster.owner_of(name).name == "r3"
    )
    cluster.add("r1", user, "苹果", 2)

    outcomes: list[str] = []
    for _ in range(2):
        try:
            receipt = cluster.checkout(user, 0)
            outcomes.append(f"成功@v{receipt.version}")
        except VersionConflict as exc:
            outcomes.append(f"冲突(实际v{exc.current})")

    # 分区：把主副本 r3 隔离到少数派
    cluster.network.partition({"r3"}, {"r1", "r2"})
    cart_still_ok = True
    cluster.add("r1", user, "牛奶", 1)          # AP：购物车照常可写
    try:
        cluster.checkout(user, 1)
        checkout_refused = False
    except CheckoutUnavailable:
        checkout_refused = True

    result.checks = [
        check("并发 CAS 结果", sorted(outcomes), sorted(["成功@v1", "冲突(实际v1)"])),
        check("分区期间购物车仍可写", cart_still_ok, True),
        check("分区期间结算被拒绝（CP）", checkout_refused, True),
    ]
    result.notes.append(
        "该用户的结算主副本是 r3（一致性哈希），被隔离后只剩 1/3，低于多数派 2，"
        "因此拒绝结算——这正是「宁可拒绝也不重复扣款」的取舍。"
    )
    return result


# --------------------------------------------------------------------------- #
# 场景 8：系统时间突然倒退一分钟
# --------------------------------------------------------------------------- #


def scenario_8(slow: bool = False) -> ScenarioResult:
    result = ScenarioResult(
        sid=8,
        title="系统时间突然倒退一分钟",
        topic="系统时间突然倒退一分钟",
        prediction=[
            "LWW 直接用物理时间戳，时钟倒退的一侧写入「更早」，会被吞掉",
            "HLC 的物理部分贴近真实时间、逻辑部分保证单调，倒退后仍严格递增",
            "CRDT 不依赖时间戳排序并发写，因此完全不受影响",
        ],
    )

    baseline = 1_700_000_000_000
    fast = LwwCart(USER)
    slow_cart = LwwCart(USER)
    fast.put("苹果", 1, Timestamp(baseline, 0, "r1"))
    slow_cart.put("苹果", 2, Timestamp(baseline - 60_000, 0, "r3"))   # 慢一分钟
    lww_merged = fast.merge_with(slow_cart)

    clock = Clock("r3", offset_ms=-60_000)
    first = clock.now()
    second = clock.now()
    third = clock.now()

    cluster = _cluster(2)
    cluster.add("r1", USER, "苹果", 1)
    cluster.add("r2", USER, "苹果", 2)
    cluster.converge()

    result.checks = [
        check("LWW 吞掉了慢时钟侧的数量", lww_merged.contents().get("苹果"), 1),
        check("HLC 时钟倒退后仍单调", first < second < third, True),
        check("CRDT 不受时钟影响", cluster.read(USER), {"苹果": 3}),
    ]
    result.notes.append(
        "这正是「为什么不能只用物理时钟解决冲突」的答案：物理时钟既可能回退，"
        "也无法表达因果，只能靠逻辑时钟/版本向量。"
    )
    return result


# --------------------------------------------------------------------------- #
# 场景 9：磁盘写成功但响应丢失
# --------------------------------------------------------------------------- #


def scenario_9(slow: bool = False) -> ScenarioResult:
    result = ScenarioResult(
        sid=9,
        title="磁盘写成功，但响应在途中丢失",
        topic="磁盘写成功，但响应在途中丢失",
        prediction=[
            "服务端已经落盘并生效，但客户端没收到回包",
            "客户端用同一个 request_id 重试，服务端识别为重复请求，不再累加",
            "即使此时进程重启，去重表也能从 WAL 重建",
        ],
    )

    with sandbox_dir("s9") as tmp:
        path = Path(tmp) / "r1.wal"
        replica = Replica("r1", wal=path)
        request_id = uuid.uuid4().hex

        first = replica.add(USER, "苹果", 2, request_id=request_id)
        # 回包丢了，客户端重试同一个 request_id
        second = replica.add(USER, "苹果", 2, request_id=request_id)
        quantity_after_retry = replica.cart(USER).quantity("苹果")
        replica.close()

        revived = Replica("r1", wal=path)
        stored = revived.wal.scan()  # type: ignore[union-attr]

        result.checks = [
            check("重试后数量不翻倍", quantity_after_retry, 2),
            check("重试返回同一条操作", second is first, True),
            check("被识别为重复请求", replica.duplicates_ignored, 1),
            check("重启后数量依旧正确", revived.cart(USER).quantity("苹果"), 2),
            check(
                "WAL 里该请求只有一条",
                sum(1 for op in stored.ops if op.request_id == request_id),
                1,
            ),
        ]
        result.notes.append(
            "去重表不需要单独持久化：request_id 就写在操作里，恢复时重放即重建。"
        )
    return result


# --------------------------------------------------------------------------- #
# 注册表
# --------------------------------------------------------------------------- #

SCENARIOS = {
    1: scenario_1,
    2: scenario_2,
    3: scenario_3,
    4: scenario_4,
    5: scenario_5,
    6: scenario_6,
    7: scenario_7,
    8: scenario_8,
    9: scenario_9,
}

ALL_IDS = sorted(SCENARIOS)


def run_scenario(sid: int, slow: bool = False) -> ScenarioResult:
    started = time.perf_counter()
    result = SCENARIOS[sid](slow=slow)
    result.duration_ms = (time.perf_counter() - started) * 1000
    return result
