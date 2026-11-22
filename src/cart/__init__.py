"""购物车网络分区（CRDT）—— 并行与分布式计算课程作业。

分层结构（上层依赖下层，下层不知道上层存在）::

    展示  console.py + static/console.html   浏览器控制台（独立进程）
          runner.py / scripts/*              终端与脚本
    服务  api.py                             FastAPI，一个进程 = 一个副本
    编排  cluster.py                         副本 + 网络 → 一个集群
    节点  replica.py                         一个副本的全部行为
    网络  network.py（拓扑） + fault.py（延迟/丢包/重复/分区）
    存储  wal.py                             预写日志 + 内嵌检查点
    数据  crdt.py（Dot / 版本向量 / OR-Set / Cart / Op）
          codec.py（JSON）  hlc.py（逻辑时钟）  lww.py（对照组）
"""

from .cluster import Cluster
from .crdt import Cart, Dot, Op, SkuEntry, VersionVector, apply_op
from .network import Network
from .replica import Replica

__version__ = "0.1.0"

__all__ = [
    "Cart",
    "Cluster",
    "Dot",
    "Network",
    "Op",
    "Replica",
    "SkuEntry",
    "VersionVector",
    "apply_op",
]
