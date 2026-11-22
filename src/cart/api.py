"""HTTP 服务：把副本包装成一个真能被 curl / docker 调用的服务。

一个进程 = 一个副本。副本之间通过 ``/internal/pull`` 做反熵，
通过 ``/admin/*`` 注入故障，客户端用 ``/cart/*`` 读写。

环境变量：

======================  ====================================================
``REPLICA_NAME``        本副本名字（默认 r1）
``CLUSTER``             全部成员，逗号分隔（用于算多数派与结算主副本）
``PEERS``               对端，形如 ``r2=http://r2:8000,r3=http://r3:8000``
``DATA_DIR``            WAL 目录；不设则纯内存
``SYNC_INTERVAL``       反熵周期（秒，默认 0.5）
======================  ====================================================
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .cluster import owner_name
from .crdt import VersionVector
from .fault import FaultInjector, FaultPolicy
from .replica import (
    CheckoutUnavailable,
    Replica,
    ReplicaDown,
    VersionConflict,
)
from .wal import WriteAheadLog

__all__ = ["create_app"]


# --------------------------------------------------------------------------- #
# 请求体
# --------------------------------------------------------------------------- #


class AddItem(BaseModel):
    sku: str
    qty: int = 1
    request_id: str | None = None


class CheckoutBody(BaseModel):
    expected_version: int = 0
    request_id: str | None = None


class PullBody(BaseModel):
    vv: dict[str, int] = Field(default_factory=dict)
    limit: int | None = None


class FaultBody(BaseModel):
    latency_ms: float | None = None
    jitter_ms: float | None = None
    duplicate: int | None = None
    drop_rate: float | None = None
    reorder_window_ms: float | None = None
    drop_response_rate: float | None = None
    heal: bool = False


class PartitionBody(BaseModel):
    peers: list[str]
    block: bool = True


class DownBody(BaseModel):
    down: bool = True


class MigrateBody(BaseModel):
    source: str
    page_size: int = 5
    pages: int | None = None      # 只拉这么多页就"中断"，用来演示断点续传
    max_pages: int = 1000


class ClockBody(BaseModel):
    offset_ms: int


class PauseBody(BaseModel):
    paused: bool = True


class BurstBody(BaseModel):
    count: int = 200
    hot_ratio: float = 0.5
    hot_sku: str = "手机"
    user: str = "alice"


# --------------------------------------------------------------------------- #
# 应用工厂
# --------------------------------------------------------------------------- #


def _parse_peers(raw: str) -> dict[str, str]:
    peers: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        peer_name, url = item.split("=", 1)
        peers[peer_name.strip()] = url.strip().rstrip("/")
    return peers


def create_app(
    name: str | None = None,
    peers: dict[str, str] | None = None,
    cluster: list[str] | None = None,
    data_dir: str | None = None,
    sync_interval: float | None = None,
) -> FastAPI:
    name = name or os.environ.get("REPLICA_NAME", "r1")
    peers = peers if peers is not None else _parse_peers(os.environ.get("PEERS", ""))
    cluster = cluster or [n for n in os.environ.get("CLUSTER", name).split(",") if n]
    data_dir = data_dir if data_dir is not None else os.environ.get("DATA_DIR")
    if sync_interval is None:
        sync_interval = float(os.environ.get("SYNC_INTERVAL", "0.5"))

    wal = WriteAheadLog(Path(data_dir) / f"{name}.wal") if data_dir else None
    replica = Replica(name, wal=wal)
    injector = FaultInjector(FaultPolicy())
    client = httpx.AsyncClient(timeout=10.0)

    # 用列表持有副本，是为了支持"重启"：重启时换掉对象，所有 handler 自动看到新的那个。
    holder = [replica]
    wal_dir = Path(data_dir) if data_dir else None

    # -- 后台反熵 ---------------------------------------------------------- #

    async def sync_once() -> int:
        applied = 0
        for peer_name, base in peers.items():
            plan = injector.plan(name, peer_name)
            if not plan.delivered:
                continue
            await injector.asleep(plan)
            try:
                response = await client.post(
                    f"{base}/internal/pull",
                    json={"vv": holder[0].vv.to_dict(), "limit": 200},
                )
                if response.status_code != 200:
                    continue
                applied += holder[0].merge_payload(response.json())
            except Exception:       # noqa: BLE001 - 对端不可达属于正常故障
                continue
        return applied

    async def sync_loop() -> None:
        while True:
            await asyncio.sleep(sync_interval)
            if app.state.paused:
                continue
            try:
                await sync_once()
            except asyncio.CancelledError:      # pragma: no cover
                raise
            except Exception:                    # noqa: BLE001
                pass

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        task = asyncio.create_task(sync_loop())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await client.aclose()
            holder[0].close()

    app = FastAPI(title=f"cart-replica-{name}", version="1.0.0", lifespan=lifespan)
    app.state.replica = replica
    app.state.injector = injector
    app.state.peers = peers
    app.state.cluster = sorted(set(cluster) | {name})
    app.state.quorum = len(app.state.cluster) // 2 + 1
    app.state.sync_once = sync_once
    app.state.paused = False

    # -- 健康与元信息 ------------------------------------------------------ #

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "name": name,
            "up": not holder[0].down,
            "ops": len(holder[0].log),
            "recovered_ops": holder[0].recovered_ops,
            "vv": holder[0].vv.to_dict(),
            "fault": injector.policy.describe(),
            "paused": app.state.paused,
        }

    @app.get("/cluster")
    def cluster_info() -> dict[str, Any]:
        return {
            "names": app.state.cluster,
            "self": name,
            "quorum": app.state.quorum,
            "peers": peers,
        }

    @app.get("/cluster/owner/{user_id}")
    def cluster_owner(user_id: str) -> dict[str, Any]:
        return {"user": user_id, "owner": owner_name(user_id, app.state.cluster)}

    # -- 购物车（AP） ------------------------------------------------------ #

    @app.get("/cart/{user_id}")
    def read_cart(user_id: str) -> dict[str, Any]:
        cart = holder[0].cart(user_id)
        return {
            "user": user_id,
            "items": cart.contents(),
            "total_qty": cart.total_quantity(),
            "checkout_version": cart.checkout_version,
            "vv": holder[0].vv.to_dict(),
            "served_by": name,
        }

    @app.post("/cart/{user_id}/items")
    def add_item(user_id: str, body: AddItem) -> dict[str, Any]:
        try:
            op = holder[0].add(user_id, body.sku, body.qty, request_id=body.request_id)
        except ReplicaDown as exc:
            raise HTTPException(status_code=503, detail=f"副本 {name} 不可用") from exc
        if injector.should_lose_response("client", name):
            # 已经生效并落盘，但回包丢了 —— 客户端会重试
            raise HTTPException(status_code=504, detail="响应在途中丢失（注入）")
        return {
            "op": str(op) if op else None,
            "request_id": body.request_id,
            "cart": holder[0].cart(user_id).contents(),
        }

    @app.delete("/cart/{user_id}/items/{sku}")
    def remove_item(user_id: str, sku: str, request_id: str | None = None) -> dict[str, Any]:
        try:
            op = holder[0].remove(user_id, sku, request_id=request_id)
        except ReplicaDown as exc:
            raise HTTPException(status_code=503, detail=f"副本 {name} 不可用") from exc
        return {"op": str(op) if op else None, "cart": holder[0].cart(user_id).contents()}

    # -- 结算（CP） -------------------------------------------------------- #

    @app.post("/cart/{user_id}/checkout")
    async def checkout(user_id: str, body: CheckoutBody) -> dict[str, Any]:
        owner = owner_name(user_id, app.state.cluster)
        if owner != name:
            raise HTTPException(
                status_code=409,
                detail=f"该用户的结算主副本是 {owner}，请把请求路由过去",
            )
        reachable = 1 + sum(
            1 for peer in peers if not injector.policy.blocks_link(name, peer)
        )
        if reachable < app.state.quorum:
            raise HTTPException(
                status_code=503,
                detail=f"只能联系到 {reachable}/{len(app.state.cluster)} 个副本，低于多数派",
            )
        try:
            await app.state.sync_once()      # 结算前读修复，保证凭据里的购物车是完整的
        except Exception:                     # noqa: BLE001
            pass
        try:
            receipt = holder[0].checkout(user_id, body.expected_version, request_id=body.request_id)
        except VersionConflict as exc:
            raise HTTPException(
                status_code=409,
                detail={"reason": "版本冲突", "expected": exc.expected, "current": exc.current},
            ) from exc
        except CheckoutUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return receipt.to_dict()

    # -- 副本间反熵 -------------------------------------------------------- #

    @app.post("/internal/pull")
    def pull(body: PullBody) -> dict[str, Any]:
        try:
            payload = holder[0].pull_payload(VersionVector.from_mapping(body.vv), limit=body.limit)
        except ReplicaDown as exc:
            raise HTTPException(status_code=503, detail=f"副本 {name} 不可用") from exc
        return payload

    # -- 故障注入 ---------------------------------------------------------- #

    @app.post("/admin/fault")
    def set_fault(body: FaultBody) -> dict[str, Any]:
        policy = injector.policy
        if body.heal:
            policy.heal()
        if body.latency_ms is not None:
            policy.latency_ms = body.latency_ms
        if body.jitter_ms is not None:
            policy.jitter_ms = body.jitter_ms
        if body.duplicate is not None:
            policy.duplicate = max(1, body.duplicate)
        if body.drop_rate is not None:
            policy.drop_rate = body.drop_rate
        if body.reorder_window_ms is not None:
            policy.reorder_window_ms = body.reorder_window_ms
        if body.drop_response_rate is not None:
            policy.drop_response_rate = body.drop_response_rate
        return {"fault": policy.describe()}

    @app.post("/admin/partition")
    def set_partition(body: PartitionBody) -> dict[str, Any]:
        for peer in body.peers:
            if body.block:
                injector.policy.block(name, peer)
            else:
                injector.policy.unblock(name, peer)
        return {"fault": injector.policy.describe(), "blocked": sorted(
            ",".join(sorted(pair)) for pair in injector.policy.blocks
        )}

    @app.post("/admin/down")
    def set_down(body: DownBody) -> dict[str, Any]:
        holder[0].set_down(body.down)
        return {"name": name, "down": holder[0].down}

    @app.get("/admin/state")
    def admin_state() -> dict[str, Any]:
        policy = injector.policy
        return {
            "name": name,
            "down": holder[0].down,
            "vv": holder[0].vv.to_dict(),
            "ops": len(holder[0].log),
            "duplicates_ignored": holder[0].duplicates_ignored,
            "carts": holder[0].state(),
            "lww": holder[0].lww_view(),
            "checkout_versions": {
                user: cart.checkout_version for user, cart in holder[0].carts.items()
            },
            "stats": injector.stats,
            "paused": app.state.paused,
            "clock_offset_ms": holder[0].clock.offset_ms,
            "blocked": [sorted(pair) for pair in policy.blocks],
            "policy": {
                "latency_ms": policy.latency_ms,
                "duplicate": policy.duplicate,
                "drop_rate": policy.drop_rate,
            },
        }

    @app.post("/admin/checkpoint")
    def admin_checkpoint() -> dict[str, Any]:
        holder[0].checkpoint()
        return {"name": name, "wal_bytes": holder[0].wal.size_bytes() if holder[0].wal else 0}

    @app.post("/admin/sync")
    async def admin_sync() -> dict[str, Any]:
        return {"applied": await sync_once()}

    # -- 运维与实验开关 ---------------------------------------------------- #

    def _swap(new_replica: Replica) -> None:
        holder[0] = new_replica
        app.state.replica = new_replica

    @app.post("/admin/restart")
    def admin_restart() -> dict[str, Any]:
        """模拟进程重启：丢掉内存，从 WAL 重新构造一个副本。"""
        old = holder[0]
        path = old.wal.path if old.wal is not None else None
        old.close()
        _swap(Replica(name, wal=path))
        return {
            "name": name,
            "recovered_ops": holder[0].recovered_ops,
            "ops": len(holder[0].log),
            "vv": holder[0].vv.to_dict(),
            "persistent": path is not None,
        }

    @app.post("/admin/wipe")
    def admin_wipe() -> dict[str, Any]:
        """清空状态，相当于一台刚加入集群的空节点。

        会同时**暂停自动反熵**，否则 0.5 秒后它就被别人补满了，
        "迁移到一半中断"这个实验根本观察不到。用 ``/admin/pause`` 恢复。
        """
        old = holder[0]
        old.close()
        path = wal_dir / f"{name}-{int(time.time() * 1000)}.wal" if wal_dir else None
        _swap(Replica(name, wal=path))
        app.state.paused = True
        return {"name": name, "ops": 0, "vv": {}, "paused": True}

    @app.post("/admin/pause")
    def admin_pause(body: PauseBody) -> dict[str, Any]:
        app.state.paused = body.paused
        return {"name": name, "paused": app.state.paused}

    @app.post("/admin/migrate")
    async def admin_migrate(body: MigrateBody) -> dict[str, Any]:
        """从某个对端分页拉数据；``pages`` 给了就有意中断，用来演示断点续传。"""
        base = peers.get(body.source)
        if base is None:
            raise HTTPException(status_code=400, detail=f"未知源副本 {body.source}")
        target = holder[0]
        total = 0
        for page in range(body.max_pages):
            response = await client.post(
                f"{base}/internal/pull",
                json={"vv": target.vv.to_dict(), "limit": body.page_size},
            )
            if response.status_code != 200:
                break
            payload = response.json()
            total += target.merge_payload(payload)
            if body.pages is not None and page + 1 >= body.pages:
                return {
                    "interrupted": True,
                    "applied": total,
                    "pages": page + 1,
                    "vv": target.vv.to_dict(),
                }
            if not payload.get("more"):
                break
        return {"interrupted": False, "applied": total, "vv": target.vv.to_dict()}

    @app.post("/admin/clock")
    def admin_clock(body: ClockBody) -> dict[str, Any]:
        """把本地时钟整体拨动（负数即倒退），用来演示"时间倒退"。"""
        holder[0].clock.set_offset(body.offset_ms)
        return {
            "name": name,
            "offset_ms": body.offset_ms,
            "wall_ms": holder[0].clock.wall_ms(),
            "hlc": str(holder[0].clock.now()),
        }

    @app.post("/admin/burst")
    def admin_burst(body: BurstBody) -> dict[str, Any]:
        """热点压测：一口气写 N 次，其中 ``hot_ratio`` 打在同一个 key 上。"""
        target = holder[0]
        if target.down:
            raise HTTPException(status_code=503, detail=f"副本 {name} 不可用")
        started = time.perf_counter()
        hot = 0
        credit = 0.0
        for index in range(body.count):
            credit += max(0.0, min(1.0, body.hot_ratio))
            is_hot = credit >= 1.0
            if is_hot:
                credit -= 1.0
                hot += 1
            sku = body.hot_sku if is_hot else f"冷门{index % 5}"
            target.add(body.user, sku, 1)
        elapsed = (time.perf_counter() - started) * 1000
        return {
            "count": body.count,
            "hot": hot,
            "elapsed_ms": round(elapsed, 1),
            "per_write_ms": round(elapsed / max(1, body.count), 3),
            "hot_qty": target.cart(body.user).quantity(body.hot_sku),
        }

    return app
