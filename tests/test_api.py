"""HTTP 接口测试：用 ASGITransport 直连应用，不需要真的监听端口。"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from httpx import ASGITransport, AsyncClient

from cart.api import create_app
from cart.cluster import owner_name


def make_app(name: str = "r1", cluster: list[str] | None = None):
    return create_app(name=name, peers={}, cluster=cluster or ["r1"], sync_interval=9999)


async def request(app, method: str, url: str, **kwargs):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        async with app.router.lifespan_context(app):
            return await client.request(method, url, **kwargs)


@asynccontextmanager
async def session(app):
    """整个用例共用一个生命周期（否则每次请求结束都会把 WAL 关掉）。"""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        async with app.router.lifespan_context(app):
            yield client


@pytest.mark.asyncio
async def test_add_and_read_cart() -> None:
    app = make_app()
    response = await request(app, "POST", "/cart/alice/items",
                             json={"sku": "苹果", "qty": 2, "request_id": "req-1"})
    assert response.status_code == 200
    assert response.json()["cart"] == {"苹果": 2}

    response = await request(app, "GET", "/cart/alice")
    assert response.json()["items"] == {"苹果": 2}
    assert response.json()["total_qty"] == 2


@pytest.mark.asyncio
async def test_retry_with_same_request_id_is_idempotent() -> None:
    app = make_app()
    body = {"sku": "苹果", "qty": 2, "request_id": "same"}
    await request(app, "POST", "/cart/alice/items", json=body)
    await request(app, "POST", "/cart/alice/items", json=body)

    response = await request(app, "GET", "/cart/alice")
    assert response.json()["items"] == {"苹果": 2}


@pytest.mark.asyncio
async def test_checkout_conflict_on_second_attempt() -> None:
    app = make_app()
    await request(app, "POST", "/cart/alice/items", json={"sku": "苹果", "qty": 1})

    first = await request(app, "POST", "/cart/alice/checkout", json={"expected_version": 0})
    assert first.status_code == 200
    assert first.json()["version"] == 1

    second = await request(app, "POST", "/cart/alice/checkout", json={"expected_version": 0})
    assert second.status_code == 409
    assert second.json()["detail"]["current"] == 1


@pytest.mark.asyncio
async def test_checkout_rejected_when_not_owner() -> None:
    app = make_app(name="r1", cluster=["r1", "r2"])
    user = next(u for u in (f"u{i}" for i in range(50)) if owner_name(u, ["r1", "r2"]) == "r2")
    response = await request(app, "POST", f"/cart/{user}/checkout", json={"expected_version": 0})
    assert response.status_code == 409
    assert "r2" in str(response.json()["detail"])


@pytest.mark.asyncio
async def test_checkout_unavailable_when_partitioned() -> None:
    app = create_app(name="r1", peers={"r2": "http://r2"}, cluster=["r1", "r2", "r3"],
                     sync_interval=9999)
    user = next(u for u in (f"u{i}" for i in range(50))
                if owner_name(u, ["r1", "r2", "r3"]) == "r1")
    await request(app, "POST", "/admin/partition", json={"peers": ["r2", "r3"], "block": True})
    response = await request(app, "POST", f"/cart/{user}/checkout", json={"expected_version": 0})
    assert response.status_code == 503
    assert "多数派" in str(response.json()["detail"])


@pytest.mark.asyncio
async def test_cart_still_writable_during_partition() -> None:
    app = create_app(name="r1", peers={"r2": "http://r2"}, cluster=["r1", "r2", "r3"],
                     sync_interval=9999)
    await request(app, "POST", "/admin/partition", json={"peers": ["r2", "r3"], "block": True})
    response = await request(app, "POST", "/cart/alice/items", json={"sku": "牛奶", "qty": 1})
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_down_replica_rejects_writes() -> None:
    app = make_app()
    await request(app, "POST", "/admin/down", json={"down": True})
    response = await request(app, "POST", "/cart/alice/items", json={"sku": "苹果", "qty": 1})
    assert response.status_code == 503


@pytest.mark.asyncio
async def test_fault_endpoint_updates_policy() -> None:
    app = make_app()
    response = await request(app, "POST", "/admin/fault", json={"latency_ms": 250, "duplicate": 3})
    assert response.status_code == 200
    assert "250" in response.json()["fault"]
    assert "3" in response.json()["fault"]


@pytest.mark.asyncio
async def test_pull_returns_missing_ops() -> None:
    app = make_app()
    await request(app, "POST", "/cart/alice/items", json={"sku": "苹果", "qty": 2})

    response = await request(app, "POST", "/internal/pull", json={"vv": {}})
    payload = response.json()
    assert payload["type"] == "ops"
    assert len(payload["ops"]) == 1

    full = await request(app, "GET", "/admin/state")
    vv = full.json()["vv"]
    response = await request(app, "POST", "/internal/pull", json={"vv": vv})
    assert response.json()["ops"] == []


# --------------------------------------------------------------------------- #
# 实验开关：重启 / 清空 / 暂停 / 时钟 / 压测
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_restart_recovers_from_wal(tmp_path) -> None:
    app = create_app(name="r1", peers={}, cluster=["r1"],
                     data_dir=str(tmp_path), sync_interval=9999)
    async with session(app) as client:
        await client.post("/cart/alice/items", json={"sku": "苹果", "qty": 2})
        response = await client.post("/admin/restart")
        assert response.status_code == 200
        assert response.json()["recovered_ops"] == 1
        assert response.json()["persistent"] is True

        cart = await client.get("/cart/alice")
        assert cart.json()["items"] == {"苹果": 2}


@pytest.mark.asyncio
async def test_restart_without_wal_loses_everything() -> None:
    """纯内存模式下重启等于清空——正好对照"有 WAL 才叫持久化"。"""
    app = make_app()
    await request(app, "POST", "/cart/alice/items", json={"sku": "苹果", "qty": 2})
    await request(app, "POST", "/admin/restart")
    cart = await request(app, "GET", "/cart/alice")
    assert cart.json()["items"] == {}


@pytest.mark.asyncio
async def test_wipe_clears_state_and_pauses_sync() -> None:
    app = make_app()
    await request(app, "POST", "/cart/alice/items", json={"sku": "苹果", "qty": 2})

    response = await request(app, "POST", "/admin/wipe")
    assert response.json()["paused"] is True

    state = await request(app, "GET", "/admin/state")
    assert state.json()["carts"] == {}
    assert state.json()["paused"] is True


@pytest.mark.asyncio
async def test_pause_can_be_turned_off_again() -> None:
    app = make_app()
    await request(app, "POST", "/admin/pause", json={"paused": True})
    assert (await request(app, "GET", "/admin/state")).json()["paused"] is True

    await request(app, "POST", "/admin/pause", json={"paused": False})
    assert (await request(app, "GET", "/admin/state")).json()["paused"] is False


@pytest.mark.asyncio
async def test_clock_offset_is_applied() -> None:
    app = make_app()
    response = await request(app, "POST", "/admin/clock", json={"offset_ms": -60000})
    assert response.status_code == 200
    assert response.json()["offset_ms"] == -60000
    assert response.json()["hlc"]


@pytest.mark.asyncio
async def test_burst_writes_hot_key() -> None:
    app = make_app()
    response = await request(app, "POST", "/admin/burst",
                             json={"count": 20, "hot_ratio": 0.5, "hot_sku": "手机"})
    payload = response.json()
    assert payload["count"] == 20
    assert payload["hot"] == 10
    assert payload["hot_qty"] == 10
