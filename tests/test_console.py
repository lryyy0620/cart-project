"""控制台测试：聚合接口在副本不可达时必须优雅降级，而不是 500。"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from cart.console import create_console_app, parse_replicas


def test_parse_replicas() -> None:
    parsed = parse_replicas("r1=http://a:1, r2=http://b:2 ,bad,r3=http://c:3/")
    assert parsed == {"r1": "http://a:1", "r2": "http://b:2", "r3": "http://c:3"}


async def get(app, path: str):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        return await client.get(path)


@pytest.mark.asyncio
async def test_state_degrades_when_replicas_unreachable() -> None:
    app = create_console_app(
        {"r1": "http://127.0.0.1:59991", "r2": "http://127.0.0.1:59992", "r3": "http://127.0.0.1:59993"}
    )
    response = await get(app, "/api/state")
    assert response.status_code == 200
    payload = response.json()
    assert payload["cluster"] == ["r1", "r2", "r3"]
    assert payload["quorum"] == 2
    assert all(not replica["reachable"] for replica in payload["replicas"])
    assert len(payload["links"]) == 3


@pytest.mark.asyncio
async def test_dashboard_page_is_served() -> None:
    app = create_console_app({"r1": "http://127.0.0.1:59991"})
    response = await get(app, "/")
    assert response.status_code == 200
    assert "网络分区控制台" in response.text
    assert "/api/state" in response.text


@pytest.mark.asyncio
async def test_unknown_action_is_rejected() -> None:
    app = create_console_app({"r1": "http://127.0.0.1:59991"})
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        response = await client.post("/api/action", json={"target": "r1", "action": "nope"})
    assert response.status_code == 400
