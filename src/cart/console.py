"""可视化控制台：一个独立的进程，汇总三个副本的状态并提供操作入口。

为什么不把页面塞进副本进程：因为"杀掉一个副本"是这个演示的核心操作之一，
如果页面本身跑在那个副本里，一按宕机页面就没了。控制台独立之后，
三个副本随便杀，页面始终在。

启动：

    python -m cart.console                       # 默认连 127.0.0.1:8001-8003
    REPLICAS="r1=http://r1:8000,r2=http://r2:8000" python -m cart.console

浏览器打开 http://127.0.0.1:8010
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .cluster import owner_name

__all__ = ["create_console_app", "parse_replicas"]

STATIC = Path(__file__).parent / "static" / "console.html"
DEFAULT_REPLICAS = "r1=http://127.0.0.1:8001,r2=http://127.0.0.1:8002,r3=http://127.0.0.1:8003"


def parse_replicas(raw: str) -> dict[str, str]:
    replicas: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        name, url = item.split("=", 1)
        replicas[name.strip()] = url.strip().rstrip("/")
    return replicas


class Action(BaseModel):
    target: str = ""
    action: str
    params: dict[str, Any] = Field(default_factory=dict)


def create_console_app(replicas: dict[str, str] | None = None) -> FastAPI:
    replicas = replicas or parse_replicas(os.environ.get("REPLICAS", DEFAULT_REPLICAS))

    app = FastAPI(title="cart-console", version="1.0.0")
    app.state.replicas = replicas

    def client() -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=3.0)

    # -- 状态汇总 ---------------------------------------------------------- #

    async def snapshot_replica(name: str, url: str) -> dict[str, Any]:
        try:
            async with client() as http:
                response = await http.get(f"{url}/admin/state")
                response.raise_for_status()
                data = response.json()
            return {"name": name, "url": url, "reachable": True, **data}
        except Exception:  # noqa: BLE001 - 拉不到就是"不可达"，属于正常状态
            return {
                "name": name,
                "url": url,
                "reachable": False,
                "down": True,
                "ops": 0,
                "vv": {},
                "carts": {},
                "fault": "不可达",
                "blocked": [],
                "duplicates_ignored": 0,
                "stats": {},
            }

    @app.get("/api/state")
    async def state() -> dict[str, Any]:
        names = sorted(replicas)
        items = await _gather([snapshot_replica(n, replicas[n]) for n in names])

        blocked = {tuple(sorted(pair)) for item in items for pair in item.get("blocked", [])}
        links = [
            {
                "a": names[i],
                "b": names[j],
                "blocked": (names[i], names[j]) in blocked,
            }
            for i in range(len(names))
            for j in range(i + 1, len(names))
        ]

        users = sorted({user for item in items for user in item.get("carts", {})})
        return {
            "cluster": names,
            "quorum": len(names) // 2 + 1,
            "replicas": items,
            "links": links,
            "users": users,
        }

    # -- 操作转发 ---------------------------------------------------------- #

    async def call(name: str, method: str, path: str, body: Any | None = None) -> dict[str, Any]:
        base = replicas.get(name)
        if base is None:
            raise HTTPException(status_code=400, detail=f"未知副本 {name}")
        try:
            async with client() as http:
                response = await http.request(method, f"{base}{path}", json=body)
            try:
                data = response.json()
            except ValueError:
                data = response.text
            return {"ok": response.status_code < 400, "status": response.status_code, "data": data}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "status": 0, "data": f"{type(exc).__name__}: {exc}"}

    @app.post("/api/action")
    async def action(body: Action) -> dict[str, Any]:
        """控制台的动作白名单。

        只转发下面这几类，参数逐项显式取出再拼 payload——
        等价于给浏览器加了一层"只能做这些事"的约束，而不是透传任意请求。
        """
        params = body.params
        kind = body.action

        if kind == "down":
            return await call(body.target, "POST", "/admin/down", {"down": True})
        if kind == "up":
            return await call(body.target, "POST", "/admin/down", {"down": False})
        if kind == "checkpoint":
            return await call(body.target, "POST", "/admin/checkpoint")
        if kind == "restart":
            return await call(body.target, "POST", "/admin/restart")
        if kind == "wipe":
            return await call(body.target, "POST", "/admin/wipe")
        if kind == "pause":
            return await call(body.target, "POST", "/admin/pause",
                              {"paused": bool(params.get("paused", True))})

        if kind == "migrate":
            return await call(body.target, "POST", "/admin/migrate", {
                "source": params.get("source"),
                "page_size": int(params.get("page_size", 5)),
                "pages": params.get("pages"),
            })

        if kind == "clock":
            return await call(body.target, "POST", "/admin/clock",
                              {"offset_ms": int(params.get("offset_ms", 0))})

        if kind == "burst":
            return await call(body.target, "POST", "/admin/burst", {
                "count": int(params.get("count", 200)),
                "hot_ratio": float(params.get("hot_ratio", 0.5)),
                "hot_sku": params.get("hot_sku", "手机"),
                "user": params.get("user", "alice"),
            })

        if kind == "add":
            user = params.get("user", "alice")
            return await call(body.target, "POST", f"/cart/{user}/items", {
                "sku": params.get("sku", "商品"),
                "qty": int(params.get("qty", 1)),
                "request_id": params.get("request_id"),
            })
        if kind == "remove":
            user = params.get("user", "alice")
            return await call(body.target, "DELETE", f"/cart/{user}/items/{params.get('sku', '')}")

        if kind == "checkout":
            user = params.get("user", "alice")
            # 结算必须打到该用户的主副本，控制台替客户端做一次一致性哈希路由
            owner = owner_name(user, sorted(replicas))
            target = owner if owner in replicas else body.target
            result = await call(target, "POST", f"/cart/{user}/checkout", {
                "expected_version": int(params.get("expected_version", 0)),
            })
            result["owner"] = owner
            result["target"] = target
            return result

        if kind == "fault":
            payload = {k: v for k, v in params.items() if k in {
                "latency_ms", "jitter_ms", "duplicate", "drop_rate",
                "reorder_window_ms", "drop_response_rate", "heal",
            }}
            targets = params.get("targets") or list(replicas)
            results = [await call(name, "POST", "/admin/fault", payload) for name in targets]
            return {"ok": all(r["ok"] for r in results), "results": results}

        if kind == "partition":
            a, b = params.get("a"), params.get("b")
            blocked = bool(params.get("blocked", True))
            if a not in replicas or b not in replicas:
                raise HTTPException(status_code=400, detail="分区需要两个已知副本")
            first = await call(a, "POST", "/admin/partition", {"peers": [b], "block": blocked})
            second = await call(b, "POST", "/admin/partition", {"peers": [a], "block": blocked})
            return {"ok": first["ok"] and second["ok"], "results": [first, second]}

        raise HTTPException(status_code=400, detail=f"未知操作 {kind}")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"name": "console", "replicas": sorted(replicas), "ok": True}

    @app.get("/")
    def index() -> FileResponse:
        if not STATIC.exists():  # pragma: no cover
            raise HTTPException(status_code=500, detail="静态页面缺失")
        return FileResponse(STATIC, media_type="text/html; charset=utf-8")

    return app


async def _gather(coroutines: list) -> list[dict[str, Any]]:
    import asyncio

    return await asyncio.gather(*coroutines)


def main() -> None:
    port = int(os.environ.get("CONSOLE_PORT", "8010"))
    uvicorn.run(create_console_app(), host="127.0.0.1", port=port)


if __name__ == "__main__":
    main()
