#!/usr/bin/env python
"""HTTP 端到端演示：对已经启动的 3 个副本跑一遍"分区 → 写入 → 合并 → 结算"。

用法：

    python scripts/demo_http.py
    python scripts/demo_http.py --urls http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003

先启动副本（二选一）：

    docker compose up --build -d
    pwsh scripts/run_local.ps1
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cart.ui import hr, table  # noqa: E402

DEFAULT_URLS = "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003"
USER = "alice"


class Checker:
    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str, bool]] = []

    def check(self, name: str, actual: object, expected: object) -> bool:
        ok = actual == expected
        self.rows.append((name, str(expected), str(actual), ok))
        return ok

    def report(self) -> bool:
        rows = [[n, e, a, "PASS" if ok else "FAIL"] for n, e, a, ok in self.rows]
        table(["检查项", "预期", "实际", ""], rows, [34, 30, 30])
        passed = sum(1 for row in self.rows if row[3])
        print(f"通过 {passed}/{len(self.rows)}")
        return passed == len(self.rows)


def wait_until(predicate, timeout: float = 8.0, interval: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", default=DEFAULT_URLS)
    args = parser.parse_args()
    urls = [u.strip().rstrip("/") for u in args.urls.split(",") if u.strip()]

    client = httpx.Client(timeout=10.0)
    checker = Checker()

    hr("1) 探测副本")
    names: dict[str, str] = {}
    for url in urls:
        info = client.get(f"{url}/health").json()
        names[info["name"]] = url
        print(f"  {info['name']} @ {url}  up={info['up']}  ops={info['ops']}")
    if len(names) < 3:
        print("需要 3 个副本，请先 docker compose up 或 pwsh scripts/run_local.ps1")
        return 2

    owner_map = client.get(f"{urls[0]}/cluster").json()
    print(f"  成员={owner_map['names']} 多数派={owner_map['quorum']}")

    def cart(name: str) -> dict[str, int]:
        return client.get(f"{names[name]}/cart/{USER}").json()["items"]

    def add(name: str, sku: str, qty: int) -> bool:
        rid = uuid.uuid4().hex
        response = client.post(
            f"{names[name]}/cart/{USER}/items",
            json={"sku": sku, "qty": qty, "request_id": rid},
        )
        return response.status_code == 200

    hr("2) 正常情况下双端写入")
    add("r1", "咖啡豆", 2)
    add("r3", "燕麦奶", 1)
    converged = wait_until(lambda: cart("r1") == cart("r2") == cart("r3"))
    print(f"  r1={cart('r1')}  r2={cart('r2')}  r3={cart('r3')}")
    checker.check("无故障时正常收敛", converged, True)

    hr("3) 制造分区：{r1, r2} | {r3}")
    for name in ("r1", "r2"):
        client.post(f"{names[name]}/admin/partition", json={"peers": ["r3"], "block": True})
    client.post(f"{names['r3']}/admin/partition", json={"peers": ["r1", "r2"], "block": True})

    add("r1", "苹果", 1)
    add("r3", "苹果", 2)
    time.sleep(1.5)
    print(f"  r1={cart('r1')}  r3={cart('r3')}")
    checker.check("分区期间两侧视图不同", cart("r1") != cart("r3"), True)
    checker.check("分区期间 r1 仍可写", cart("r1").get("苹果"), 1)

    hr("4) 恢复网络，并让每条消息重复投递三份")
    for name in ("r1", "r2"):
        client.post(f"{names[name]}/admin/partition", json={"peers": ["r3"], "block": False})
    client.post(f"{names['r3']}/admin/partition", json={"peers": ["r1", "r2"], "block": False})
    for name in names:
        client.post(f"{names[name]}/admin/fault", json={"duplicate": 3})

    merged = wait_until(lambda: cart("r1") == cart("r2") == cart("r3"))
    print(f"  r1={cart('r1')}  r2={cart('r2')}  r3={cart('r3')}")
    checker.check("恢复后三副本一致", merged, True)
    checker.check("两端添加的商品都在", cart("r1"), {"咖啡豆": 2, "燕麦奶": 1, "苹果": 3})
    checker.check("重复投递没有翻倍", cart("r1").get("苹果"), 3)

    hr("5) 结算 CAS：两个客户端拿同一个版本号")
    owner = client.get(f"{names['r1']}/cluster/owner/{USER}").json()["owner"]
    print(f"  用户 {USER} 的结算主副本是 {owner}")
    first = client.post(f"{names[owner]}/cart/{USER}/checkout", json={"expected_version": 0})
    second = client.post(f"{names[owner]}/cart/{USER}/checkout", json={"expected_version": 0})
    print(f"  第一次：{first.status_code}  第二次：{second.status_code}")
    checker.check("第一次结算成功", first.status_code, 200)
    checker.check("第二次版本冲突", second.status_code, 409)

    hr("6) 分区时结算被拒绝，但购物车照常可写")
    client.post(f"{names[owner]}/admin/partition", json={"peers": [n for n in names if n != owner]})
    for name in names:
        if name != owner:
            client.post(f"{names[name]}/admin/partition", json={"peers": [owner]})
    checkout = client.post(f"{names[owner]}/cart/{USER}/checkout",
                           json={"expected_version": first.json().get("version", 1)})
    writable = add(owner, "牛奶", 1)
    checker.check("结算被拒绝（CP）", checkout.status_code, 503)
    checker.check("购物车仍可写（AP）", writable, True)

    ok = checker.report()
    print("\n结果：" + ("全部通过 ✅" if ok else "存在失败项 ❌"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
