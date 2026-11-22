#!/usr/bin/env python
"""本地启动/停止三个副本 + 可视化控制台（不需要 Docker，也不碰 PowerShell 执行策略）。

    python scripts/local.py start     # r1/r2/r3 → 8001-8003，控制台 → 8010
    python scripts/local.py status
    python scripts/local.py stop
    python scripts/local.py reset     # 停掉并清空 data/

启动后浏览器打开 http://127.0.0.1:8010
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
PORTS = {"r1": 8001, "r2": 8002, "r3": 8003}
CONSOLE_PORT = 8010
PID_FILE = DATA / "pids.json"


def python_exe() -> str:
    candidate = ROOT / ".venv" / "Scripts" / "python.exe"
    return str(candidate) if candidate.exists() else sys.executable


def base_env() -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": str(ROOT / "src"),
            "PYTHONIOENCODING": "utf-8",
        }
    )
    return env


def spawn(args: list[str], env: dict[str, str], log_name: str) -> int:
    log = (DATA / "logs" / log_name).open("a", encoding="utf-8")
    process = subprocess.Popen(  # noqa: S603 - 参数完全由本文件决定
        args, cwd=str(ROOT), env=env, stdout=log, stderr=log
    )
    return process.pid


def start() -> int:
    DATA.mkdir(parents=True, exist_ok=True)
    (DATA / "logs").mkdir(parents=True, exist_ok=True)

    if PID_FILE.exists():
        running = [name for name, port in PORTS.items() if alive(port)]
        if running or alive(CONSOLE_PORT):
            print(f"已有副本在运行：{running}，先执行 stop")
            return 1
        PID_FILE.unlink(missing_ok=True)

    pids: dict[str, int] = {}

    for name, port in PORTS.items():
        others = [other for other in PORTS if other != name]
        env = base_env()
        env.update(
            {
                "REPLICA_NAME": name,
                "CLUSTER": ",".join(PORTS),
                "PEERS": ",".join(f"{o}=http://127.0.0.1:{PORTS[o]}" for o in others),
                "DATA_DIR": str(DATA / name),
                "SYNC_INTERVAL": "0.5",
            }
        )
        (DATA / name).mkdir(parents=True, exist_ok=True)
        pids[name] = spawn(
            [python_exe(), "-m", "uvicorn", "cart.api:create_app", "--factory",
             "--host", "127.0.0.1", "--port", str(port)],
            env,
            f"{name}.log",
        )
        print(f"启动 {name} (pid {pids[name]}) → http://127.0.0.1:{port}")

    console_env = base_env()
    console_env.update(
        {
            "REPLICAS": ",".join(f"{n}=http://127.0.0.1:{PORTS[n]}" for n in PORTS),
            "CONSOLE_PORT": str(CONSOLE_PORT),
        }
    )
    pids["console"] = spawn(
        [python_exe(), "-m", "uvicorn", "cart.console:create_console_app", "--factory",
         "--host", "127.0.0.1", "--port", str(CONSOLE_PORT)],
        console_env,
        "console.log",
    )
    print(f"启动 控制台 (pid {pids['console']}) → http://127.0.0.1:{CONSOLE_PORT}")

    PID_FILE.write_text(json.dumps(pids), encoding="utf-8")

    for _ in range(40):
        if all(alive(port) for port in PORTS.values()) and alive(CONSOLE_PORT):
            print("\n全部就绪：")
            status()
            print(f"\n浏览器打开 http://127.0.0.1:{CONSOLE_PORT}")
            return 0
        time.sleep(0.5)

    print("\n启动超时，请查看 data/logs/*.log", file=sys.stderr)
    return 1


def alive(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError):
        return False


def status() -> int:
    healthy = 0
    for name, port in PORTS.items():
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
                info = json.loads(response.read().decode("utf-8"))
            print(f"  {name}: up={info['up']} ops={info['ops']} vv={info['vv']} {info['fault']}")
            healthy += 1
        except (urllib.error.URLError, OSError):
            print(f"  {name}: 未启动（{port}）")

    console = "在线" if alive(CONSOLE_PORT) else "未启动"
    print(f"  控制台: {console} → http://127.0.0.1:{CONSOLE_PORT}")
    print(f"在线 {healthy}/3")
    return 0 if healthy == 3 else 1


def stop() -> int:
    if not PID_FILE.exists():
        print("没有进程记录（data/pids.json）")
        return 0
    pids = json.loads(PID_FILE.read_text(encoding="utf-8"))
    for name, pid in pids.items():
        try:
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)
            print(f"已停止 {name} (pid {pid})")
        except OSError:
            print(f"{name} (pid {pid}) 已经不在")
    PID_FILE.unlink(missing_ok=True)
    return 0


def reset() -> int:
    stop()
    shutil.rmtree(DATA, ignore_errors=True)
    print("已清空 data/")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="本地三副本 + 控制台")
    parser.add_argument("action", choices=["start", "stop", "status", "restart", "reset"])
    args = parser.parse_args(argv)
    if args.action == "start":
        return start()
    if args.action == "stop":
        return stop()
    if args.action == "status":
        return status()
    if args.action == "reset":
        return reset()
    stop()
    time.sleep(1)
    return start()


if __name__ == "__main__":
    raise SystemExit(main())
