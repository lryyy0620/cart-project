"""预写日志（WAL）：一行一个 JSON 记录的追加文件，内嵌检查点。

设计目标（对应作业 topic 4「节点重启丢失内存状态」与 topic 9「磁盘写成功但响应丢失」）：

- **写日志先于回 ack**：只要客户端收到"成功"，这条操作就一定已经 fsync 落盘；
  进程被杀后重启，能从日志原样恢复。
- **检查点内嵌在同一个文件里**：恢复时顺序扫描，记住最后一个 checkpoint，
  再重放它之后的操作即可。不需要维护"日志段 + 快照文件"两套一致性逻辑，
  也就没有"快照写完、日志没截断"这种经典 bug 的土壤。
- **截断的行要能容忍**：写到一半掉电时最后一行可能不完整，读取时跳过即可
  （这条操作本来就没来得及回 ack，丢它对客户端不可见）。

注意：本实现不做日志裁剪（GC）。生产环境需要按大小分段、把 checkpoint 之前的
日志删掉，并在 checkpoint 里记录"已确认水位"，否则日志会无限增长。
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .codec import cart_from_json, cart_to_json, op_from_json, op_to_json
from .crdt import Op, VersionVector

__all__ = ["WriteAheadLog", "Recovered"]


class Recovered:
    """从 WAL 恢复出来的状态。"""

    def __init__(
        self,
        checkpoint: dict | None,
        ops: list[Op],
        checkpoints: int,
        torn_lines: int,
    ) -> None:
        self.checkpoint = checkpoint
        self.ops = ops
        self.checkpoints = checkpoints
        self.torn_lines = torn_lines

    @property
    def has_checkpoint(self) -> bool:
        return self.checkpoint is not None

    def base_state(
        self,
    ) -> tuple[dict, VersionVector, int, set[str], set]:
        """把检查点还原成 (carts, vv, counter, requests, applied)。"""
        from .crdt import Dot  # 局部导入避免循环

        if self.checkpoint is None:
            return {}, VersionVector.empty(), 0, set(), set()
        carts = {
            user: cart_from_json(payload)
            for user, payload in self.checkpoint.get("carts", {}).items()
        }
        vv = VersionVector.from_mapping(self.checkpoint.get("vv", {}))
        counter = int(self.checkpoint.get("counter", 0))
        requests = set(self.checkpoint.get("requests", []))
        applied = {Dot(str(r), int(s)) for r, s in self.checkpoint.get("applied", [])}
        return carts, vv, counter, requests, applied


class WriteAheadLog:
    """append-only 的 JSONL 日志。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a", encoding="utf-8", newline="\n")

    # -- 写 ---------------------------------------------------------------- #

    def append_op(self, op: Op) -> None:
        self._write({"type": "op", "op": op_to_json(op)})

    def append_checkpoint(
        self,
        carts: dict,
        vv: VersionVector,
        counter: int,
        requests: set[str],
        applied: set | None = None,
    ) -> None:
        self._write(
            {
                "type": "checkpoint",
                "counter": counter,
                "vv": vv.to_dict(),
                "requests": sorted(requests),
                "applied": sorted([dot.replica, dot.seq] for dot in (applied or set())),
                "carts": {user: cart_to_json(cart) for user, cart in carts.items()},
            }
        )

    def _write(self, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        self._handle.write(line + "\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())      # 落盘后才允许上层回 ack

    # -- 读 ---------------------------------------------------------------- #

    def scan(self) -> Recovered:
        """顺序扫描：记住最后一个 checkpoint，收集它之后的操作。"""
        checkpoint: dict | None = None
        ops: list[Op] = []
        checkpoints = 0
        torn = 0

        if not self.path.exists():
            return Recovered(None, [], 0, 0)

        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    torn += 1           # 掉电造成的半行，忽略
                    continue
                if record.get("type") == "checkpoint":
                    checkpoint = record
                    checkpoints += 1
                    ops = []            # 检查点之前的操作已被包含
                elif record.get("type") == "op":
                    ops.append(op_from_json(record["op"]))

        return Recovered(checkpoint, ops, checkpoints, torn)

    # -- 杂项 -------------------------------------------------------------- #

    def size_bytes(self) -> int:
        return self.path.stat().st_size if self.path.exists() else 0

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> "WriteAheadLog":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __str__(self) -> str:
        return f"WAL({self.path}, {self.size_bytes()} bytes)"
