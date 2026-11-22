"""pytest 公共配置。

**为什么要覆盖 tmp_path**：默认的 tmp_path 落在系统 %TEMP%，而某些受限环境
（比如带文件沙箱的 CI）会禁止在系统临时目录创建文件，导致所有用例直接报
PermissionError。改成仓库内的 `.scratch/` 就与运行环境无关了。
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def tmp_path() -> Path:
    path = ROOT / ".scratch" / f"pytest-{uuid.uuid4().hex[:10]}"
    path.mkdir(parents=True, exist_ok=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
