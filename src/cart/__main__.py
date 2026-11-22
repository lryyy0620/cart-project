"""``python -m cart`` 入口：默认运行全部故障场景。"""

from __future__ import annotations

import sys

from .runner import main

if __name__ == "__main__":
    sys.exit(main())
