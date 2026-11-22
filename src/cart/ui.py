"""终端输出小工具：中文对齐（全角按两列算）与简单表格。

单独抽出来是因为演示脚本和场景报告都要用，而且中文表格用 ``str.ljust``
必然错位——`ljust` 按字符数补空格，一个汉字却占两列。
"""

from __future__ import annotations

import unicodedata

__all__ = ["width", "pad", "clip", "hr", "table"]


def width(text: str) -> int:
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in text)


def pad(text: str, target: int) -> str:
    return text + " " * max(0, target - width(text))


def clip(text: str, limit: int) -> str:
    if width(text) <= limit:
        return text
    kept: list[str] = []
    used = 0
    for ch in text:
        w = 2 if unicodedata.east_asian_width(ch) in "WF" else 1
        if used + w > limit - 1:
            break
        kept.append(ch)
        used += w
    return "".join(kept) + "…"


def hr(title: str = "", size: int = 78) -> None:
    print()
    print("=" * size)
    if title:
        print(title)
        print("=" * size)


def table(headers: list[str], rows: list[list[str]], widths: list[int]) -> None:
    """``widths`` 对应除最后一列（结果列）以外的各列宽度。

    传错列数会直接抛错（``strict=True``），而不是静默错位。
    """
    total = sum(widths) + 8
    head = "".join(pad(h, w) for h, w in zip(headers[:-1], widths, strict=True)) + headers[-1]
    print(head)
    print("-" * total)
    for row in rows:
        cells = [pad(clip(str(value), w), w) for value, w in zip(row[:-1], widths, strict=True)]
        print("".join(cells) + str(row[-1]))
    print("-" * total)
