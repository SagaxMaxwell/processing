"""Hadoop Streaming Reducer：累加 SKU 销量。

场景：
    接收 Mapper 输出的 `sku_id\tquantity`，按 SKU 聚合每日销售件数。
"""

from __future__ import annotations

import sys
from collections.abc import Iterable


def parse_mapper_line(line: str) -> tuple[str, int] | None:
    """解析 Mapper 输出。

    Args:
        line: `sku_id\tquantity` 格式文本。

    Returns:
        SKU 和数量；格式错误时返回 None。
    """

    parts = line.rstrip("\n").split("\t")
    if len(parts) != 2:
        return None
    try:
        return parts[0], int(parts[1])
    except ValueError:
        return None


def reduce_sales(lines: Iterable[str]) -> Iterable[tuple[str, int]]:
    """对已按 SKU 排序的 Mapper 输出做聚合。

    Args:
        lines: Hadoop Streaming 传入的标准输入行。

    Yields:
        每个 SKU 的总销量。
    """

    current_sku = None
    current_total = 0

    for line in lines:
        parsed = parse_mapper_line(line)
        if parsed is None:
            continue
        sku_id, quantity = parsed

        if current_sku is not None and sku_id != current_sku:
            yield current_sku, current_total
            current_total = 0

        current_sku = sku_id
        current_total += quantity

    if current_sku is not None:
        yield current_sku, current_total


def main() -> None:
    """读取标准输入并输出 Reducer 结果。"""

    for sku_id, total in reduce_sales(sys.stdin):
        print(f"{sku_id}\t{total}")


if __name__ == "__main__":
    main()
