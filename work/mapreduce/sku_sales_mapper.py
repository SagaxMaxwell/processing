"""Hadoop Streaming Mapper：抽取 SKU 销量。

场景：
    电商 order-service 的订单明细按 JSON 行存储在 HDFS。Mapper 从已支付订单中
    抽取 sku_id 与 quantity，交给 Reducer 聚合。
"""

from __future__ import annotations

import json
import sys
from typing import Any


def parse_quantity(value: Any) -> int:
    """把订单数量转换为整数。

    Args:
        value: JSON 中的 quantity 字段。

    Returns:
        转换后的数量；无法转换时返回 0。
    """

    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def emit_sku_quantity(line: str) -> str | None:
    """从一行订单日志中生成 Map 输出。

    Args:
        line: 一行 JSON 订单日志。

    Returns:
        `sku_id\tquantity` 格式的字符串；无效记录返回 None。
    """

    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return None

    if event.get("event_type") != "order_paid":
        return None

    sku_id = event.get("sku_id")
    quantity = parse_quantity(event.get("quantity"))
    if not sku_id or quantity <= 0:
        return None
    return f"{sku_id}\t{quantity}"


def main() -> None:
    """读取标准输入并输出 Mapper 结果。"""

    for line in sys.stdin:
        output = emit_sku_quantity(line.strip())
        if output:
            print(output)


if __name__ == "__main__":
    main()
