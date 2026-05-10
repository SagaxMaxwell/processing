"""Kafka 消费者：实时识别交易风险。

场景：
    risk-service 订阅 trade_events 主题。单笔金额超过阈值的订单会打印为风险事件，
    实际系统中可替换为写 Redis、MySQL 或告警平台。
"""

from __future__ import annotations

import argparse
import json
from typing import Any


def is_high_risk(event: dict[str, Any], amount_threshold: float) -> bool:
    """判断交易事件是否高风险。

    Args:
        event: Kafka 消息反序列化后的事件。
        amount_threshold: 高金额风险阈值。

    Returns:
        True 表示命中风险规则，False 表示未命中。
    """

    return float(event.get("amount", 0)) >= amount_threshold


def consume_events(bootstrap_servers: str, topic: str, group_id: str) -> None:
    """消费 Kafka 事件并提交 offset。

    Args:
        bootstrap_servers: Kafka Broker 地址，多个地址用逗号分隔。
        topic: Kafka 主题名。
        group_id: 消费者组 ID。
    """

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    for message in consumer:
        event = message.value
        if is_high_risk(event, amount_threshold=3000.0):
            print(f"高风险订单: {event['order_id']} amount={event['amount']}")
        consumer.commit()


def main() -> None:
    """解析命令行参数并启动消费者。"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", default="trade_events")
    parser.add_argument("--group-id", default="risk-online-group")
    args = parser.parse_args()
    consume_events(args.bootstrap, args.topic, args.group_id)


if __name__ == "__main__":
    main()
