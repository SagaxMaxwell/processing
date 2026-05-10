"""Kafka 生产者：发送交易事件。

场景：
    支付风控系统中，order-service 将订单创建事件写入 Kafka 的 trade_events 主题，
    risk-service 后续按消费者组拉取并计算风险。
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class TradeEvent:
    """交易事件。

    Attributes:
        event_type: 事件类型，例如 order_created。
        order_id: 订单 ID。
        user_id: 用户 ID。
        city: 下单城市。
        amount: 订单金额。
        event_time: 事件产生时间戳。
    """

    event_type: str
    order_id: str
    user_id: str
    city: str
    amount: float
    event_time: int


def build_producer(bootstrap_servers: str):
    """创建 KafkaProducer。

    Args:
        bootstrap_servers: Kafka Broker 地址，多个地址用逗号分隔。

    Returns:
        kafka.KafkaProducer 实例。
    """

    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )


def send_event(bootstrap_servers: str, topic: str, event: TradeEvent) -> None:
    """发送单条交易事件。

    Args:
        bootstrap_servers: Kafka Broker 地址。
        topic: Kafka 主题名。
        event: 待发送事件。
    """

    producer = build_producer(bootstrap_servers)
    producer.send(topic, key=event.order_id, value=asdict(event)).get(timeout=10)
    producer.flush()
    producer.close()


def main() -> None:
    """解析命令行参数并发送示例事件。"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", default="trade_events")
    args = parser.parse_args()

    event = TradeEvent(
        event_type="order_created",
        order_id="O202605090001",
        user_id="U7788",
        city="Shanghai",
        amount=3580.0,
        event_time=int(time.time()),
    )
    send_event(args.bootstrap, args.topic, event)


if __name__ == "__main__":
    main()
