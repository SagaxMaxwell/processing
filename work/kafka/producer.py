"""发送交易事件到 Kafka。"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(frozen=True)
class TradeEvent:
    event_type: str
    event_time: str
    order_id: str
    user_id: str
    shop_id: str
    city: str
    status: str
    amount: float


def build_producer(bootstrap_servers: str):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 5,
            "linger.ms": 20,
            "compression.type": "zstd",
            "client.id": "processing-demo-producer",
        }
    )


def send_events(bootstrap_servers: str, topic: str, events: list[TradeEvent]) -> None:
    producer = build_producer(bootstrap_servers)
    for event in events:
        producer.produce(
            topic,
            key=event.order_id.encode("utf-8"),
            value=json.dumps(asdict(event), ensure_ascii=False).encode("utf-8"),
        )
        producer.poll(0)
    remaining = producer.flush(timeout=10)
    if remaining:
        raise RuntimeError(f"仍有 {remaining} 条消息未发送完成")


def build_sample_events() -> list[TradeEvent]:
    event_time = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
    return [
        TradeEvent(
            event_type="order_paid",
            event_time=event_time,
            order_id="O202605090001",
            user_id="U7788",
            shop_id="S1008",
            city="Shanghai",
            status="PAY_SUCCESS",
            amount=3580.0,
        ),
        TradeEvent(
            event_type="payment_result",
            event_time=event_time,
            order_id="O202605090002",
            user_id="U8821",
            shop_id="S1008",
            city="Beijing",
            status="PAY_FAILED",
            amount=5200.0,
        ),
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", default="trade_events")
    args = parser.parse_args()

    send_events(args.bootstrap, args.topic, build_sample_events())


if __name__ == "__main__":
    main()
