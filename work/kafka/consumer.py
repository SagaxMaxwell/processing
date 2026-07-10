"""消费 Kafka 事件并打印风险告警。"""

from __future__ import annotations

import argparse
import json
from typing import Any


def risk_reasons(event: dict[str, Any], amount_threshold: float) -> list[str]:
    reasons = []
    amount = float(event.get("amount", 0))
    if amount >= amount_threshold:
        reasons.append("high_amount")
    if event.get("event_type") == "payment_result" and event.get("status") == "PAY_FAILED":
        reasons.append("payment_failed")
    return reasons


def consume_events(bootstrap_servers: str, topic: str, group_id: str) -> None:
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "client.id": "processing-demo-consumer",
        }
    )

    consumer.subscribe([topic])
    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            event = json.loads(message.value().decode("utf-8"))
            reasons = risk_reasons(event, amount_threshold=3000.0)
            if reasons:
                alert = {
                    "order_id": event.get("order_id"),
                    "shop_id": event.get("shop_id"),
                    "amount": event.get("amount"),
                    "reasons": reasons,
                }
                print(json.dumps(alert, ensure_ascii=False))
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", default="trade_events")
    parser.add_argument("--group-id", default="risk-online-group")
    args = parser.parse_args()
    consume_events(args.bootstrap, args.topic, args.group_id)


if __name__ == "__main__":
    main()
