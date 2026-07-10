# Kafka 事件总线

`producer.py` 发送交易事件，`consumer.py` 打印高金额和支付失败告警。

```bash
uv add confluent-kafka
python work/kafka/producer.py --bootstrap kafka01:9092 --topic trade_events
python work/kafka/consumer.py --bootstrap kafka01:9092 --topic trade_events
```
