# Kafka 事件

`producer.py` 发送交易事件；`consumer.py` 输出大额和支付失败告警。

```bash
uv add confluent-kafka
python work/kafka/producer.py --bootstrap kafka01:9092 --topic trade_events
python work/kafka/consumer.py --bootstrap kafka01:9092 --topic trade_events
```
