# Kafka 使用场景：支付风控事件总线

## 非常具体的场景

在“支付风控系统”中：

- `order-service` 在用户下单后发送 `order_created` 事件到 Kafka。
- `payment-service` 在支付成功或失败后发送 `payment_result` 事件到 Kafka。
- `risk-service` 作为消费者组 `risk-online-group` 读取主题 `trade_events`，识别高金额、异地、失败次数异常等风险。

Kafka 的作用是削峰填谷：业务服务只负责快速写消息，风控服务可以按自己的消费能力拉取消息，并通过 offset 保证可恢复。

## 代码说明

- `order_event_producer.py`：模拟订单服务发送订单事件。
- `risk_event_consumer.py`：风控消费者组读取消息并提交 offset。

依赖示例：

```bash
uv add kafka-python
```

运行示例：

```bash
python work/kafka/order_event_producer.py --bootstrap centos01:9092 --topic trade_events
python work/kafka/risk_event_consumer.py --bootstrap centos01:9092 --topic trade_events
```
