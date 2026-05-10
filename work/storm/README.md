# Storm 使用场景：支付失败实时风控

## 非常具体的场景

在“支付风控系统”中，Kafka 主题 `trade_events` 持续写入支付结果事件：

```text
event_time|shop_id|order_id|status|amount
2026-05-09T10:01:01|S1008|O9001|PAY_FAILED|399.00
```

Storm 拓扑实时消费该主题：

- `KafkaSpout` 从 Kafka 拉取交易事件。
- `PaymentFailBolt` 只保留 `PAY_FAILED` 事件，并发射 `shop_id`。
- `ShopFailCountBolt` 按 `shop_id` 做字段分组，统计每个店铺的支付失败次数。

当某店铺短时间支付失败次数异常升高时，实际系统可以把结果写入 Redis、MySQL 或告警平台。Storm 适合这个场景，因为它面向低延迟流式计算，Bolt 可以持续处理 Tuple。

## 代码说明

`PaymentFailTopology.java` 是一个简洁拓扑模板，展示 KafkaSpout、Bolt、fieldsGrouping 的组合方式。

提交示例：

```bash
mvn package
storm jar target/risk-topology.jar PaymentFailTopology centos01:9092 trade_events
```
