# Spark 使用场景：O2O 零售门店实时 GMV 看板

## 非常具体的场景

在“O2O 零售系统”中，`pos-service` 会把门店收银事件写入 Kafka 主题 `store_orders`：

```json
{"event_time":"2026-05-09T10:01:03","store_id":"STORE-88","order_id":"P1001","amount":129.9}
```

数据团队需要实时看板展示每个门店最近 5 分钟 GMV，并允许数据延迟 10 分钟到达。Spark Structured Streaming 适合这个场景：

- 从 Kafka 连续读取 JSON。
- 用 watermark 处理迟到数据。
- 按窗口和门店聚合。
- 通过 checkpoint 保证故障恢复。

## 代码说明

`store_gmv_streaming.py` 是 PySpark Structured Streaming 示例。

运行示例：

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  work/spark/store_gmv_streaming.py \
  --bootstrap centos01:9092,centos02:9092 \
  --topic store_orders \
  --checkpoint hdfs:///warehouse/checkpoint/store_gmv
```
