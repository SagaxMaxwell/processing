# 现代数据处理实践

只保留新项目默认选型：Kafka/KRaft、Debezium CDC、Flink SQL、Spark、Iceberg、Kubernetes。旧 PPT、Flume、MapReduce、Storm、ZooKeeper、HDFS、YARN、裸 Parquet 分区已删除或替换。

## 模块

| 模块 | 用途 | 入口 |
| --- | --- | --- |
| Kafka | 事件总线和风险消费 | `work/kafka/producer.py`、`work/kafka/consumer.py` |
| Connect | PostgreSQL CDC 入 Kafka | `work/connect/postgres-cdc.json` |
| Flink SQL | 支付失败窗口告警 | `work/flink/risk.sql` |
| Spark Batch | SKU 日销量写 Iceberg | `work/spark/sales.py` |
| Spark Streaming | 门店 5 分钟 GMV | `work/spark/gmv.py` |
| Iceberg | 湖仓库表初始化 | `work/lakehouse/catalog.sql` |
| Kubernetes | Spark Operator 作业 | `work/kubernetes/sparkapp.yaml` |

## 替换关系

| 旧内容 | 现代替代 |
| --- | --- |
| Flume | Debezium / Kafka Connect / 日志采集器 |
| MapReduce / Hadoop Streaming | Spark DataFrame / Flink SQL |
| Storm | Flink SQL |
| ZooKeeper | Kafka KRaft |
| HDFS / 裸 Parquet | 对象存储 + Iceberg/Paimon/Hudi |
| YARN | Spark on Kubernetes |
| Scala 入门 / 旧 PPT | 已删除 |

## 检查

```bash
uv add confluent-kafka pyspark
uv run python -m py_compile work/**/*.py
```

## 参考

[Kafka 4.3](https://kafka.apache.org/43/) · [KRaft](https://kafka.apache.org/43/getting-started/zk2kraft/) · [Kafka Connect](https://kafka.apache.org/43/kafka-connect/overview/) · [Debezium PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) · [Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html) · [Iceberg Spark](https://iceberg.apache.org/docs/latest/spark-configuration/) · [Spark Streaming](https://spark.apache.org/docs/latest/streaming/index.html) · [Flink](https://flink.apache.org/what-is-flink/flink-applications/)
