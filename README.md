# 大数据处理工具实践整理

本项目根据 `material/` 下的课件，把 Hadoop 生态中的常用工具整理到 `work/` 目录。每个工具都补充了一个非常具体的业务场景，并提供可直接阅读和改造的简洁代码或配置。

## 模块入口

| 工具 | 具体场景 | 入口 |
| --- | --- | --- |
| Hadoop/YARN | 教育 SaaS 夜间数仓任务提交和监控 | `work/hadoop/submit_yarn_pipeline.py` |
| HDFS | 物流 TMS 归档 order-service、tracking-service 两个子模块日志 | `work/hdfs/archive_tms_logs.py` |
| MapReduce | 电商订单明细按 SKU 统计日销量 | `work/mapreduce/sku_sales_mapper.py`、`work/mapreduce/sku_sales_reducer.py` |
| Flume | 交易系统直接采集 order-service、payment-service 日志到 HDFS/Kafka | `work/flume/*.conf` |
| Kafka | 支付风控系统的交易事件生产与消费 | `work/kafka/order_event_producer.py`、`work/kafka/risk_event_consumer.py` |
| ZooKeeper | 推荐系统多实例定时任务抢主和读取动态配置 | `work/zookeeper/leader_lock_job.py` |
| Storm | 实时消费 Kafka，按店铺统计支付失败次数 | `work/storm/PaymentFailTopology.java` |
| Spark | O2O 零售实时统计门店 5 分钟 GMV | `work/spark/store_gmv_streaming.py` |
| Scala | 会员运营系统用 case class 和模式匹配生成用户标签 | `work/scala/UserTagExample.scala` |

## 目录说明

- `material/`：原始 PPT 课件。
- `dataset/`：示例数据。
- `work/`：按工具拆分后的场景说明与代码。

## 运行说明

多数示例需要真实 Hadoop、Kafka、ZooKeeper、Spark 或 Storm 集群才能运行。Python 示例把第三方依赖放在函数内部导入，因此可以先做语法检查；运行真实任务前再安装对应依赖并替换集群地址。

常用依赖安装示例：

```bash
uv add kafka-python kazoo pyspark
```

Python 语法检查示例：

```bash
uv run python -m py_compile work/**/*.py
```
