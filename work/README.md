# work 模块索引

本目录按课件中的工具拆分，每个子目录都包含两类内容：

- `README.md`：具体业务场景、为什么用这个工具、运行方式。
- 示例代码或配置：尽量短小，保留真实集群参数位置，代码注释采用中文 Google 风格。

| 模块 | 场景摘要 | 代码入口 |
| --- | --- | --- |
| `hadoop` | 教育 SaaS 数仓夜间任务提交到 YARN，并等待任务完成 | `submit_yarn_pipeline.py` |
| `hdfs` | 物流 TMS 把订单、轨迹两个子模块日志归档到 HDFS ODS 层 | `archive_tms_logs.py` |
| `mapreduce` | 电商订单明细用 Hadoop Streaming 统计 SKU 销量 | `sku_sales_mapper.py`、`sku_sales_reducer.py` |
| `flume` | 交易系统直接采集 order-service、payment-service 日志到 HDFS/Kafka | `order_payment_logs_to_hdfs.conf`、`order_payment_logs_to_kafka.conf` |
| `kafka` | 支付风控系统生产订单事件，风控消费者组实时消费 | `order_event_producer.py`、`risk_event_consumer.py` |
| `zookeeper` | 推荐系统多实例定时任务通过 ZooKeeper 抢主并读取动态配置 | `leader_lock_job.py` |
| `storm` | 支付风控实时拓扑消费 Kafka，按店铺统计支付失败次数 | `PaymentFailTopology.java` |
| `spark` | O2O 零售看板用 Structured Streaming 统计门店 GMV | `store_gmv_streaming.py` |
| `scala` | 会员运营系统用 Scala case class 和模式匹配生成用户标签 | `UserTagExample.scala` |
