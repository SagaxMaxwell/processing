---
date created: 2026-07-10T15:32
date updated: 2026-10-26T13:18
---

# Spark 作业

- [[processing/work/spark/sales.ipynb|SKU 日销量与 Iceberg]]
- [[processing/work/spark/gmv.ipynb|门店实时 GMV]]

## 环境

需要 Java、Spark 和相关服务。下面命令从 `processing` 根目录执行；`SPARK_VERSION` 使用实际 Spark 版本，`ICEBERG_VERSION` 使用与 Spark、Scala 匹配的 Iceberg 版本。示例按 Spark 4.0 / Scala 2.13 的 Iceberg 连接器命名，其他版本应调整坐标。

S3 输入还需要与 Hadoop 版本匹配的 S3A 连接器、凭据及存储配置。Notebook 保留本地检查，外部作业默认不启动。

## Notebook 运行

Kafka 流处理可通过启动参数加载连接器，再打开 `gmv.ipynb`。

```bash
export SPARK_VERSION=4.1.2
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION} pyspark-shell"
uv run --with jupyterlab --with "pyspark==${SPARK_VERSION}" jupyter lab
```

运行 Iceberg 教程时，改用下一节的 `--packages` 和 `--conf` 参数设置 `PYSPARK_SUBMIT_ARGS`，末尾保留 `pyspark-shell`；重启内核后生效。

## 导出并提交

`spark-submit` 接收 Python 脚本。Notebook 是维护源，提交时导出到临时目录，不在仓库保留重复脚本。

```bash
mkdir -p /tmp/processing-jobs
uv run --with nbconvert jupyter nbconvert --to script work/spark/sales.ipynb work/spark/gmv.ipynb --output-dir /tmp/processing-jobs
```

先设置 `ICEBERG_VERSION`，再提交批处理。

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:${ICEBERG_VERSION} \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=rest \
  --conf spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.lakehouse.warehouse=s3://lakehouse/warehouse \
  /tmp/processing-jobs/sales.py --input 's3a://raw/orders/dt=2026-07-10/*.json' \
  --table lakehouse.dws.sku_sales_daily --date 2026-07-10
```

实时任务会持续运行，停止时中断进程。

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION} \
  /tmp/processing-jobs/gmv.py --bootstrap kafka01:9092,kafka02:9092 \
  --topic store_orders --checkpoint s3a://lakehouse/checkpoints/store_gmv
```
