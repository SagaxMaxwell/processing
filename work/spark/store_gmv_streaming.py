"""Spark Structured Streaming：统计门店实时 GMV。

场景：
    O2O 零售系统的 pos-service 将门店收银订单写入 Kafka。Spark 按 5 分钟窗口
    统计每个门店 GMV，结果可替换为写入 MySQL、ClickHouse 或 Kafka。
"""

from __future__ import annotations

import argparse


def build_query(bootstrap_servers: str, topic: str, checkpoint: str):
    """构建 Structured Streaming 查询。

    Args:
        bootstrap_servers: Kafka Broker 地址。
        topic: Kafka 主题名。
        checkpoint: checkpoint 目录。

    Returns:
        StreamingQuery 实例。
    """

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, sum as spark_sum, to_timestamp, window
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    spark = SparkSession.builder.appName("store-gmv-streaming").getOrCreate()
    schema = StructType(
        [
            StructField("event_time", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("amount", DoubleType(), False),
        ]
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp("event_time"))
    )

    gmv = (
        events.withWatermark("event_ts", "10 minutes")
        .groupBy(window("event_ts", "5 minutes"), "store_id")
        .agg(spark_sum("amount").alias("gmv"))
    )

    return (
        gmv.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint)
        .start()
    )


def main() -> None:
    """解析命令行参数并启动流任务。"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", default="store_orders")
    parser.add_argument("--checkpoint", required=True)
    args = parser.parse_args()
    query = build_query(args.bootstrap, args.topic, args.checkpoint)
    query.awaitTermination()


if __name__ == "__main__":
    main()
