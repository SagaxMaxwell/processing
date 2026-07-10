"""统计门店 5 分钟 GMV。"""

from __future__ import annotations

import argparse


def build_query(bootstrap_servers: str, topic: str, checkpoint: str):
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", default="store_orders")
    parser.add_argument("--checkpoint", required=True)
    args = parser.parse_args()
    query = build_query(args.bootstrap, args.topic, args.checkpoint)
    query.awaitTermination()


if __name__ == "__main__":
    main()
