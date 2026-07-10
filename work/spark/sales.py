"""汇总 SKU 日销量并写入 Iceberg。"""

from __future__ import annotations

import argparse
from typing import Any


def amount_column(raw: Any) -> Any:
    from pyspark.sql.functions import col

    if "pay_amount" in raw.columns:
        return col("pay_amount")
    return col("amount")


def build_sales_table(spark: Any, input_path: str, biz_date: str) -> Any:
    from pyspark.sql.functions import col, lit, sum as spark_sum

    raw = spark.read.json(input_path)
    paid_orders = raw.filter(col("event_type").isin("order_paid", "payment_result"))
    return (
        paid_orders.select(
            col("sku_id"),
            col("quantity").cast("long").alias("quantity"),
            amount_column(raw).cast("double").alias("amount"),
        )
        .filter(col("sku_id").isNotNull() & (col("quantity") > 0))
        .groupBy("sku_id")
        .agg(
            spark_sum("quantity").alias("sales_qty"),
            spark_sum("amount").alias("gmv"),
        )
        .withColumn("dt", lit(biz_date))
    )


def namespace_of(table: str) -> str:
    parts = table.split(".")
    if len(parts) < 3:
        raise ValueError("表名需要包含 catalog.namespace.table，例如 lakehouse.dws.sku_sales_daily")
    return ".".join(parts[:-1])


def ensure_table(spark: Any, table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_of(table)}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          sku_id STRING,
          sales_qty BIGINT,
          gmv DOUBLE,
          dt STRING
        )
        USING iceberg
        PARTITIONED BY (dt)
        """
    )


def run(input_path: str, table: str, biz_date: str) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("sku-sales-iceberg").getOrCreate()
    try:
        ensure_table(spark, table)
        sales = build_sales_table(spark, input_path, biz_date)
        sales.writeTo(table).overwritePartitions()
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--table", default="lakehouse.dws.sku_sales_daily")
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    run(args.input, args.table, args.date)


if __name__ == "__main__":
    main()
