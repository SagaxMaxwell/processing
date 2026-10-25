# Spark 作业

## 批处理：写入 Iceberg

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:${ICEBERG_VERSION} \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=rest \
  --conf spark.sql.catalog.lakehouse.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.lakehouse.warehouse=s3://lakehouse/warehouse \
  work/spark/sales.py --input s3a://raw/orders/dt=2026-07-10/*.json \
  --table lakehouse.dws.sku_sales_daily --date 2026-07-10
```

## 实时：计算门店 GMV

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION} \
  work/spark/gmv.py --bootstrap kafka01:9092,kafka02:9092 \
  --topic store_orders --checkpoint s3a://lakehouse/checkpoints/store_gmv
```
