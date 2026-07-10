CREATE NAMESPACE IF NOT EXISTS lakehouse.ods;
CREATE NAMESPACE IF NOT EXISTS lakehouse.dws;
CREATE NAMESPACE IF NOT EXISTS lakehouse.ads;

CREATE TABLE IF NOT EXISTS lakehouse.dws.sku_sales_daily (
  sku_id STRING,
  sales_qty BIGINT,
  gmv DOUBLE,
  dt STRING
)
USING iceberg
PARTITIONED BY (dt);
