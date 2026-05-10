# MapReduce 使用场景：电商 SKU 日销量统计

## 非常具体的场景

在“电商交易系统”里，`order-service` 每条已支付订单会落一行 JSON 到 HDFS：

```json
{"event_type":"order_paid","order_id":"O1001","sku_id":"SKU-8","quantity":2,"pay_amount":199.0}
```

离线数仓每天用 MapReduce 统计每个 SKU 的销售件数，输出到：

`/warehouse/dws/sku_sales_daily/dt=2026-05-09`

MapReduce 适合这个场景的原因：输入是大批量历史日志，统计逻辑是典型的 `map -> shuffle -> reduce` 聚合，计算依赖少，吞吐稳定。

## 代码说明

- `sku_sales_mapper.py`：读取 JSON 行，只保留 `order_paid`，输出 `sku_id\tquantity`。
- `sku_sales_reducer.py`：按 `sku_id` 累加销量。
- `run_streaming.sh`：Hadoop Streaming 提交命令模板。

本地快速验证：

```bash
printf '%s\n' \
  '{"event_type":"order_paid","sku_id":"SKU-8","quantity":2}' \
  '{"event_type":"order_paid","sku_id":"SKU-8","quantity":3}' \
  | python work/mapreduce/sku_sales_mapper.py \
  | sort \
  | python work/mapreduce/sku_sales_reducer.py
```
