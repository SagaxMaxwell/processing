# Kafka Connect CDC

PostgreSQL 订单、支付表通过 Debezium 写入 Kafka。

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @work/connect/postgres-cdc.json \
  http://connect01:8083/connectors

curl http://connect01:8083/connectors/orders-postgres-cdc/status
```
