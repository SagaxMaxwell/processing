# PostgreSQL CDC

Debezium 将 PostgreSQL 订单和支付表写入 Kafka。

```bash
curl -X POST -H 'Content-Type: application/json' \
  --data @work/connect/postgres-cdc.json \
  http://connect01:8083/connectors
curl http://connect01:8083/connectors/orders-postgres-cdc/status
```
