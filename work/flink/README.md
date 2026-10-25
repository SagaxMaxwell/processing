# Flink 风控

`risk.sql` 从 `trade_events` 按 5 分钟窗口统计支付失败，写入 `risk_alerts`。

```bash
sql-client.sh -f work/flink/risk.sql
```
