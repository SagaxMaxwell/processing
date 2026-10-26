---
date created: 2026-07-10T15:32
date updated: 2026-10-26T13:04
---

# Flink 风控

`risk.sql` 从 `trade_events` 按 5 分钟窗口统计支付失败，写入 `risk_alerts`。

- [[processing/work/flink/risk.ipynb|规则预览与本地样例]]

Notebook 只用 Python 标准库验证窗口分组和告警阈值，不连接 Kafka 或 Flink。生产提交仍使用 `risk.sql`，因为 Watermark、Kafka 连接和容错需要在 Flink 环境中验证。

```bash
sql-client.sh -f work/flink/risk.sql
```
