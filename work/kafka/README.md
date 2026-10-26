---
date created: 2026-07-10T15:32
date updated: 2026-10-26T13:17
---

# Kafka 事件

先构造交易事件，再消费事件并输出风险告警。

- [[processing/work/kafka/producer.ipynb|事件生产]]
- [[processing/work/kafka/consumer.ipynb|告警消费]]

## 运行

在 `processing` 根目录启动，选择 Python 3 内核。

```bash
uv run --with jupyterlab --with confluent-kafka jupyter lab
```

按顺序执行单元。本地检查不需要 Kafka；连接服务前修改地址、Topic 和执行开关。消费会持续运行，使用中断内核停止。两个 Notebook 可分别打开，先启动消费者，再发送事件。
