# Flume 使用场景：交易系统多子模块日志采集

## 非常具体的场景

在“电商交易系统”每台应用服务器上，`order-service` 和 `payment-service` 分别写本地日志：

- `/data/app/order-service/logs/order.log`
- `/data/app/payment-service/logs/payment.log`

需求分两路：

- 原始日志实时落 HDFS：用于离线数仓审计和重跑。
- 同一份日志写 Kafka：用于 Storm/Spark Streaming 做实时风控。

Flume 在这里部署在每台应用服务器本地，直接 tail 两个子模块日志，不需要业务代码主动推送。

## 代码说明

- `order_payment_logs_to_hdfs.conf`：Taildir Source 读取两个日志文件，Memory Channel 缓冲，HDFS Sink 按日期、模块和主机写入。
- `order_payment_logs_to_kafka.conf`：同样读取两个日志文件，写入 Kafka 主题 `order_payment_log`。

启动示例：

```bash
flume-ng agent \
  --conf conf \
  --conf-file work/flume/order_payment_logs_to_hdfs.conf \
  --name a1 \
  -Dflume.root.logger=INFO,console
```
