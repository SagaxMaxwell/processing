# HDFS 使用场景：物流 TMS 日志归档

## 非常具体的场景

在“物流运输管理系统 TMS”里，`order-service` 负责订单创建和改派，`tracking-service` 负责车辆 GPS 轨迹上报。两个子模块每天在本地写日志：

- `/data/tms/order-service/order.2026-05-09.log`
- `/data/tms/tracking-service/tracking.2026-05-09.log`

数据平台每天 00:10 把这两个模块的日志归档到 HDFS ODS 层：

- `/warehouse/ods/tms_log/dt=2026-05-09/module=order-service/`
- `/warehouse/ods/tms_log/dt=2026-05-09/module=tracking-service/`

HDFS 在这里的价值是保存大文件、提供副本容错，并让后续 MapReduce/Spark 作业就近读取数据。

## 代码说明

`archive_tms_logs.py` 通过 `hadoop fs` 完成目录创建、上传、设置副本数和统计。

运行示例：

```bash
python work/hdfs/archive_tms_logs.py \
  --date 2026-05-09 \
  --local-root /data/tms \
  --hdfs-root /warehouse/ods/tms_log
```
