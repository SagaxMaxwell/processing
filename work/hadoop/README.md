# Hadoop 使用场景：教育 SaaS 夜间数仓调度

## 非常具体的场景

在“在线教育 SaaS 平台”中，`course-service` 每天产生课程访问日志，`exam-service` 每天产生答题日志。凌晨 01:10，调度系统需要把 HDFS 上的原始日志输入到一个 Hadoop/YARN 作业中，生成 `dws_student_daily` 学生学习汇总表。

这里 Hadoop 主要承担两件事：

- HDFS 保存输入、输出数据：`/warehouse/ods/course_log/dt=2026-05-09`、`/warehouse/ods/exam_log/dt=2026-05-09`。
- YARN 调度计算任务：提交 Hadoop Streaming 或 Spark on YARN 作业，并等待任务完成。

## 代码说明

`submit_yarn_pipeline.py` 是一个轻量提交脚本，封装了：

- 删除已有输出目录，避免 Hadoop 作业因为输出目录存在而失败。
- 提交 Hadoop Streaming 任务。
- 从命令输出中提取 `application_...`，并轮询 `yarn application -status`。

运行示例：

```bash
python work/hadoop/submit_yarn_pipeline.py \
  --streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar \
  --mapper work/mapreduce/sku_sales_mapper.py \
  --reducer work/mapreduce/sku_sales_reducer.py \
  --input /warehouse/ods/order_detail/dt=2026-05-09 \
  --output /warehouse/dws/sku_sales_daily/dt=2026-05-09
```
