#!/usr/bin/env bash
# 场景：电商数仓每天统计 SKU 销量。
# Args:
#   $1: hadoop-streaming.jar 路径。
#   $2: HDFS 输入路径。
#   $3: HDFS 输出路径。

set -euo pipefail

STREAMING_JAR="$1"
INPUT_PATH="$2"
OUTPUT_PATH="$3"

hadoop fs -rm -r -f "${OUTPUT_PATH}"
hadoop jar "${STREAMING_JAR}" \
  -files work/mapreduce/sku_sales_mapper.py,work/mapreduce/sku_sales_reducer.py \
  -mapper "python sku_sales_mapper.py" \
  -reducer "python sku_sales_reducer.py" \
  -input "${INPUT_PATH}" \
  -output "${OUTPUT_PATH}"
