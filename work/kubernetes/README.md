---
date created: 2026-07-10T15:32
date updated: 2026-10-26T13:05
---

# Kubernetes 上的 Spark

`sparkapp.yaml` 用 Spark Operator 提交 SKU 日销量任务。

先按 [[processing/work/spark/README#导出并提交|Spark 导出步骤]] 生成 `sales.py`，构建镜像时将它放到 `/opt/jobs/work/spark/sales.py`。将清单中的示例镜像替换为实际镜像，并配置命名空间、服务账号、存储权限与依赖版本，再提交任务。

```bash
kubectl apply -f work/kubernetes/sparkapp.yaml
kubectl get sparkapplication sku-sales-daily -n data
```
