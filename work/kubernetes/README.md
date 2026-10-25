# Kubernetes 上的 Spark

`sparkapp.yaml` 用 Spark Operator 提交 SKU 日销量任务。

```bash
kubectl apply -f work/kubernetes/sparkapp.yaml
kubectl get sparkapplication sku-sales-daily -n data
```
