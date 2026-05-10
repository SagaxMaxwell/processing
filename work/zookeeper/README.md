# ZooKeeper 使用场景：推荐系统定时任务抢主

## 非常具体的场景

在“内容推荐系统”中，`feature-refresh-job` 部署了 3 个实例，分别在 `worker-01`、`worker-02`、`worker-03`。这些实例都能刷新用户画像缓存，但同一时间只能有一个实例真正执行，否则会重复写 Redis 和 Hive 分区。

ZooKeeper 用于：

- 通过临时节点/锁实现抢主：只有拿到 `/locks/feature-refresh` 的实例执行刷新。
- 存储动态配置：`/config/recommend/feature-refresh` 保存刷新批次大小、目标 Redis 集群等。

如果主实例宕机，会话过期后锁自动释放，其他实例能继续抢主。

## 代码说明

`leader_lock_job.py` 使用 `kazoo` 客户端演示抢主、读取配置、执行任务。

依赖示例：

```bash
uv add kazoo
```

运行示例：

```bash
python work/zookeeper/leader_lock_job.py --zk centos01:2181,centos02:2181,centos03:2181
```
