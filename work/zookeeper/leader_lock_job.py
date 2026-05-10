"""ZooKeeper 抢主任务示例。

场景：
    推荐系统的 feature-refresh-job 部署多个实例。所有实例都启动，但只有抢到
    ZooKeeper 锁的实例刷新用户画像缓存。
"""

from __future__ import annotations

import argparse
import json
import socket
from typing import Any


def load_config(zk, path: str) -> dict[str, Any]:
    """从 ZooKeeper 读取 JSON 配置。

    Args:
        zk: KazooClient 实例。
        path: 配置节点路径。

    Returns:
        配置字典；节点不存在时返回默认配置。
    """

    if not zk.exists(path):
        return {"batch_size": 1000, "redis_cluster": "redis-feature-prod"}
    data, _ = zk.get(path)
    return json.loads(data.decode("utf-8"))


def refresh_feature_cache(config: dict[str, Any]) -> None:
    """刷新用户画像缓存。

    Args:
        config: 从 ZooKeeper 读取的任务配置。
    """

    print(
        "刷新用户画像缓存 "
        f"batch_size={config['batch_size']} redis_cluster={config['redis_cluster']}"
    )


def run_as_leader(zk_hosts: str, lock_path: str, config_path: str) -> None:
    """抢到 ZooKeeper 锁后执行任务。

    Args:
        zk_hosts: ZooKeeper 集群地址。
        lock_path: 分布式锁路径。
        config_path: 动态配置路径。
    """

    from kazoo.client import KazooClient

    zk = KazooClient(hosts=zk_hosts)
    zk.start()
    try:
        identifier = socket.gethostname()
        with zk.Lock(lock_path, identifier):
            config = load_config(zk, config_path)
            refresh_feature_cache(config)
    finally:
        zk.stop()
        zk.close()


def main() -> None:
    """解析命令行参数并执行抢主任务。"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--zk", required=True)
    parser.add_argument("--lock-path", default="/locks/feature-refresh")
    parser.add_argument("--config-path", default="/config/recommend/feature-refresh")
    args = parser.parse_args()
    run_as_leader(args.zk, args.lock_path, args.config_path)


if __name__ == "__main__":
    main()
