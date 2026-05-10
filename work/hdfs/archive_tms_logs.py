"""把 TMS 子模块日志归档到 HDFS。

场景：
    物流 TMS 的 order-service 与 tracking-service 每天生成本地日志。数据平台
    将它们上传到 HDFS ODS 分区，供后续 Spark 和 MapReduce 读取。
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


MODULE_FILES = {
    "order-service": "order.{date}.log",
    "tracking-service": "tracking.{date}.log",
}


def run_hdfs(args: list[str]) -> str:
    """执行 hadoop fs 命令。

    Args:
        args: `hadoop fs` 后面的参数列表。

    Returns:
        命令标准输出。

    Raises:
        RuntimeError: 命令执行失败。
    """

    command = ["hadoop", "fs", *args]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"HDFS 命令失败: {' '.join(command)}\n{result.stderr}")
    return result.stdout.strip()


def upload_module_log(local_root: Path, hdfs_root: str, module: str, biz_date: str) -> None:
    """上传一个业务模块的日志。

    Args:
        local_root: 本地日志根目录。
        hdfs_root: HDFS ODS 根目录。
        module: 业务模块名。
        biz_date: 业务日期，格式为 YYYY-MM-DD。
    """

    filename = MODULE_FILES[module].format(date=biz_date)
    local_file = local_root / module / filename
    hdfs_dir = f"{hdfs_root}/dt={biz_date}/module={module}"

    run_hdfs(["-mkdir", "-p", hdfs_dir])
    run_hdfs(["-put", "-f", str(local_file), hdfs_dir])
    run_hdfs(["-setrep", "3", f"{hdfs_dir}/{filename}"])


def archive_logs(local_root: Path, hdfs_root: str, biz_date: str) -> None:
    """归档全部 TMS 模块日志。

    Args:
        local_root: 本地日志根目录。
        hdfs_root: HDFS ODS 根目录。
        biz_date: 业务日期，格式为 YYYY-MM-DD。
    """

    for module in MODULE_FILES:
        upload_module_log(local_root, hdfs_root, module, biz_date)

    summary = run_hdfs(["-count", "-h", f"{hdfs_root}/dt={biz_date}"])
    print(summary)


def main() -> None:
    """解析命令行参数并执行归档。"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--local-root", type=Path, required=True)
    parser.add_argument("--hdfs-root", default="/warehouse/ods/tms_log")
    args = parser.parse_args()
    archive_logs(args.local_root, args.hdfs_root, args.date)


if __name__ == "__main__":
    main()
