"""提交并监控 Hadoop/YARN 离线任务。

场景：
    在线教育 SaaS 平台每天凌晨把 course-service 与 exam-service 的日志交给
    Hadoop/YARN 计算学生学习日报。本脚本负责提交作业、清理旧输出、等待任务完成。
"""

from __future__ import annotations

import argparse
import re
import subprocess
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class StreamingJob:
    """Hadoop Streaming 作业参数。

    Attributes:
        streaming_jar: hadoop-streaming.jar 的本地路径。
        mapper: mapper 脚本路径。
        reducer: reducer 脚本路径。
        input_path: HDFS 输入路径。
        output_path: HDFS 输出路径。
    """

    streaming_jar: str
    mapper: str
    reducer: str
    input_path: str
    output_path: str


def run_command(command: list[str]) -> str:
    """执行命令并返回标准输出。

    Args:
        command: 需要执行的命令及参数。

    Returns:
        命令标准输出和标准错误合并后的文本。

    Raises:
        RuntimeError: 命令退出码不为 0。
    """

    result = subprocess.run(command, text=True, capture_output=True, check=False)
    output = result.stdout + result.stderr
    if result.returncode != 0:
        raise RuntimeError(f"命令执行失败: {' '.join(command)}\n{output}")
    return output


def remove_output_if_exists(output_path: str) -> None:
    """删除已存在的 HDFS 输出目录。

    Args:
        output_path: HDFS 输出目录。
    """

    run_command(["hadoop", "fs", "-rm", "-r", "-f", output_path])


def submit_streaming_job(job: StreamingJob) -> str | None:
    """提交 Hadoop Streaming 作业。

    Args:
        job: Streaming 作业参数。

    Returns:
        YARN 应用 ID。如果命令输出中没有应用 ID，则返回 None。
    """

    output = run_command(
        [
            "hadoop",
            "jar",
            job.streaming_jar,
            "-files",
            f"{job.mapper},{job.reducer}",
            "-mapper",
            f"python {job.mapper.rsplit('/', maxsplit=1)[-1]}",
            "-reducer",
            f"python {job.reducer.rsplit('/', maxsplit=1)[-1]}",
            "-input",
            job.input_path,
            "-output",
            job.output_path,
        ]
    )
    match = re.search(r"application_\d+_\d+", output)
    return match.group(0) if match else None


def wait_until_finished(application_id: str, interval_seconds: int = 10) -> None:
    """轮询 YARN 应用状态直到结束。

    Args:
        application_id: YARN 应用 ID。
        interval_seconds: 查询间隔秒数。

    Raises:
        RuntimeError: 应用最终状态不是 SUCCEEDED。
    """

    while True:
        status = run_command(["yarn", "application", "-status", application_id])
        state = re.search(r"Final-State\s*:\s*(\w+)", status)
        if state and state.group(1) != "UNDEFINED":
            if state.group(1) != "SUCCEEDED":
                raise RuntimeError(f"{application_id} 执行失败，最终状态: {state.group(1)}")
            return
        time.sleep(interval_seconds)


def main() -> None:
    """解析命令行参数并提交作业。"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--streaming-jar", required=True)
    parser.add_argument("--mapper", required=True)
    parser.add_argument("--reducer", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    job = StreamingJob(args.streaming_jar, args.mapper, args.reducer, args.input, args.output)
    remove_output_if_exists(job.output_path)
    application_id = submit_streaming_job(job)
    if application_id:
        wait_until_finished(application_id)


if __name__ == "__main__":
    main()
