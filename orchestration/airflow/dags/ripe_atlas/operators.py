"""Operator chạy job của đồ án qua Docker SDK (socket /var/run/docker.sock được mount vào Airflow).

- IngestionOperator: tạo container mới từ image ripe-atlas-ingestion (code + probe set nằm sẵn trong image),
  chỉ truyền đúng biến môi trường ingestion cần; container bị xóa sau khi chạy.
- SparkJobOperator: `docker exec` spark-submit trong container spark-iceberg đang chạy.
Cả hai có on_kill: khi task bị timeout/hủy, dừng luôn tiến trình thật (không để job chạy ngầm và ghi chồng).

Mã thoát 2 của job Silver/Gold = có ngày bị chặn vì lỗi chất lượng critical -> task thất bại, không retry.
Lưu ý bảo mật: quyền dùng docker.sock tương đương root trên Docker host; chấp nhận cho môi trường đồ án local.
"""

from __future__ import annotations

import os
import shlex
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.sdk import BaseOperator

NETWORK = os.environ.get("LAKEHOUSE_NETWORK", "ripe-atlas-lakehouse_lakehouse")
SPARK_CONTAINER = os.environ.get("SPARK_CONTAINER", "ripe-atlas-lakehouse-spark-iceberg-1")
INGEST_IMAGE = os.environ.get("INGEST_IMAGE", "ripe-atlas-ingestion:0.2")
INGEST_ENV = ["S3_INGEST_ACCESS_KEY", "S3_INGEST_SECRET_KEY", "STAGING_S3_ENDPOINT", "STAGING_BUCKET", "PROBE_SET_VERSION",
              "OVERLAP_HOURS", "ATLAS_CONCURRENCY", "PUSHGATEWAY_URL"]


def overlap_hours() -> int:
    return int(os.environ.get("OVERLAP_HOURS", "3"))


def window_bounds(context: dict[str, Any]) -> tuple[str, str]:
    """[start, end) của lần chạy theo giờ: end = giờ tròn của data_interval_end; start = end - (overlap + 1) giờ."""
    # Airflow 3: lần chạy thủ công có thể không có data_interval_end -> dùng run_after.
    anchor = context.get("data_interval_end") or context["dag_run"].run_after
    end = anchor.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=overlap_hours() + 1)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")


def _docker():
    import docker

    return docker.from_env()


class IngestionOperator(BaseOperator):
    template_fields: Sequence[str] = ("arguments",)

    def __init__(self, *, arguments: list[str], **kwargs):
        super().__init__(**kwargs)
        self.arguments = arguments
        self._container = None

    def resolve_args(self, context) -> list[str]:
        return self.arguments

    def execute(self, context):
        args = self.resolve_args(context)
        env = {k: os.environ[k] for k in INGEST_ENV if os.environ.get(k)}
        self.log.info("ingestion %s (image %s, network %s)", shlex.join(args), INGEST_IMAGE, NETWORK)
        client = _docker()
        self._container = client.containers.run(INGEST_IMAGE, command=args, environment=env, network=NETWORK, detach=True,
                                                labels={"ripe.airflow.task": self.task_id})
        try:
            for line in self._container.logs(stream=True, follow=True):
                self.log.info(line.decode("utf-8", errors="replace").rstrip())
            code = self._container.wait()["StatusCode"]
        finally:
            self._container.remove(force=True)
            self._container = None
        if code != 0:
            raise AirflowException(f"ingestion exited with code {code}")

    def on_kill(self):
        if self._container is not None:
            self.log.warning("killing ingestion container %s", self._container.id[:12])
            self._container.stop(timeout=10)


class HourlyIngestionOperator(IngestionOperator):
    """incremental --now <giờ tròn của data_interval_end>: giờ vừa kết thúc + OVERLAP_HOURS giờ trước đó."""

    def resolve_args(self, context) -> list[str]:
        _, end = window_bounds(context)
        return ["incremental", "--now", end]


class SparkJobOperator(BaseOperator):
    template_fields: Sequence[str] = ("arguments",)

    def __init__(self, *, script: str, arguments: list[str] | None = None, hourly_range: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.script, self.arguments, self.hourly_range = script, arguments or [], hourly_range

    def execute(self, context):
        args = list(self.arguments)
        if self.hourly_range:
            start, end = window_bounds(context)
            args += ["--start", start, "--end", end]
        cmd = ["spark-submit", f"/opt/app/transformation/jobs/{self.script}", *args]
        self.log.info("docker exec %s %s", SPARK_CONTAINER, shlex.join(cmd))
        client = _docker()
        container = client.containers.get(SPARK_CONTAINER)
        exec_id = client.api.exec_create(container.id, cmd)["Id"]
        for chunk in client.api.exec_start(exec_id, stream=True):
            for line in chunk.decode("utf-8", errors="replace").splitlines():
                if line.strip() and " INFO " not in line:
                    self.log.info(line)
        code = client.api.exec_inspect(exec_id)["ExitCode"]
        if code == 2:
            raise AirflowFailException(f"{self.script}: partition bị chặn vì lỗi chất lượng critical (exit 2); xem ops.dq_result")
        if code != 0:
            raise AirflowException(f"{self.script} exited with code {code}")

    def on_kill(self):
        self.log.warning("killing spark-submit %s in %s", self.script, SPARK_CONTAINER)
        client = _docker()
        container = client.containers.get(SPARK_CONTAINER)
        container.exec_run(["pkill", "-f", f"/opt/app/transformation/jobs/{self.script}"])


def vn_date(run_after: datetime, offset_days: int = 0) -> str:
    """Ngày Asia/Ho_Chi_Minh của run_after + offset (macro cho template)."""
    from zoneinfo import ZoneInfo

    return (run_after.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date() + timedelta(days=offset_days)).isoformat()


def utc_date(run_after: datetime, offset_days: int = 0) -> str:
    return (run_after.astimezone(timezone.utc).date() + timedelta(days=offset_days)).isoformat()
