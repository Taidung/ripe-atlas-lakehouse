"""Operator chạy job của đồ án qua Docker SDK (socket /var/run/docker.sock được mount vào Airflow).

Mỗi task tạo một container mới từ image của job (code nằm sẵn trong image), chỉ truyền đúng biến môi trường job cần,
và xóa container sau khi chạy:
- IngestionOperator: image ripe-atlas-ingestion (dlt -> staging), khóa atlas-ingest.
- TransformOperator: image ripe-atlas-transform (python -m lakehouse: Bronze/Silver/Gold bằng dbt trên Trino),
  khóa atlas-processing để đọc manifest; dữ liệu do Trino xử lý.
on_kill: khi task bị timeout/hủy, dừng luôn container (không để job chạy ngầm và ghi chồng).

Mã thoát 2 của transform silver/gold = có ngày bị chặn vì lỗi chất lượng critical -> task thất bại, không retry.
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
INGEST_IMAGE = os.environ.get("INGEST_IMAGE", "ripe-atlas-ingestion:0.2")
TRANSFORM_IMAGE = os.environ.get("TRANSFORM_IMAGE", "ripe-atlas-transform:0.1")
INGEST_ENV = [
    "S3_INGEST_ACCESS_KEY",
    "S3_INGEST_SECRET_KEY",
    "STAGING_S3_ENDPOINT",
    "STAGING_BUCKET",
    "PROBE_SET_VERSION",
    "OVERLAP_HOURS",
    "ATLAS_CONCURRENCY",
    "PUSHGATEWAY_URL",
]
TRANSFORM_ENV = ["S3_PROCESSING_ACCESS_KEY", "S3_PROCESSING_SECRET_KEY", "PUSHGATEWAY_URL"]
TRANSFORM_FIXED_ENV = {"TRINO_HOST": "trino", "TRINO_PORT": "8080", "S3_ENDPOINT": "http://s3:8333"}


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


class DockerJobOperator(BaseOperator):
    """Chạy `image` với `command`, stream log vào Airflow, xóa container khi xong."""

    template_fields: Sequence[str] = ("arguments",)
    image: str = ""
    env_keys: list[str] = []
    fixed_env: dict[str, str] = {}

    def __init__(self, *, arguments: list[str], **kwargs):
        super().__init__(**kwargs)
        self.arguments = arguments
        self._container = None

    def resolve_args(self, context) -> list[str]:
        return self.arguments

    def on_exit_code(self, code: int) -> None:
        if code != 0:
            raise AirflowException(f"{self.image} exited with code {code}")

    def execute(self, context):
        args = self.resolve_args(context)
        env = {**self.fixed_env, **{k: os.environ[k] for k in self.env_keys if os.environ.get(k)}}
        self.log.info("%s %s (network %s)", self.image, shlex.join(args), NETWORK)
        client = _docker()
        self._container = client.containers.run(
            self.image, command=args, environment=env, network=NETWORK, detach=True, labels={"ripe.airflow.task": self.task_id}
        )
        try:
            for line in self._container.logs(stream=True, follow=True):
                self.log.info(line.decode("utf-8", errors="replace").rstrip())
            code = self._container.wait()["StatusCode"]
        finally:
            self._container.remove(force=True)
            self._container = None
        self.on_exit_code(code)

    def on_kill(self):
        if self._container is not None:
            self.log.warning("killing container %s", self._container.id[:12])
            self._container.stop(timeout=10)


class IngestionOperator(DockerJobOperator):
    image = INGEST_IMAGE
    env_keys = INGEST_ENV


class HourlyIngestionOperator(IngestionOperator):
    """incremental --now <giờ tròn của data_interval_end>: giờ vừa kết thúc + OVERLAP_HOURS giờ trước đó."""

    def resolve_args(self, context) -> list[str]:
        _, end = window_bounds(context)
        return ["incremental", "--now", end, "--run-type", "scheduled"]


class TransformOperator(DockerJobOperator):
    """python -m lakehouse <command> ... --run-type <run_type> [--start --end của cửa sổ giờ]."""

    image = TRANSFORM_IMAGE
    env_keys = TRANSFORM_ENV
    fixed_env = TRANSFORM_FIXED_ENV

    def __init__(self, *, command: str, run_type: str, arguments: list[str] | None = None, hourly_range: bool = False, **kwargs):
        """run_type (bắt buộc): nhãn metric của job (scheduled | backfill | reconcile | repair | manual), xem lakehouse/common.py.

        Không có mặc định: chỉ lần chạy chính của một stage được gắn `scheduled`. Hai task dùng chung (stage, run_type) thì
        dùng chung group Pushgateway, và lần thành công của task này sẽ che lần thất bại của task kia trong cảnh báo.
        """
        super().__init__(arguments=arguments or [], **kwargs)
        self.command, self.run_type, self.hourly_range = command, run_type, hourly_range

    def resolve_args(self, context) -> list[str]:
        args = [self.command, *self.arguments, "--run-type", self.run_type]
        if self.hourly_range:
            start, end = window_bounds(context)
            args += ["--start", start, "--end", end]
        return args

    def on_exit_code(self, code: int) -> None:
        if code == 2:
            raise AirflowFailException(
                f"lakehouse {self.command}: partition bị chặn vì lỗi chất lượng critical (exit 2); xem ops.dq_result"
            )
        super().on_exit_code(code)


def vn_date(run_after: datetime, offset_days: int = 0) -> str:
    """Ngày Asia/Ho_Chi_Minh của run_after + offset (macro cho template)."""
    from zoneinfo import ZoneInfo

    return (run_after.astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date() + timedelta(days=offset_days)).isoformat()


def utc_date(run_after: datetime, offset_days: int = 0) -> str:
    return (run_after.astimezone(timezone.utc).date() + timedelta(days=offset_days)).isoformat()
