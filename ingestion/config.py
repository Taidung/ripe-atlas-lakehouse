"""Cấu hình ingestion: hằng số nghiệp vụ và tham số vận hành đọc từ biến môi trường.

Hằng số measurement được khai báo một lần tại đây; mọi tầng sau đối chiếu lại với
metadata thật (xem docs/api-survey.md §1).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo

REPORT_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
API_BASE_URL = "https://atlas.ripe.net/api/v2/"
DATASET_NAME = "ripe_atlas"


@dataclass(frozen=True)
class MeasurementSpec:
    measurement_id: int
    af: int
    target: str


MEASUREMENTS: dict[int, MeasurementSpec] = {
    1001: MeasurementSpec(1001, 4, "k.root-servers.net"),
    1004: MeasurementSpec(1004, 4, "f.root-servers.net"),
    2001: MeasurementSpec(2001, 6, "k.root-servers.net"),
    2004: MeasurementSpec(2004, 6, "f.root-servers.net"),
}

REPO_ROOT = Path(__file__).resolve().parents[1]


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    value = default if raw in (None, "") else int(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.environ.get(name)
    value = default if raw in (None, "") else float(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


@dataclass(frozen=True)
class HttpSettings:
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 120.0
    max_attempts: int = 5
    backoff_base_s: float = 2.0
    backoff_max_s: float = 120.0
    max_response_bytes: int = 256 * 1024 * 1024
    concurrency: int = 2

    @classmethod
    def from_env(cls) -> HttpSettings:
        return cls(
            connect_timeout_s=_env_float("ATLAS_CONNECT_TIMEOUT_S", cls.connect_timeout_s, 1),
            read_timeout_s=_env_float("ATLAS_READ_TIMEOUT_S", cls.read_timeout_s, 1),
            max_attempts=_env_int("ATLAS_MAX_ATTEMPTS", cls.max_attempts, 1),
            backoff_base_s=_env_float("ATLAS_BACKOFF_BASE_S", cls.backoff_base_s, 0),
            backoff_max_s=_env_float("ATLAS_BACKOFF_MAX_S", cls.backoff_max_s, 0),
            max_response_bytes=_env_int("ATLAS_MAX_RESPONSE_BYTES", cls.max_response_bytes, 1024),
            concurrency=_env_int("ATLAS_CONCURRENCY", cls.concurrency, 1),
        )


@dataclass(frozen=True)
class StagingSettings:
    bucket: str = "staging"
    endpoint_url: str = "http://s3:8333"
    access_key: str = field(default="", repr=False)
    secret_key: str = field(default="", repr=False)
    region: str = "us-east-1"

    @property
    def root(self) -> str:
        """Gốc dữ liệu dlt trong bucket staging, ví dụ s3://staging/ripe_atlas."""
        return f"s3://{self.bucket}/{DATASET_NAME}"

    @classmethod
    def from_env(cls) -> StagingSettings:
        settings = cls(
            bucket=os.environ.get("STAGING_BUCKET", cls.bucket),
            endpoint_url=os.environ.get("STAGING_S3_ENDPOINT", cls.endpoint_url),
            access_key=os.environ.get("S3_INGEST_ACCESS_KEY", ""),
            secret_key=os.environ.get("S3_INGEST_SECRET_KEY", ""),
            region=os.environ.get("STAGING_S3_REGION", cls.region),
        )
        if not settings.access_key or not settings.secret_key:
            raise ValueError("S3_INGEST_ACCESS_KEY and S3_INGEST_SECRET_KEY must be set")
        return settings


@dataclass(frozen=True)
class IngestSettings:
    probe_set_version: str = "dev-200-v1"
    probe_chunk_size: int = 500
    overlap_hours: int = 3

    @classmethod
    def from_env(cls) -> IngestSettings:
        return cls(
            probe_set_version=os.environ.get("PROBE_SET_VERSION", cls.probe_set_version),
            probe_chunk_size=_env_int("PROBE_CHUNK_SIZE", cls.probe_chunk_size, 1),
            overlap_hours=_env_int("OVERLAP_HOURS", cls.overlap_hours, 0),
        )


def parse_measurement_ids(raw: str | None) -> list[int]:
    if not raw:
        return sorted(MEASUREMENTS)
    ids = [int(part) for part in raw.split(",") if part.strip()]
    unknown = sorted(set(ids) - set(MEASUREMENTS))
    if unknown:
        raise ValueError(f"Unsupported measurement ids: {unknown}; allowed: {sorted(MEASUREMENTS)}")
    return sorted(set(ids))
