"""Tiện ích dùng chung cho mọi job Spark: hằng số nghiệp vụ, đường dẫn, DDL, log JSON, run id, tham số dòng lệnh."""

from __future__ import annotations

import json
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------- hằng số
REPORT_TZ = "Asia/Ho_Chi_Minh"  # múi giờ báo cáo: ictrl_dt, giờ địa phương trên dashboard
REPORT_ZONE = ZoneInfo(REPORT_TZ)
# measurement_id -> (address family, target). Cùng giá trị với ingestion/settings.py::MEASUREMENTS.
MEASUREMENTS = {
    1001: (4, "k.root-servers.net"),
    1004: (4, "f.root-servers.net"),
    2001: (6, "k.root-servers.net"),
    2004: (6, "f.root-servers.net"),
}

STAGING_ROOT = "s3://staging/ripe_atlas"
# DDL của các tầng: transformation/sql/{bronze,silver,ops}.sql (mount vào /opt/app/transformation).
SQL_DIR = Path("/opt/app/transformation/sql")


# ---------------------------------------------------------------------------- thời gian
def parse_instant(value: str) -> datetime:
    """ISO-8601 có offset bắt buộc (vd 2026-11-19T00:00:00Z) -> datetime UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include an offset")
    return parsed.astimezone(timezone.utc)


def parse_dates(value: str) -> list[date]:
    """'2026-11-18,2026-11-19' -> [date, date] (ngày theo giờ báo cáo)."""
    return [date.fromisoformat(d.strip()) for d in value.split(",") if d.strip()]


def days_for_range(start: datetime, end: datetime) -> list[date]:
    """Các ngày (giờ báo cáo) chạm vào khoảng [start, end)."""
    first = start.astimezone(REPORT_ZONE).date()
    last = (end - timedelta(microseconds=1)).astimezone(REPORT_ZONE).date()
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def report_today() -> date:
    return datetime.now(REPORT_ZONE).date()


# ---------------------------------------------------------------------------- log, run id
def new_run_id(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def log_event(stage: str, event: str, **fields: Any) -> None:
    """Một sự kiện = một dòng JSON trên stderr (cùng định dạng với ingestion/logs.py)."""
    record = {"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"), "stage": stage, "event": event, **fields}
    sys.stderr.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------- DDL
def split_sql(text: str) -> list[str]:
    """Tách file SQL theo ';' cuối câu, bỏ comment dòng `--`."""
    statements, current = [], []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).rstrip().rstrip(";"))
            current = []
    if current:
        statements.append("\n".join(current))
    return statements


def run_sql_file(spark: SparkSession, path: Path) -> None:
    for statement in split_sql(path.read_text(encoding="utf-8")):
        spark.sql(statement)


# ---------------------------------------------------------------------------- object storage
def to_s3a(path: str) -> str:
    return "s3a://" + path.removeprefix("s3://") if path.startswith("s3://") else path


def to_s3(path: str) -> str:
    return "s3://" + path.removeprefix("s3a://") if path.startswith("s3a://") else path


def _hadoop_path(spark: SparkSession, path: str):
    p = spark._jvm.org.apache.hadoop.fs.Path(to_s3a(path))
    return p, p.getFileSystem(spark._jsc.hadoopConfiguration())


def glob_paths(spark: SparkSession, pattern: str) -> list[str]:
    """Liệt kê object khớp glob qua Hadoop FileSystem (s3a); trả về [] nếu không có."""
    path, fs = _hadoop_path(spark, pattern)
    statuses = fs.globStatus(path)
    return sorted(str(s.getPath()) for s in statuses) if statuses else []


def path_exists(spark: SparkSession, path: str) -> bool:
    p, fs = _hadoop_path(spark, path)
    return bool(fs.exists(p))


# ---------------------------------------------------------------------------- bảng
def table_columns(spark: SparkSession, table: str) -> list[str]:
    return [f.name for f in spark.table(table).schema.fields]


def count_by(df: DataFrame, key: str) -> dict[Any, int]:
    """{giá trị key: số dòng}."""
    return {r[key]: r["n"] for r in df.groupBy(key).agg(F.count("*").alias("n")).collect()}


def count_by_measurement(df: DataFrame) -> dict[int, int]:
    return count_by(df, "measurement_id")
