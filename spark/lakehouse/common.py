"""Tiện ích chung: đường dẫn staging, chạy file SQL, log JSON, run id."""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STAGING_ROOT = "s3://staging/ripe_atlas"
SQL_DIR = Path("/opt/app/sql")


def days_for_range(start, end) -> list:
    """Các ngày Asia/Ho_Chi_Minh chạm vào khoảng [start, end) (datetime có múi giờ)."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("Asia/Ho_Chi_Minh")
    first = start.astimezone(tz).date()
    last = (end - timedelta(microseconds=1)).astimezone(tz).date()
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def to_s3a(path: str) -> str:
    return "s3a://" + path[len("s3://"):] if path.startswith("s3://") else path


def to_s3(path: str) -> str:
    return "s3://" + path[len("s3a://"):] if path.startswith("s3a://") else path


def new_run_id(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def log_event(stage: str, event: str, **fields: Any) -> None:
    record = {"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"), "stage": stage, "event": event, **fields}
    sys.stderr.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
    sys.stderr.flush()


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


def run_sql_file(spark, path: Path) -> None:
    for statement in split_sql(path.read_text(encoding="utf-8")):
        spark.sql(statement)


def glob_paths(spark, pattern: str) -> list[str]:
    """Liệt kê object khớp glob qua Hadoop FileSystem (s3a); trả về [] nếu không có."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(to_s3a(pattern))
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    statuses = fs.globStatus(path)
    return sorted(str(s.getPath()) for s in statuses) if statuses else []


def path_exists(spark, path: str) -> bool:
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(to_s3a(path))
    return bool(p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p))
