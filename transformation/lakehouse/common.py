"""Tiện ích dùng chung cho runner: hằng số nghiệp vụ, đường dẫn, log JSON, run id, thời gian, tham số dòng lệnh."""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------- hằng số
REPORT_TZ = "Asia/Ho_Chi_Minh"  # múi giờ báo cáo: ictrl_dt, giờ địa phương trên dashboard
REPORT_ZONE = ZoneInfo(REPORT_TZ)
# measurement_id -> (address family, target). Cùng giá trị với ingestion/settings.py và macro measurements() của dbt.
MEASUREMENTS = {
    1001: (4, "k.root-servers.net"),
    1004: (4, "f.root-servers.net"),
    2001: (6, "k.root-servers.net"),
    2004: (6, "f.root-servers.net"),
}

STAGING_ROOT = "s3://staging/ripe_atlas"
APP_DIR = Path(os.environ.get("APP_DIR", Path(__file__).resolve().parents[2]))
TRANSFORM_DIR = APP_DIR / "transformation"
SQL_DIR = TRANSFORM_DIR / "sql"
DBT_DIR = TRANSFORM_DIR / "dbt"
CONFIG_DIR = APP_DIR / "config"


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


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------- tham số dòng lệnh
# Loại lần chạy, gắn vào nhãn metric. Alert "job lỗi liên tiếp" chỉ xét run_type=scheduled, nên backfill hay
# chạy tay không che mất (hoặc gây ra) cảnh báo của lịch chạy chính.
RUN_TYPES = ("scheduled", "backfill", "reconcile", "repair", "manual")


def add_day_args(parser):
    """--start/--end (UTC) | --dates (giờ VN), loại trừ nhau; trả về group để lệnh thêm lựa chọn riêng."""
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", type=parse_instant)
    group.add_argument("--dates", help="Danh sách ngày Asia/Ho_Chi_Minh, phân tách bằng dấu phẩy")
    parser.add_argument("--end", type=parse_instant)
    return group


def validate_day_args(parser, args) -> None:
    if args.start and not args.end:
        parser.error("--end is required with --start")


def selected_days(args) -> list[date]:
    return parse_dates(args.dates) if args.dates else days_for_range(args.start, args.end)


def add_run_type_arg(parser) -> None:
    parser.add_argument("--run-type", choices=RUN_TYPES, default="manual", help="Loại lần chạy cho nhãn metric (mặc định: manual)")


# ---------------------------------------------------------------------------- log, run id
def new_run_id(prefix: str) -> str:
    return f"{prefix}-{utc_now():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def log_event(stage: str, event: str, **fields: Any) -> None:
    """Một sự kiện = một dòng JSON trên stderr (cùng định dạng với ingestion/logs.py)."""
    record = {"ts": utc_now().isoformat(timespec="milliseconds"), "stage": stage, "event": event, **fields}
    sys.stderr.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------- SQL
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


def sql_str(value: Any) -> str:
    """Literal SQL an toàn cho giá trị do runner tự sinh (đường dẫn, run id, ngày)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, datetime):
        return f"from_iso8601_timestamp('{iso_z(value)}')"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"
