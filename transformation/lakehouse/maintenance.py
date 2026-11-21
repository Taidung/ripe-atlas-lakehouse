"""Bảo trì: gộp file nhỏ (ALTER TABLE ... EXECUTE optimize) cho các ngày đã đóng; dọn schema tạm còn sót.

  python -m lakehouse maintenance [--before 2026-11-19] [--dry-run]

KHÔNG chạy expire_snapshots / remove_orphan_files định kỳ: với Nessie catalog, file có thể vẫn được tham chiếu bởi
nhánh hay tag khác (ví dụ nhánh dqtest), nên dọn snapshot và file không còn tham chiếu làm thủ công theo
docs/runbook.md#nessie-gc.

optimize tạo snapshot mới (commit nguyên tử). Reader đang đọc snapshot cũ (Gold đọc Silver đã công bố) không bị ảnh
hưởng; file cũ vẫn còn cho đến khi được dọn.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone

from lakehouse.common import log_event, report_today, utc_now
from lakehouse.trino_db import Trino

TABLES = [
    "bronze.ping_result_raw",
    "bronze.probe_metadata_raw",
    "bronze.measurement_metadata_raw",
    "silver.fact_ping_result",
    "silver.fact_ping_reply",
    "gold.quality_hourly_target",
    "gold.quality_hourly_country",
    "gold.quality_hourly_asn",
    "gold.quality_hourly_probe",
    "gold.rtt_histogram_hourly",
    "gold.ipv4_ipv6_paired_hourly",
    "gold.coverage_hourly",
]


def add_args(parser) -> None:
    parser.add_argument("--before", type=date.fromisoformat, help="Chỉ gộp partition ictrl_dt < ngày này (mặc định: hôm nay giờ VN)")
    parser.add_argument("--dry-run", action="store_true")


def stale_work_schemas(schemas: list[str], now: datetime, max_age: timedelta = timedelta(days=1)) -> list[str]:
    """Schema tạm `work_<stage>_<yyyymmddThhmmssZ>_<id>` cũ hơn max_age: lượt chạy đã chết mà không kịp tự xóa."""
    stale = []
    for name in schemas:
        m = re.fullmatch(r"work_\w+?_(\d{8}t\d{6}z)_[0-9a-f]+", name)
        if m and now - datetime.strptime(m.group(1), "%Y%m%dt%H%M%Sz").replace(tzinfo=timezone.utc) > max_age:
            stale.append(name)
    return stale


def run(args, m) -> int:
    before = args.before or report_today()
    db = Trino.connect()
    for table in TABLES:
        if not db.table_exists(table):
            continue
        schema, name = table.split(".")
        files_sql = f"SELECT file_path FROM {schema}.\"{name}$files\" WHERE partition.ictrl_dt < DATE '{before}'"
        files_before = {r[0] for r in db.query(files_sql)}
        if not args.dry_run and len(files_before) > 1:
            db.execute(f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '128MB') WHERE ictrl_dt < DATE '{before}'")
        files_after = {r[0] for r in db.query(files_sql)}
        rewritten = len(files_before - files_after)  # file cũ bị thay bằng file gộp
        log_event(
            "maintenance",
            "compacted",
            table=table,
            before=str(before),
            files_before=len(files_before),
            files_after=len(files_after),
            rewritten=rewritten,
            added=len(files_after - files_before),
            dry_run=args.dry_run,
        )
        m.set("ripe_iceberg_closed_partition_files", "Số data file của các ngày đã đóng sau bảo trì", len(files_after), table=table)
        m.set("ripe_iceberg_files_rewritten", "Số file được gộp lần chạy gần nhất", rewritten, table=table)

    schemas = [r[0] for r in db.query("SHOW SCHEMAS LIKE 'work\\_%' ESCAPE '\\'")]
    for name in stale_work_schemas(schemas, utc_now()):
        if not args.dry_run:
            db.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
        log_event("maintenance", "stale_work_schema_dropped", schema=name, dry_run=args.dry_run)
    return 0
