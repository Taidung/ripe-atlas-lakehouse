"""Bảo trì Iceberg: gộp file nhỏ (ALTER TABLE ... EXECUTE optimize) cho các ngày đã đóng.

  python -m lakehouse maintenance [--before 2026-11-19] [--dry-run]

KHÔNG chạy expire_snapshots / remove_orphan_files định kỳ: với Nessie catalog, file có thể vẫn được tham chiếu bởi
nhánh hay tag khác (ví dụ nhánh dq-test), nên dọn snapshot và file không còn tham chiếu làm thủ công theo
docs/runbook.md#nessie-gc.

optimize tạo snapshot mới (commit nguyên tử). Reader đang đọc snapshot cũ (Gold đọc Silver đã công bố) không bị ảnh
hưởng; file cũ vẫn còn cho đến khi được dọn.
"""

from __future__ import annotations

from datetime import date

from lakehouse.common import log_event, report_today
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


def run(args, m) -> int:
    before = args.before or report_today()
    db = Trino.connect()
    for table in TABLES:
        if not db.table_exists(table):
            continue
        schema, name = table.split(".")
        files_sql = f"SELECT count(*) FROM {schema}.\"{name}$files\" WHERE partition.ictrl_dt < DATE '{before}'"
        files_before = db.scalar(files_sql)
        if not args.dry_run and files_before > 1:
            db.execute(f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '128MB') WHERE ictrl_dt < DATE '{before}'")
        files_after = db.scalar(files_sql)
        log_event(
            "maintenance",
            "compacted",
            table=table,
            before=str(before),
            files_before=files_before,
            files_after=files_after,
            dry_run=args.dry_run,
        )
        m.set("ripe_iceberg_closed_partition_files", "Số data file của các ngày đã đóng sau bảo trì", files_after, table=table)
        m.set("ripe_iceberg_files_rewritten", "Số file được gộp lần chạy gần nhất", max(files_before - files_after, 0), table=table)
    return 0
