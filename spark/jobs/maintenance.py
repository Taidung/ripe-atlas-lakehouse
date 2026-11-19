"""Bảo trì Iceberg: gộp file nhỏ (rewrite_data_files) cho các ngày đã đóng.

  spark-submit /opt/app/spark/jobs/maintenance.py [--before 2026-11-19] [--dry-run]

KHÔNG chạy expire_snapshots / remove_orphan_files của Iceberg: với Nessie catalog, file có thể vẫn được tham chiếu
bởi nhánh hay tag khác (ví dụ nhánh dq-test), nên hai procedure này không an toàn. Việc dọn snapshot và file
không còn được tham chiếu thuộc về công cụ Nessie GC, xem docs/runbook.md#nessie-gc.

rewrite_data_files tạo snapshot mới (commit nguyên tử). Reader đang đọc snapshot cũ không bị ảnh hưởng; file cũ
vẫn còn cho đến khi Nessie GC dọn.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

from lakehouse.common import log_event
from lakehouse.metrics import job_metrics

TABLES = [
    "bronze.ping_result_raw", "bronze.probe_metadata_raw", "bronze.measurement_metadata_raw",
    "silver.fact_ping_result", "silver.fact_ping_reply",
    "gold.quality_hourly_target", "gold.quality_hourly_country", "gold.quality_hourly_asn", "gold.quality_hourly_probe",
    "gold.rtt_histogram_hourly", "gold.ipv4_ipv6_paired_hourly", "gold.coverage_hourly",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", type=date.fromisoformat, help="Chỉ gộp partition ictrl_dt < ngày này (mặc định: hôm nay giờ VN)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    before = args.before or datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()

    spark = SparkSession.builder.appName("maintenance").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    with job_metrics("maintenance") as m:
        try:
            for table in TABLES:
                full = f"nessie.{table}"
                if not spark.catalog.tableExists(full):
                    continue
                where = f"ictrl_dt < DATE '{before}'"
                files_before = spark.sql(f"SELECT count(*) FROM {full}.files WHERE partition.ictrl_dt < DATE '{before}'").first()[0]
                rewritten = added = 0
                if not args.dry_run and files_before > 0:
                    r = spark.sql(f"""CALL nessie.system.rewrite_data_files(table => '{table}', where => "{where}",
                                      options => map('min-input-files', '2', 'target-file-size-bytes', '134217728'))""").first()
                    rewritten, added = r["rewritten_data_files_count"], r["added_data_files_count"]
                files_after = spark.sql(f"SELECT count(*) FROM {full}.files WHERE partition.ictrl_dt < DATE '{before}'").first()[0]
                log_event("maintenance", "compacted", table=table, before=str(before), files_before=files_before,
                          files_after=files_after, rewritten=rewritten, added=added, dry_run=args.dry_run)
                m.set("ripe_iceberg_closed_partition_files", "Số data file của các ngày đã đóng sau bảo trì", files_after, table=table)
                m.set("ripe_iceberg_files_rewritten", "Số file được gộp lần chạy gần nhất", rewritten, table=table)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
