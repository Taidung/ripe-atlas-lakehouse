"""Giao thức CÔNG BỐ partition giữa các tầng (Silver -> Gold -> dashboard).

- Mỗi job ghi xong một ngày thì cập nhật nessie.ops.partition_status: published | published_with_warnings | blocked,
  kèm snapshot_id của commit đã qua kiểm tra chất lượng.
- Tầng sau đọc tầng trước ĐÚNG snapshot đã công bố (Iceberg time travel), không đọc bản mới nhất: nếu một lần chạy
  sau đó chết giữa hai commit, tầng sau vẫn thấy dữ liệu nhất quán (sự cố 2026-11-19, docs/test-report.md §2).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

STATUS_TABLE = "nessie.ops.partition_status"
PUBLISHED = ("published", "published_with_warnings")


def latest_snapshot(spark: SparkSession, table: str) -> int | None:
    row = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return row[0] if row else None


def set_partition_status(spark: SparkSession, rows: list[tuple]) -> None:
    """rows: (layer, table_name, ictrl_dt, measurement_id, status, row_count, run_id, dq_run_id, snapshot_id, reason, updated_at)."""
    if not rows:
        return
    spark.createDataFrame(rows, spark.table(STATUS_TABLE).schema).createOrReplaceTempView("src_status")
    spark.sql(f"""
        MERGE INTO {STATUS_TABLE} t USING src_status s
        ON t.table_name = s.table_name AND t.ictrl_dt = s.ictrl_dt AND t.measurement_id = s.measurement_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    """)


def partition_statuses(spark: SparkSession, table: str, day: date) -> dict[int, str]:
    """measurement_id -> trạng thái công bố của bảng `table` cho ngày `day`."""
    rows = (spark.table(STATUS_TABLE).filter((F.col("table_name") == table) & (F.col("ictrl_dt") == F.lit(day)))
            .select("measurement_id", "status").collect())
    return {r["measurement_id"]: r["status"] for r in rows}


def published_snapshot(spark: SparkSession, table: str, day: date) -> int | None:
    """Snapshot đã công bố của `table` cho ngày `day`; lỗi nếu các measurement trỏ về các snapshot khác nhau."""
    snapshots = {r["snapshot_id"] for r in spark.table(STATUS_TABLE)
                 .filter((F.col("table_name") == table) & (F.col("ictrl_dt") == F.lit(day)) & F.col("status").isin(*PUBLISHED))
                 .select("snapshot_id").collect()}
    if len(snapshots) > 1:
        raise RuntimeError(f"{table} {day}: nhiều snapshot công bố khác nhau {snapshots}; chạy lại job tạo ra bảng này")
    return next(iter(snapshots), None)


def read_published(spark: SparkSession, table: str, snapshot_id: int | None) -> DataFrame:
    """Đọc bảng tại snapshot đã công bố (nếu có), ngược lại đọc bản mới nhất."""
    reader = spark.read.format("iceberg")
    if snapshot_id is not None:
        reader = reader.option("snapshot-id", str(snapshot_id))
    return reader.load(table)
