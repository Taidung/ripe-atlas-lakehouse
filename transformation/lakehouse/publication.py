"""Giao thức CÔNG BỐ partition giữa các tầng (Silver -> Gold -> dashboard).

Bảng ops.partition_status, một dòng mỗi (table_name, ictrl_dt, measurement_id):
- `status`: kết quả của LẦN CHẠY GẦN NHẤT: published | published_with_warnings | blocked | blocked_upstream.
- `snapshot_id`, `row_count`: của BẢN CÔNG BỐ GẦN NHẤT đã qua kiểm tra chất lượng. Lần chạy bị chặn (blocked,
  blocked_upstream) KHÔNG xoá hai cột này, nên bản công bố trước vẫn đọc được. `snapshot_id` NULL nghĩa là
  partition chưa từng được công bố.

Tầng sau đọc tầng trước ĐÚNG snapshot đã công bố (Iceberg time travel `FOR VERSION AS OF`), không đọc bản mới nhất:
nếu một lần chạy sau đó chết giữa hai commit hoặc bị chặn vì lỗi chất lượng, tầng sau vẫn thấy dữ liệu nhất quán.
`table_name` giữ dạng `nessie.<schema>.<table>` (tên mà Superset/DBeaver thấy qua catalog chỉ đọc).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import NamedTuple

from lakehouse.common import sql_str
from lakehouse.trino_db import Trino

STATUS_TABLE = "ops.partition_status"
PUBLISHED = ("published", "published_with_warnings")
BLOCKED = ("blocked", "blocked_upstream")


class PartitionStatus(NamedTuple):
    """Một dòng của ops.partition_status. Với status bị chặn, truyền row_count = snapshot_id = None."""

    layer: str
    table_name: str
    ictrl_dt: date
    measurement_id: int
    status: str
    row_count: int | None
    pipeline_run_id: str
    dq_run_id: str
    snapshot_id: int | None
    reason: str | None
    updated_at: datetime


def set_partition_status(db: Trino, rows: list[PartitionStatus]) -> None:
    """Upsert theo (table_name, ictrl_dt, measurement_id). Lần chạy bị chặn giữ snapshot_id/row_count đã công bố."""
    if not rows:
        return
    values = ",\n".join("(" + ", ".join(sql_str(v) for v in r) + ")" for r in rows)
    blocked = ", ".join(f"'{s}'" for s in BLOCKED)
    cols = ", ".join(PartitionStatus._fields)
    db.execute(f"""
        MERGE INTO {STATUS_TABLE} t
        USING (SELECT * FROM (VALUES {values}) AS v({cols})) s
        ON t.table_name = s.table_name AND t.ictrl_dt = s.ictrl_dt AND t.measurement_id = s.measurement_id
        WHEN MATCHED THEN UPDATE SET
            layer = s.layer,
            status = s.status,
            row_count = CASE WHEN s.status IN ({blocked}) THEN t.row_count ELSE s.row_count END,
            pipeline_run_id = s.pipeline_run_id,
            dq_run_id = s.dq_run_id,
            snapshot_id = CASE WHEN s.status IN ({blocked}) THEN t.snapshot_id ELSE s.snapshot_id END,
            reason = s.reason,
            updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({", ".join("s." + c for c in PartitionStatus._fields)})
    """)


def _day_rows(db: Trino, table: str, day: date) -> list[dict]:
    return db.dicts(
        f"SELECT measurement_id, status, snapshot_id FROM {STATUS_TABLE} WHERE table_name = {sql_str(table)} AND ictrl_dt = {sql_str(day)}"
    )


def partition_statuses(db: Trino, table: str, day: date) -> dict[int, str]:
    """measurement_id -> trạng thái lần chạy gần nhất của bảng `table` cho ngày `day`."""
    return {r["measurement_id"]: r["status"] for r in _day_rows(db, table, day)}


def readable_measurements(db: Trino, table: str, day: date) -> list[int]:
    """Measurement của ngày `day` đã có bản công bố đọc được (snapshot_id khác NULL), kể cả khi lần chạy sau bị chặn."""
    return sorted(r["measurement_id"] for r in _day_rows(db, table, day) if r["snapshot_id"] is not None)


def published_snapshot(db: Trino, table: str, day: date) -> int | None:
    """Snapshot công bố gần nhất của `table` cho ngày `day` (None nếu chưa từng công bố).

    Mọi measurement của một ngày được ghi trong cùng một lần chạy, nên phải trỏ về cùng một snapshot.
    """
    snapshots = {r["snapshot_id"] for r in _day_rows(db, table, day) if r["snapshot_id"] is not None}
    if len(snapshots) > 1:
        raise RuntimeError(f"{table} {day}: nhiều snapshot công bố khác nhau {snapshots}; chạy lại job tạo ra bảng này")
    return next(iter(snapshots), None)
