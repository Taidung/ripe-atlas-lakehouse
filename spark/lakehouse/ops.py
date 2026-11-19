"""Ghi kết quả chất lượng (ops.dq_result) và trạng thái công bố (ops.partition_status), dùng chung Silver/Gold."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession

from lakehouse.common import log_event, new_run_id


def latest_snapshot(spark: SparkSession, table: str) -> int | None:
    row = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return row[0] if row else None


class DqRecorder:
    def __init__(self, spark: SparkSession, pipeline_run_id: str):
        self.spark, self.pipeline_run_id = spark, pipeline_run_id
        self.dq_run_id = new_run_id("dq")

    def write(self, layer: str, table: str, scope: str, checks: list[dict], snapshot_id: int | None) -> None:
        now = datetime.now(timezone.utc)
        rows = [(self.dq_run_id, self.pipeline_run_id, layer, table, scope, c["check_name"], c["engine"], c["severity"],
                 c["success"], c["observed_value"], c["unexpected_count"], c["element_count"], c["details"], snapshot_id, now)
                for c in checks]
        schema = self.spark.table("nessie.ops.dq_result").schema
        self.spark.createDataFrame(rows, schema).writeTo("nessie.ops.dq_result").append()
        failed = [c["check_name"] for c in checks if not c["success"]]
        log_event("dq", "recorded", table=table, scope=scope, checks=len(checks), failed=failed[:10])


def set_partition_status(spark: SparkSession, rows: list[tuple]) -> None:
    """rows: (layer, table_name, ictrl_dt, measurement_id, status, row_count, run_id, dq_run_id, snapshot_id, reason, updated_at)."""
    if not rows:
        return
    schema = spark.table("nessie.ops.partition_status").schema
    spark.createDataFrame(rows, schema).createOrReplaceTempView("src_status")
    spark.sql("""
        MERGE INTO nessie.ops.partition_status t USING src_status s
        ON t.table_name = s.table_name AND t.ictrl_dt = s.ictrl_dt AND t.measurement_id = s.measurement_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    """)
