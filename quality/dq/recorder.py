"""Ghi kết quả kiểm tra chất lượng vào bảng nessie.ops.dq_result (DDL: transformation/sql/ops.sql)."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession

from dq.results import CheckResult
from lakehouse.common import log_event, new_run_id

DQ_TABLE = "nessie.ops.dq_result"


class DqRecorder:
    """Một recorder cho một lần chạy job; đếm số kiểm tra thất bại theo mức độ để đẩy metric."""

    def __init__(self, spark: SparkSession, pipeline_run_id: str):
        self.spark = spark
        self.pipeline_run_id = pipeline_run_id
        self.dq_run_id = new_run_id("dq")
        self.failed = {"critical": 0, "warn": 0}

    def write(self, layer: str, table: str, scope: str, checks: list[CheckResult], snapshot_id: int | None) -> None:
        now = datetime.now(timezone.utc)
        rows = [
            (
                self.dq_run_id,
                self.pipeline_run_id,
                layer,
                table,
                scope,
                c["check_name"],
                c["engine"],
                c["severity"],
                c["success"],
                c["observed_value"],
                c["unexpected_count"],
                c["element_count"],
                c["details"],
                snapshot_id,
                now,
            )
            for c in checks
        ]
        self.spark.createDataFrame(rows, self.spark.table(DQ_TABLE).schema).writeTo(DQ_TABLE).append()
        for c in checks:
            if not c["success"]:
                self.failed[c["severity"]] = self.failed.get(c["severity"], 0) + 1
        failed = [c["check_name"] for c in checks if not c["success"]]
        log_event("dq", "recorded", table=table, scope=scope, checks=len(checks), failed=failed[:10])
