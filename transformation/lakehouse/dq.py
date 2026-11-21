"""Kết quả kiểm tra chất lượng: dbt test -> ops.dq_result.

severity của dbt test: error = critical (chặn công bố), warn = cảnh báo (vẫn công bố, ghi published_with_warnings).
"""

from __future__ import annotations

import json
from typing import Any

from lakehouse.common import log_event, new_run_id, utc_now
from lakehouse.dbt_runner import NodeResult
from lakehouse.trino_db import Trino, insert_rows

CRITICAL = "critical"
WARN = "warn"
DQ_TABLE = "ops.dq_result"
CheckResult = dict[str, Any]


def make_check(name: str, severity: str, success: bool, observed: Any = None) -> CheckResult:
    """Kiểm tra do runner tự làm (vd đếm dòng sau khi ghi)."""
    return {
        "check_name": name,
        "engine": "runner",
        "severity": severity,
        "success": bool(success),
        "observed_value": None if observed is None else json.dumps(observed, default=str),
        "unexpected_count": None,
        "details": None,
        "model": None,
    }


def from_dbt(tests: list[NodeResult]) -> list[CheckResult]:
    """Test dbt đã chạy (pass/fail/warn) -> CheckResult. Test bị skip không được ghi."""
    from dbt.version import __version__

    return [
        {
            "check_name": t.name,
            "engine": f"dbt-{__version__}",
            "severity": WARN if t.severity == "warn" else CRITICAL,
            "success": t.status == "pass",
            "observed_value": None,
            "unexpected_count": t.failures,
            "details": json.dumps({"model": t.attached_to, "message": t.message}, default=str)[:4000],
            "model": t.attached_to,  # None với test đối soát nhiều bảng (dbt/tests/*.sql)
        }
        for t in tests
        if t.status in ("pass", "fail", "warn")
    ]


def has_critical_failure(checks: list[CheckResult]) -> bool:
    return any(c["severity"] == CRITICAL and not c["success"] for c in checks)


def has_warning(checks: list[CheckResult]) -> bool:
    return any(c["severity"] == WARN and not c["success"] for c in checks)


def failed_names(checks: list[CheckResult], severity: str | None = None) -> list[str]:
    return [c["check_name"] for c in checks if not c["success"] and (severity is None or c["severity"] == severity)]


class DqRecorder:
    """Một recorder cho một lần chạy job; đếm số kiểm tra thất bại theo mức độ để đẩy metric."""

    def __init__(self, db: Trino, pipeline_run_id: str):
        self.db = db
        self.pipeline_run_id = pipeline_run_id
        self.dq_run_id = new_run_id("dq")
        self.failed = {CRITICAL: 0, WARN: 0}

    def write(self, layer: str, table: str, scope: str, checks: list[CheckResult], snapshot_id: int | None) -> None:
        if not checks:
            return
        now = utc_now()
        stored = ("check_name", "engine", "severity", "success", "observed_value", "unexpected_count", "details")
        rows = [
            {
                "dq_run_id": self.dq_run_id,
                "pipeline_run_id": self.pipeline_run_id,
                "layer": layer,
                "table_name": table,
                "partition_scope": scope,
                **{k: c[k] for k in stored},
                "target_snapshot_id": snapshot_id,
                "checked_at": now,
            }
            for c in checks
        ]
        insert_rows(self.db, DQ_TABLE, rows)
        for c in checks:
            if not c["success"]:
                self.failed[c["severity"]] += 1
        log_event("dq", "recorded", table=table, scope=scope, checks=len(checks), failed=failed_names(checks)[:10])

    def push_metrics(self, m, layer: str) -> None:
        for severity, n in self.failed.items():
            m.set("ripe_dq_failed_checks", "Số kiểm tra chất lượng thất bại, lần chạy gần nhất", n, layer=layer, severity=severity)
