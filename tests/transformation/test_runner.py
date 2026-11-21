"""Test offline cho runner biến đổi (transformation/lakehouse): không cần Trino, dbt hay S3.

Logic dữ liệu (Bronze/Silver/Gold) nằm trong dbt và được kiểm chứng trên dữ liệu thật bằng dbt test, job crosscheck
và tests/integration/dq_block_on_branch.sh. Ở đây kiểm tra phần thủ tục của runner.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone

import pytest

from lakehouse import bronze, dq
from lakehouse.__main__ import parse
from lakehouse.common import SQL_DIR, days_for_range, split_sql, sql_str
from lakehouse.dbt_runner import NodeResult

UTC = timezone.utc
MANIFEST = {
    "manifest_path": "s3://staging/ripe_atlas/_manifests/ping_result_raw/window_date=2026-11-21/window_hour=07/msm_id=1001/r1.json",
    "entity": "ping_result_raw",
    "run_id": "20261121T013454Z-4626d997",
    "run_type": "scheduled",
    "load_id": "1795224921.9008577",
    "measurement_id": 1001,
    "af": 4,
    "window_start": "2026-11-21T00:00:00+00:00",
    "window_end": "2026-11-21T01:00:00+00:00",
    "ictrl_dt": "2026-11-21",
    "probe_set_version": "dev-200-v1",
    "probes_requested": 200,
    "requests": [{}, {}],
    "counts": {"fetched": 3000, "new": 2919, "changed": 0, "unchanged": 81, "staged": 2919},
    "objects": [
        {
            "path": "s3://staging/ripe_atlas/ping_result_raw/window_date=2026-11-21/window_hour=07/msm_id=1001/1795224921.9008577.f5.jsonl.gz",
            "bytes": 120,
        }
    ],
    "started_at": "2026-11-21T01:34:54+00:00",
    "finished_at": "2026-11-21T01:35:10+00:00",
    "_raw": "{}",
}


@pytest.mark.parametrize(
    "argv",
    [
        ["bronze", "--entity", "ping_result_raw", "--start", "2026-11-21T00:00:00Z", "--end", "2026-11-21T01:00:00Z"],
        ["bronze", "--entity", "metadata"],
        ["silver", "--dates", "2026-11-20,2026-11-21"],
        ["silver", "--repair-unknown", "--run-type", "repair"],
        ["gold", "--start", "2026-11-21T00:00:00Z", "--end", "2026-11-21T04:00:00Z", "--run-type", "scheduled"],
        ["crosscheck", "--date", "2026-11-20"],
        ["maintenance", "--dry-run"],
    ],
)
def test_every_command_imports_and_parses(argv):
    module, args = parse(argv)
    assert module.__name__ == f"lakehouse.{argv[0]}"
    assert args.run_type in ("manual", "repair", "scheduled")


@pytest.mark.parametrize(
    "argv",
    [
        ["bronze", "--entity", "ping_result_raw"],  # results cần --start/--end
        ["silver", "--start", "2026-11-21T00:00:00Z"],  # --start cần --end
        ["gold", "--dates", "2026-11-21", "--run-type", "nightly"],
    ],
)
def test_invalid_arguments_are_rejected(argv):
    with pytest.raises(SystemExit):
        parse(argv)


def test_days_for_range_uses_vietnam_calendar():
    # 16:00Z-18:00Z ngày 20/11 = 23:00 ngày 20 -> 01:00 ngày 21 giờ VN
    days = days_for_range(datetime(2026, 11, 20, 16, tzinfo=UTC), datetime(2026, 11, 20, 18, tzinfo=UTC))
    assert days == [date(2026, 11, 20), date(2026, 11, 21)]


def test_result_manifest_globs_follow_local_hour_folders():
    globs = bronze.manifest_globs("ping_result_raw", datetime(2026, 11, 20, 16, tzinfo=UTC), datetime(2026, 11, 20, 18, tzinfo=UTC), 3)
    assert [g.split("/_manifests/")[1] for g in globs] == [
        "ping_result_raw/window_date=2026-11-20/window_hour=23/msm_id=*/*.json",
        "ping_result_raw/window_date=2026-11-21/window_hour=00/msm_id=*/*.json",
    ]


def test_metadata_manifest_globs_cover_lookback_days():
    globs = bronze.manifest_globs("probe_metadata_raw", None, None, 3)
    assert len(globs) == 3 and all("/probe_metadata_raw/ictrl_dt=" in g for g in globs)


def test_pending_rows_carry_partition_and_manifest_context():
    (r,) = bronze.pending_rows("ping_result_raw", [MANIFEST])
    assert (r["window_date"], r["window_hour"], r["msm_id"], r["part_ictrl_dt"]) == ("2026-11-21", "07", "1001", None)
    assert r["window_start_ts"] == int(datetime(2026, 11, 21, tzinfo=UTC).timestamp())
    assert r["manifest_staged_rows"] == 2919


def test_audit_row_matches_ingest_manifest_columns():
    stats = {MANIFEST["manifest_path"]: {"read": 10, "valid": 9, "inserted": 4, "already_present": 5, "quarantined": 1}}
    (r,) = bronze.audit_rows([MANIFEST], stats, "bronze-x", datetime(2026, 11, 21, tzinfo=UTC))
    assert r["rows_inserted"] == 4 and r["rows_already_present"] == 5 and r["request_count"] == 2
    assert r["bronze_status"] == "loaded" and r["object_bytes"] == 120


def _ddl_columns(table: str) -> list[str]:
    """Tên cột của `table` trong transformation/sql/ops.sql (đúng thứ tự khai báo)."""
    ddl = (SQL_DIR / "ops.sql").read_text(encoding="utf-8")
    body = re.search(rf"CREATE TABLE IF NOT EXISTS {re.escape(table)} \((.*?)\n\)", ddl, re.S).group(1)
    return re.findall(r"(?:^|,)\s*(\w+) (?:varchar|bigint|integer|date|boolean|timestamp)", body)


def test_runner_rows_match_ddl_columns():
    stats = {MANIFEST["manifest_path"]: dict.fromkeys(("read", "valid", "inserted", "already_present", "quarantined"), 0)}
    assert list(bronze.pending_rows("ping_result_raw", [MANIFEST])[0]) == _ddl_columns("work.bronze_pending_object")
    assert list(bronze.audit_rows([MANIFEST], stats, "b", datetime(2026, 11, 21, tzinfo=UTC))[0]) == _ddl_columns("bronze.ingest_manifest")


def test_sql_literals_escape_and_type():
    assert sql_str("it's") == "'it''s'"
    assert sql_str(None) == "NULL" and sql_str(True) == "true" and sql_str(5) == "5"
    assert sql_str(date(2026, 11, 21)) == "DATE '2026-11-21'"
    assert sql_str(datetime(2026, 11, 21, tzinfo=UTC)) == "from_iso8601_timestamp('2026-11-21T00:00:00Z')"


def test_split_sql_drops_comments_and_keeps_statements():
    assert split_sql("-- c\nCREATE SCHEMA a;\n\nCREATE TABLE b (\n x int\n);\n") == ["CREATE SCHEMA a", "CREATE TABLE b (\n x int\n)"]


def _test(status: str, severity: str) -> NodeResult:
    return NodeResult(f"t_{status}_{severity}", "test", status, severity, 1, None, "fact_ping_result_candidate")


def test_dbt_test_results_map_to_critical_and_warn():
    pytest.importorskip("dbt.version")
    checks = dq.from_dbt([_test("pass", "error"), _test("warn", "warn"), _test("skipped", "error")])
    assert [c["severity"] for c in checks] == ["critical", "warn"]  # test bị skip không ghi
    assert not dq.has_critical_failure(checks) and dq.has_warning(checks)
    assert checks[1]["model"] == json.loads(checks[1]["details"])["model"] == "fact_ping_result_candidate"
    assert dq.has_critical_failure(dq.from_dbt([_test("fail", "error")]))
