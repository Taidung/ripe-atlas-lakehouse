"""Silver -> Gold marts bằng dbt. Chỉ đọc Silver ĐÃ CÔNG BỐ, kiểm tra nhất quán trước khi ghi.

  python -m lakehouse gold --dates 2026-11-19
  python -m lakehouse gold --start 2026-11-19T00:00:00Z --end 2026-11-19T03:00:00Z

Luồng cho mỗi ngày (giờ VN):
  1. Lấy snapshot công bố gần nhất của fact/reply Silver (ops.partition_status). Không measurement nào có bản công bố
     -> không ghi đè mart, đánh dấu blocked_upstream.
  2. dbt build tag:gold_build: cohort (chỉ probe set thực sự được ingest giờ đó, Silver đọc `FOR VERSION AS OF`),
     7 mart nháp, kiểm tra nhất quán giữa các mart + khóa/quan hệ.
  3. Lỗi critical -> không ghi, blocked. Không lỗi -> dbt run tag:gold_publish (xóa ngày rồi chèn, kể cả kết quả rỗng),
     cập nhật ops.partition_status.
  4. Đẩy metric độ tươi/độ phủ lên Pushgateway.
Mã thoát 2 nếu có ngày bị chặn. Định nghĩa metric: docs/metric-dictionary.md.
"""

from __future__ import annotations

import csv
from collections import Counter
from datetime import date

from lakehouse.common import DBT_DIR, SQL_DIR, add_day_args, iso_z, log_event, new_run_id, selected_days, utc_now, validate_day_args
from lakehouse.dbt_runner import dbt
from lakehouse.dq import CRITICAL, DqRecorder, failed_names, from_dbt, has_critical_failure, has_warning
from lakehouse.publication import (
    PUBLISHED,
    PartitionStatus,
    partition_statuses,
    published_snapshot,
    readable_measurements,
    set_partition_status,
)
from lakehouse.trino_db import Trino

SILVER_FACT = "nessie.silver.fact_ping_result"
SILVER_REPLY = "nessie.silver.fact_ping_reply"
MARTS = [
    "quality_hourly_target",
    "quality_hourly_country",
    "quality_hourly_asn",
    "quality_hourly_probe",
    "rtt_histogram_hourly",
    "ipv4_ipv6_paired_hourly",
    "coverage_hourly",
]


def ensure_probe_set_seed(db: Trino) -> None:
    """Nạp seed probe_set_member khi bảng chưa có hoặc khác file CSV (probe set mới). Probe set là bất biến nên
    so số dòng theo version là đủ; tránh nạp lại 7 nghìn dòng ở mỗi lần chạy theo giờ."""
    with (DBT_DIR / "seeds" / "probe_set_member.csv").open(encoding="utf-8") as f:
        expected = Counter(row["probe_set_version"] for row in csv.DictReader(f))
    current = (
        dict(db.query("SELECT probe_set_version, count(*) FROM work.probe_set_member GROUP BY 1"))
        if db.table_exists("work.probe_set_member")
        else {}
    )
    if current != dict(expected):
        run = dbt("seed", "probe_set_member", {})
        if not run.success:
            raise RuntimeError("dbt seed probe_set_member thất bại")


def mark_marts_blocked(db: Trino, day: date, measurement_ids: list[int], status: str, reason: str, run_id: str, dq_run_id: str):
    """Đánh dấu mọi mart của ngày `day` là bị chặn; snapshot Gold đã công bố trước đó được giữ nguyên."""
    now = utc_now()
    rows = [
        PartitionStatus("gold", f"nessie.gold.{k}", day, m, status, None, run_id, dq_run_id, None, reason, now)
        for k in MARTS
        for m in measurement_ids
    ]
    set_partition_status(db, rows)


def build_day(db: Trino, day: date, run_id: str, dq: DqRecorder) -> str:
    silver_status = partition_statuses(db, SILVER_FACT, day)
    readable = readable_measurements(db, SILVER_FACT, day)
    log_event("gold", "day_start", ictrl_dt=str(day), silver_status=silver_status, readable=readable)
    if not readable:
        # Không measurement nào có bản Silver công bố: KHÔNG ghi đè mart (bản Gold cũ, nếu có, giữ nguyên).
        reason = "no published silver snapshot"
        mark_marts_blocked(db, day, sorted(silver_status) or [-1], "blocked_upstream", reason, run_id, dq.dq_run_id)
        log_event("gold", "day_skipped", ictrl_dt=str(day), reason=reason)
        return "blocked_upstream"

    pinned = {t: published_snapshot(db, t, day) for t in (SILVER_FACT, SILVER_REPLY)}
    log_event("gold", "silver_pinned", ictrl_dt=str(day), snapshots=pinned)
    missing = [t for t, s in pinned.items() if s is None]
    if missing:
        raise RuntimeError(f"{day}: chưa có snapshot Silver công bố cho {missing}")
    variables = {
        "day": str(day),
        "run_id": run_id,
        "run_ts": iso_z(utc_now()),
        "measurement_ids": readable,
        "fact_snapshot": pinned[SILVER_FACT],
        "reply_snapshot": pinned[SILVER_REPLY],
    }

    run = dbt("build", "tag:gold_build", variables)
    if run.errors():
        raise RuntimeError(f"{day}: dbt lỗi khi tính Gold: {[n.name for n in run.errors()]}")
    checks = from_dbt(run.tests())
    scope = f"ictrl_dt={day}"
    measurement_ids = sorted(set(silver_status) | set(readable))

    if has_critical_failure(checks):
        dq.write("gold", "nessie.gold.*", scope, checks, None)
        failed = failed_names(checks, CRITICAL)
        mark_marts_blocked(db, day, measurement_ids, "blocked", "critical: " + "; ".join(failed)[:900], run_id, dq.dq_run_id)
        log_event("gold", "day_blocked", ictrl_dt=str(day), failed=failed)
        return "blocked"

    publish = dbt("run", "tag:gold_publish", variables)
    if not publish.success:
        raise RuntimeError(f"{day}: dbt lỗi khi ghi Gold: {[n.name for n in publish.errors()]}")
    snapshots = {k: db.latest_snapshot(f"gold.{k}") for k in MARTS}
    counts = {k: db.scalar(f"SELECT count(*) FROM work.{k}_candidate") for k in MARTS}
    dq.write("gold", "nessie.gold.*", scope, checks, snapshots["quality_hourly_target"])

    status = "published_with_warnings" if has_warning(checks) else "published"
    now = utc_now()
    rows = []
    for k in MARTS:
        for m in measurement_ids:
            if m not in readable:
                reason = f"silver status {silver_status.get(m)}"
                rows.append(
                    PartitionStatus("gold", f"nessie.gold.{k}", day, m, "blocked_upstream", None, run_id, dq.dq_run_id, None, reason, now)
                )
                continue
            # Silver lần gần nhất bị chặn nhưng còn bản công bố cũ: Gold dựng từ bản đó và ghi chú lại.
            stale = silver_status.get(m) not in PUBLISHED
            note = f"silver latest run {silver_status.get(m)}; built from last published snapshot" if stale else None
            rows.append(
                PartitionStatus("gold", f"nessie.gold.{k}", day, m, status, counts[k], run_id, dq.dq_run_id, snapshots[k], note, now)
            )
    set_partition_status(db, rows)
    log_event("gold", "day_written", ictrl_dt=str(day), status=status, rows=counts)
    return status


# ---------------------------------------------------------------------------- metric vận hành
def push_freshness(db: Trino, m) -> None:
    """Độ tươi và độ phủ theo cohort × measurement, đọc từ Gold đã công bố (mọi ngày)."""
    if not db.table_exists("gold.coverage_hourly"):
        return
    for r in db.dicts("""
        SELECT probe_set_version, measurement_id, to_unixtime(max(max_event_time)) AS mx
        FROM gold.coverage_hourly WHERE data_state = 'measured' GROUP BY 1, 2
    """):
        m.set(
            "ripe_gold_max_event_timestamp_seconds",
            "event_time lớn nhất đã có trong Gold (Unix giây)",
            r["mx"],
            probe_set_version=r["probe_set_version"],
            measurement_id=str(r["measurement_id"]),
        )
    for r in db.dicts("""
        WITH latest AS (
            SELECT probe_set_version, measurement_id, max(event_hour_utc) AS h
            FROM gold.coverage_hourly WHERE data_state <> 'not_processed' GROUP BY 1, 2
        )
        SELECT c.probe_set_version, c.measurement_id, c.results_total_raw, c.expected_results, c.data_state,
               to_unixtime(c.event_hour_utc) AS h
        FROM gold.coverage_hourly c
        JOIN latest l ON c.probe_set_version = l.probe_set_version AND c.measurement_id = l.measurement_id AND c.event_hour_utc = l.h
    """):
        labels = {"probe_set_version": r["probe_set_version"], "measurement_id": str(r["measurement_id"])}
        m.set("ripe_gold_latest_hour_results", "Số result của giờ gần nhất đã xử lý", r["results_total_raw"], **labels)
        if r["expected_results"]:  # expected = 0 (vd probe set không có probe IPv6): độ đầy đủ không xác định, không đẩy
            m.set(
                "ripe_gold_latest_hour_completeness_ratio",
                "Độ đầy đủ ước tính của giờ gần nhất đã xử lý",
                r["results_total_raw"] / r["expected_results"],
                **labels,
            )
        m.set("ripe_gold_latest_hour_timestamp_seconds", "Giờ gần nhất đã xử lý (Unix giây)", r["h"], **labels)
        m.set(
            "ripe_gold_latest_hour_measured", "1 nếu giờ gần nhất đã xử lý có dữ liệu", 1 if r["data_state"] == "measured" else 0, **labels
        )


# ---------------------------------------------------------------------------- CLI
add_args = add_day_args
validate = validate_day_args


def run(args, m) -> int:
    db = Trino.connect()
    db.run_file(SQL_DIR / "ops.sql")
    db.execute("CREATE SCHEMA IF NOT EXISTS gold")
    ensure_probe_set_seed(db)
    run_id = new_run_id("gold")
    days = selected_days(args)
    dq = DqRecorder(db, run_id)
    try:
        results = {str(d): build_day(db, d, run_id, dq) for d in days}
        log_event("gold", "run_finished", run_id=run_id, results=results)
        for status in ("published", "published_with_warnings", "blocked", "blocked_upstream"):
            m.set(
                "ripe_partition_days",
                "Số ngày theo trạng thái công bố, lần chạy gần nhất",
                sum(1 for r in results.values() if r == status),
                layer="gold",
                status=status,
            )
        push_freshness(db, m)
        return 0 if "blocked" not in results.values() else 2
    finally:
        for severity, n in dq.failed.items():
            m.set("ripe_dq_failed_checks", "Số kiểm tra chất lượng thất bại, lần chạy gần nhất", n, layer="gold", severity=severity)
