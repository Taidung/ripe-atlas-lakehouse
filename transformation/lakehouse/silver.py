"""Bronze -> Silver (star schema) bằng dbt; kiểm tra chất lượng TRƯỚC khi ghi, công bố theo partition.

  python -m lakehouse silver --start 2026-11-18T22:00:00Z --end 2026-11-19T02:00:00Z
  python -m lakehouse silver --dates 2026-11-19
  python -m lakehouse silver --repair-unknown     # metadata probe đến muộn
  python -m lakehouse silver --dims-only

Luồng:
  1. Dimension SCD2: dbt build tag:silver_dims_build (bảng nháp + test) -> không lỗi critical -> dbt run
     tag:silver_dims_publish (dim_probe, dim_measurement, dim_date, dim_time). Lỗi critical -> job thất bại.
  2. Với mỗi ngày (ictrl_dt, giờ VN) bị ảnh hưởng: dbt build tag:silver_facts_build (fact + reply nháp của cả ngày,
     test critical/warn, đối soát với Bronze).
  3. Lỗi critical -> không ghi, partition = blocked; snapshot đã công bố trước đó vẫn giữ trong ops.partition_status
     nên Gold tiếp tục đọc được bản cũ.
  4. Không lỗi -> dbt run tag:silver_facts_publish (xóa ngày rồi chèn) -> kiểm tra sau ghi -> cập nhật partition_status.
Mã thoát 2 khi có ngày bị chặn vì lỗi chất lượng critical. Đặc tả: docs/silver-model.md.
"""

from __future__ import annotations

from datetime import date

from lakehouse.common import SQL_DIR, add_day_args, iso_z, log_event, new_run_id, selected_days, sql_str, utc_now, validate_day_args
from lakehouse.dbt_runner import dbt
from lakehouse.dq import CRITICAL, DqRecorder, failed_names, from_dbt, has_critical_failure, has_warning, make_check
from lakehouse.publication import PartitionStatus, push_partition_days, set_partition_status
from lakehouse.trino_db import Trino, work_schema

FACT = "nessie.silver.fact_ping_result"
REPLY = "nessie.silver.fact_ping_reply"


def _counts(db: Trino, table: str, day: date) -> dict[int, int]:
    return {r[0]: r[1] for r in db.query(f"SELECT measurement_id, count(*) FROM {table} WHERE ictrl_dt = {sql_str(day)} GROUP BY 1")}


# ---------------------------------------------------------------------------- 1. dimensions
def build_dimensions(db: Trino, dq: DqRecorder, variables: dict) -> None:
    run = dbt("build", "tag:silver_dims_build", variables)
    if run.errors():
        raise RuntimeError(f"dbt lỗi khi tính dimension: {[n.name for n in run.errors()]}")
    checks = from_dbt(run.tests())
    for name in ("dim_probe", "dim_measurement"):
        mine = [c for c in checks if c["model"] == f"{name}_candidate"]
        dq.write("silver", f"nessie.silver.{name}", "all", mine, None)
    if has_critical_failure(checks):
        raise RuntimeError(f"Dimension quality check failed (critical): {failed_names(checks, CRITICAL)}; facts not rebuilt")
    publish = dbt("run", "tag:silver_dims_publish", variables)
    if not publish.success:
        raise RuntimeError(f"dbt lỗi khi ghi dimension: {[n.name for n in publish.errors()]}")
    log_event(
        "silver",
        "dims_written",
        probe_versions=db.scalar("SELECT count(*) - 1 FROM silver.dim_probe"),
        measurement_versions=db.scalar("SELECT count(*) - 1 FROM silver.dim_measurement"),
    )


# ---------------------------------------------------------------------------- 2-4. một ngày
def build_day(db: Trino, ws: str, day: date, run_id: str, dq: DqRecorder) -> str:
    variables = {"day": str(day), "run_id": run_id, "run_ts": iso_z(utc_now()), "work_schema": ws}
    if not db.scalar(f"SELECT count(*) FROM bronze.ping_result_raw WHERE ictrl_dt = {sql_str(day)}"):
        log_event("silver", "day_empty", ictrl_dt=str(day))
        return "empty"

    run = dbt("build", "tag:silver_facts_build", variables)
    if run.errors():
        raise RuntimeError(f"{day}: dbt lỗi khi tính fact: {[n.name for n in run.errors()]}")
    checks = from_dbt(run.tests())
    reply_checks = [c for c in checks if c["model"] == "fact_ping_reply_candidate"]
    fact_checks = [c for c in checks if c not in reply_checks]  # gồm cả test đối soát nhiều bảng
    fact_counts = _counts(db, f"{ws}.fact_ping_result_candidate", day)
    scope, now = f"ictrl_dt={day}", utc_now()

    if has_critical_failure(checks):
        dq.write("silver", FACT, scope, fact_checks, None)
        dq.write("silver", REPLY, scope, reply_checks, None)
        failed = failed_names(checks, CRITICAL)
        reason = "critical: " + "; ".join(failed)[:900]
        # row_count/snapshot_id = None: MERGE giữ nguyên bản đã công bố trước đó (nếu có).
        rows = [
            PartitionStatus("silver", t, day, m, "blocked", None, run_id, dq.dq_run_id, None, reason, now)
            for t in (FACT, REPLY)
            for m in fact_counts
        ]
        set_partition_status(db, rows)
        log_event("silver", "day_blocked", ictrl_dt=str(day), failed=failed)
        return "blocked"

    # Ghi (mỗi bảng: xóa ngày rồi chèn), rồi kiểm tra sau ghi
    publish = dbt("run", "tag:silver_facts_publish", variables)
    if not publish.success:
        raise RuntimeError(f"{day}: dbt lỗi khi ghi fact: {[n.name for n in publish.errors()]}")
    fact_snapshot, reply_snapshot = db.latest_snapshot("silver.fact_ping_result"), db.latest_snapshot("silver.fact_ping_reply")
    written = _counts(db, "silver.fact_ping_result", day)
    post_write = make_check(
        "post_write:row_count_matches", CRITICAL, written == fact_counts, observed={"written": written, "expected": fact_counts}
    )
    dq.write("silver", FACT, scope, [*fact_checks, post_write], fact_snapshot)
    dq.write("silver", REPLY, scope, reply_checks, reply_snapshot)

    # Công bố. Nếu kiểm tra sau ghi sai, bản vừa ghi KHÔNG được công bố: Gold vẫn đọc snapshot công bố trước đó.
    reply_counts = _counts(db, f"{ws}.fact_ping_reply_candidate", day)
    if not post_write["success"]:
        status, reason = "blocked", f"post-write count mismatch at snapshot {fact_snapshot}; not published"
        fact_snapshot = reply_snapshot = None
        fact_counts = reply_counts = dict.fromkeys(fact_counts)
    elif has_warning(checks):
        status, reason = "published_with_warnings", "; ".join(failed_names(checks))[:900]
    else:
        status, reason = "published", None
    rows = [
        PartitionStatus("silver", table, day, m, status, n, run_id, dq.dq_run_id, snapshot, reason, now)
        for table, counts, snapshot in ((FACT, fact_counts, fact_snapshot), (REPLY, reply_counts, reply_snapshot))
        for m, n in counts.items()
    ]
    set_partition_status(db, rows)
    log_event(
        "silver",
        "day_written",
        ictrl_dt=str(day),
        status=status,
        rows=fact_counts,
        replies=reply_counts,
        fact_snapshot=fact_snapshot,
        reply_snapshot=reply_snapshot,
    )
    return status


def days_needing_repair(db: Trino) -> list[date]:
    """Ngày có fact probe_sk = -1 mà probe đó NAY đã có metadata (metadata đến muộn)."""
    if not db.table_exists("silver.fact_ping_result"):
        return []
    rows = db.query("""
        SELECT DISTINCT f.ictrl_dt
        FROM silver.fact_ping_result f
        JOIN (SELECT DISTINCT probe_id FROM silver.dim_probe WHERE NOT is_unknown_member) p ON f.probe_id = p.probe_id
        WHERE f.probe_sk = -1
    """)
    return sorted(r[0] for r in rows)


# ---------------------------------------------------------------------------- CLI
def add_args(parser) -> None:
    group = add_day_args(parser)
    group.add_argument("--repair-unknown", action="store_true")
    group.add_argument("--dims-only", action="store_true")


validate = validate_day_args


def push_run_metrics(db: Trino, m, results: dict[str, str], days: list[date]) -> None:
    push_partition_days(m, "silver", results, ("published", "published_with_warnings", "blocked", "empty"))
    if days:
        in_days = ", ".join(sql_str(d) for d in days)
        for mid, n in db.query(
            f"SELECT measurement_id, sum(row_count) FROM ops.partition_status "
            f"WHERE table_name = {sql_str(FACT)} AND ictrl_dt IN ({in_days}) GROUP BY 1"
        ):
            m.set("ripe_silver_rows", "Số dòng fact_ping_result của các ngày vừa tính lại", n or 0, measurement_id=str(mid))


def run(args, m) -> int:
    db = Trino.connect()
    db.run_file(SQL_DIR / "ops.sql")
    run_id = new_run_id("silver")
    dq = DqRecorder(db, run_id)
    try:
        with work_schema(db, run_id) as ws:
            build_dimensions(db, dq, {"run_id": run_id, "run_ts": iso_z(utc_now()), "work_schema": ws})
            if args.dims_only:
                return 0
            days = days_needing_repair(db) if args.repair_unknown else selected_days(args)
            log_event("silver", "run_start", run_id=run_id, days=[str(d) for d in days])
            results = {str(d): build_day(db, ws, d, run_id, dq) for d in days}
        log_event("silver", "run_finished", run_id=run_id, results=results)
        push_run_metrics(db, m, results, days)
        return 0 if "blocked" not in results.values() else 2
    finally:
        dq.push_metrics(m, "silver")
