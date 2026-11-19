"""Job Bronze -> Silver (star schema), kiểm tra chất lượng TRƯỚC khi ghi, công bố theo partition.

  spark-submit /opt/app/transformation/jobs/silver_build.py --start 2026-11-18T22:00:00Z --end 2026-11-19T02:00:00Z
  spark-submit /opt/app/transformation/jobs/silver_build.py --dates 2026-11-19
  spark-submit /opt/app/transformation/jobs/silver_build.py --repair-unknown     # metadata probe đến muộn
  spark-submit /opt/app/transformation/jobs/silver_build.py --dims-only

Luồng:
  1. Dimension SCD2 (dim_probe, dim_measurement) build lại toàn bộ từ snapshot Bronze; kiểm tra; ghi.
  2. Với mỗi ngày (ictrl_dt, giờ VN) bị ảnh hưởng: tính lại TOÀN BỘ fact của ngày đó từ Bronze (cả 4 measurement).
  3. Kiểm tra GX + đối soát. Lỗi critical -> không ghi, partition = blocked; snapshot đã công bố trước đó
     vẫn giữ trong ops.partition_status nên Gold tiếp tục đọc được bản cũ.
  4. Ghi overwrite theo partition (ictrl_dt, measurement_id) -> kiểm tra sau ghi -> cập nhật ops.partition_status.
Mã thoát 2 khi có ngày bị chặn vì lỗi chất lượng critical. Đặc tả: docs/silver-model.md.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dq import gx_suites
from dq.checks import scd2_checks, silver_reconciliation_checks
from dq.gx_runner import run_suite
from dq.recorder import DqRecorder
from dq.results import CRITICAL, failed_names, has_critical_failure, has_warning, make_check
from lakehouse.common import (
    SQL_DIR,
    add_run_type_arg,
    count_by_measurement,
    days_for_range,
    log_event,
    new_run_id,
    parse_dates,
    parse_instant,
    run_sql_file,
    table_columns,
)
from lakehouse.metrics import job_metrics
from lakehouse.publication import PartitionStatus, latest_snapshot, set_partition_status
from lakehouse.silver import (
    MEASUREMENT_TRACKED,
    PROBE_TRACKED,
    asof_join,
    build_fact_reply,
    build_fact_result,
    build_scd2,
    dim_date,
    dim_time,
    measurement_snapshots,
    probe_snapshots,
    select_result_versions,
    with_unknown_member,
)

BRONZE_RESULTS = "nessie.bronze.ping_result_raw"
FACT = "nessie.silver.fact_ping_result"
REPLY = "nessie.silver.fact_ping_reply"


# ---------------------------------------------------------------------------- 1. dimensions
def build_dimensions(spark: SparkSession, dq: DqRecorder) -> tuple[DataFrame, DataFrame]:
    probe = build_scd2(probe_snapshots(spark.table("nessie.bronze.probe_metadata_raw")), "probe_id", PROBE_TRACKED, "probe", "probe_sk")
    probe = with_unknown_member(spark, probe, "probe_id", "probe_sk").cache()
    measurement = build_scd2(
        measurement_snapshots(spark.table("nessie.bronze.measurement_metadata_raw")),
        "measurement_id",
        MEASUREMENT_TRACKED,
        "measurement",
        "measurement_sk",
    )
    measurement = with_unknown_member(spark, measurement, "measurement_id", "measurement_sk").cache()

    blocked = []
    for name, df, sk, key in (
        ("dim_probe", probe, "probe_sk", "probe_id"),
        ("dim_measurement", measurement, "measurement_sk", "measurement_id"),
    ):
        table = f"nessie.silver.{name}"
        checks = run_suite(df, f"silver_{name}", gx_suites.silver_dimension(sk, key), dq.pipeline_run_id) + scd2_checks(df, key, name)
        if has_critical_failure(checks):
            dq.write("silver", table, "all", checks, None)
            blocked.append(name)
            continue
        df.writeTo(table).using("iceberg").tableProperty("format-version", "2").createOrReplace()
        dq.write("silver", table, "all", checks, latest_snapshot(spark, table))
    if blocked:
        raise RuntimeError(f"Dimension quality check failed (critical): {blocked}; facts not rebuilt")

    for name, df in (("dim_date", dim_date(spark)), ("dim_time", dim_time(spark))):
        if not spark.catalog.tableExists(f"nessie.silver.{name}"):
            df.writeTo(f"nessie.silver.{name}").using("iceberg").createOrReplace()
    log_event("silver", "dims_written", probe_versions=probe.count() - 1, measurement_versions=measurement.count() - 1)
    return probe, measurement


# ---------------------------------------------------------------------------- 2-4. một ngày
def build_day(spark: SparkSession, day: date, probe_dim: DataFrame, msm_dim: DataFrame, run_id: str, dq: DqRecorder) -> str:
    cached: list[DataFrame] = []
    try:
        return _build_day(spark, day, probe_dim, msm_dim, run_id, dq, cached)
    finally:  # mọi nhánh (empty, blocked, published) đều giải phóng cache, tránh dồn bộ nhớ driver khi backfill nhiều ngày
        for df in cached:
            df.unpersist()


def _build_day(
    spark: SparkSession, day: date, probe_dim: DataFrame, msm_dim: DataFrame, run_id: str, dq: DqRecorder, cached: list[DataFrame]
) -> str:
    def cache(df: DataFrame) -> DataFrame:
        cached.append(df.cache())
        return cached[-1]

    loaded_at = datetime.now(timezone.utc)
    bronze = cache(spark.table(BRONZE_RESULTS).filter(F.col("ictrl_dt") == F.lit(day)))
    if bronze.limit(1).count() == 0:
        log_event("silver", "day_empty", ictrl_dt=str(day))
        return "empty"

    # Tính fact + reply cho cả ngày
    facts = cache(asof_join(build_fact_result(select_result_versions(bronze), run_id, loaded_at), probe_dim, msm_dim))
    replies = cache(build_fact_reply(facts))
    fact_out = cache(facts.select(*table_columns(spark, FACT)))
    reply_out = replies.select(*table_columns(spark, REPLY))

    # Kiểm tra trước khi ghi
    checks_fact = run_suite(fact_out, "silver_fact_ping_result", gx_suites.silver_fact_result(), run_id)
    checks_reply = run_suite(reply_out, "silver_fact_ping_reply", gx_suites.silver_fact_reply(), run_id)
    checks_reconcile = silver_reconciliation_checks(bronze, fact_out, reply_out)
    all_checks = checks_fact + checks_reply + checks_reconcile
    fact_counts = count_by_measurement(fact_out)
    scope, now = f"ictrl_dt={day}", datetime.now(timezone.utc)

    if has_critical_failure(all_checks):
        dq.write("silver", FACT, scope, checks_fact + checks_reconcile, None)
        dq.write("silver", REPLY, scope, checks_reply, None)
        failed = failed_names(all_checks, CRITICAL)
        reason = "critical: " + "; ".join(failed)[:900]
        # row_count/snapshot_id = None: MERGE giữ nguyên bản đã công bố trước đó (nếu có).
        rows = [
            PartitionStatus("silver", t, day, m, "blocked", None, run_id, dq.dq_run_id, None, reason, now)
            for t in (FACT, REPLY)
            for m in fact_counts
        ]
        set_partition_status(spark, rows)
        log_event("silver", "day_blocked", ictrl_dt=str(day), failed=failed)
        return "blocked"

    # Ghi (mỗi bảng là một commit Iceberg nguyên tử), rồi kiểm tra sau ghi
    fact_out.writeTo(FACT).overwritePartitions()
    reply_out.writeTo(REPLY).overwritePartitions()
    fact_snapshot, reply_snapshot = latest_snapshot(spark, FACT), latest_snapshot(spark, REPLY)
    written = count_by_measurement(spark.table(FACT).filter(F.col("ictrl_dt") == F.lit(day)))
    post_write = [
        make_check(
            "post_write:row_count_matches",
            "spark",
            CRITICAL,
            written == fact_counts,
            observed={"written": written, "expected": fact_counts},
        )
    ]
    dq.write("silver", FACT, scope, checks_fact + checks_reconcile + post_write, fact_snapshot)
    dq.write("silver", REPLY, scope, checks_reply, reply_snapshot)

    # Công bố. Nếu kiểm tra sau ghi sai, bản vừa ghi KHÔNG được công bố: Gold vẫn đọc snapshot công bố trước đó.
    reply_counts = count_by_measurement(reply_out)
    if not post_write[0]["success"]:
        status, reason = "blocked", f"post-write count mismatch at snapshot {fact_snapshot}; not published"
        fact_snapshot = reply_snapshot = None
        fact_counts = reply_counts = dict.fromkeys(fact_counts)
    elif has_warning(all_checks):
        status, reason = "published_with_warnings", "; ".join(failed_names(all_checks))[:900]
    else:
        status, reason = "published", None
    rows = [
        PartitionStatus("silver", table, day, m, status, n, run_id, dq.dq_run_id, snapshot, reason, now)
        for table, counts, snapshot in ((FACT, fact_counts, fact_snapshot), (REPLY, reply_counts, reply_snapshot))
        for m, n in counts.items()
    ]
    set_partition_status(spark, rows)
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


def days_needing_repair(spark: SparkSession, probe_dim: DataFrame) -> list[date]:
    """Ngày có fact probe_sk = -1 mà probe đó NAY đã có metadata (metadata đến muộn)."""
    if not spark.catalog.tableExists(FACT):
        return []
    known = probe_dim.filter(~F.col("is_unknown_member")).select("probe_id").distinct()
    rows = (
        spark.table(FACT)
        .filter(F.col("probe_sk") == -1)
        .select("ictrl_dt", "probe_id")
        .distinct()
        .join(known, "probe_id")
        .select("ictrl_dt")
        .distinct()
        .collect()
    )
    return sorted(r["ictrl_dt"] for r in rows)


# ---------------------------------------------------------------------------- entry point
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", type=parse_instant)
    group.add_argument("--dates", help="Danh sách ngày Asia/Ho_Chi_Minh, phân tách bằng dấu phẩy")
    group.add_argument("--repair-unknown", action="store_true")
    group.add_argument("--dims-only", action="store_true")
    parser.add_argument("--end", type=parse_instant)
    add_run_type_arg(parser)
    args = parser.parse_args()
    if args.start and not args.end:
        parser.error("--end is required with --start")
    return args


def push_run_metrics(spark: SparkSession, m, results: dict[str, str], days: list[date]) -> None:
    for status in ("published", "published_with_warnings", "blocked", "empty"):
        m.set(
            "ripe_partition_days",
            "Số ngày theo trạng thái công bố, lần chạy gần nhất",
            sum(1 for r in results.values() if r == status),
            layer="silver",
            status=status,
        )
    if days:
        for r in (
            spark.table("nessie.ops.partition_status")
            .filter(F.col("table_name") == FACT)
            .filter(F.col("ictrl_dt").isin(days))
            .groupBy("measurement_id")
            .agg(F.sum("row_count").alias("n"))
            .collect()
        ):
            m.set(
                "ripe_silver_rows",
                "Số dòng fact_ping_result của các ngày vừa tính lại",
                r["n"] or 0,
                measurement_id=str(r["measurement_id"]),
            )


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.appName("silver_build").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    run_id = new_run_id("silver")
    with job_metrics("silver", args.run_type) as m:
        try:
            run_sql_file(spark, SQL_DIR / "silver.sql")
            run_sql_file(spark, SQL_DIR / "ops.sql")
            dq = DqRecorder(spark, run_id)
            try:
                probe_dim, msm_dim = build_dimensions(spark, dq)
                if args.dims_only:
                    return 0
                if args.repair_unknown:
                    days = days_needing_repair(spark, probe_dim)
                elif args.dates:
                    days = parse_dates(args.dates)
                else:
                    days = days_for_range(args.start, args.end)
                log_event("silver", "run_start", run_id=run_id, days=[str(d) for d in days])
                results = {str(d): build_day(spark, d, probe_dim, msm_dim, run_id, dq) for d in days}
                log_event("silver", "run_finished", run_id=run_id, results=results)
                push_run_metrics(spark, m, results, days)
                m.ok = "blocked" not in results.values()
                return 0 if m.ok else 2
            finally:
                for severity, n in dq.failed.items():
                    m.set(
                        "ripe_dq_failed_checks", "Số kiểm tra chất lượng thất bại, lần chạy gần nhất", n, layer="silver", severity=severity
                    )
        finally:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
