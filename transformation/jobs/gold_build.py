"""Job Silver -> Gold marts. Chỉ đọc Silver ĐÃ CÔNG BỐ, kiểm tra nhất quán trước khi ghi.

  spark-submit /opt/app/transformation/jobs/gold_build.py --dates 2026-11-19
  spark-submit /opt/app/transformation/jobs/gold_build.py --start 2026-11-19T00:00:00Z --end 2026-11-19T03:00:00Z

Luồng cho mỗi ngày (giờ VN):
  1. Đọc fact/reply Silver tại snapshot công bố gần nhất (lakehouse/publication.py). Không measurement nào
     có bản công bố -> không ghi đè mart, đánh dấu blocked_upstream.
  2. Lọc result theo cohort (chỉ probe set thực sự được ingest giờ đó), tính 7 mart (lakehouse/gold.py).
  3. Kiểm tra nhất quán giữa các mart + GX (quality/dq). Lỗi critical -> không ghi, blocked.
  4. Ghi overwrite theo ictrl_dt = ngày (kể cả kết quả rỗng), cập nhật ops.partition_status.
  5. Đẩy metric độ tươi/độ phủ lên Pushgateway.
Mã thoát 2 nếu có ngày bị chặn. Định nghĩa metric: docs/metric-dictionary.md.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dq import gx_suites
from dq.checks import gold_consistency_checks
from dq.gx_runner import run_suite
from dq.recorder import DqRecorder
from dq.results import CRITICAL, failed_names, has_critical_failure, has_warning
from lakehouse import gold
from lakehouse.common import SQL_DIR, add_run_type_arg, days_for_range, log_event, new_run_id, parse_dates, parse_instant, run_sql_file
from lakehouse.metrics import StageMetrics, job_metrics
from lakehouse.publication import (
    PUBLISHED,
    PartitionStatus,
    latest_snapshot,
    partition_statuses,
    published_snapshot,
    read_published,
    readable_measurements,
    set_partition_status,
)

PROBE_SET_DIR = Path("/opt/app/config/probe_sets")
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


# ---------------------------------------------------------------------------- đọc đầu vào
def read_published_silver(spark: SparkSession, day: date, measurement_ids: list[int]) -> tuple[DataFrame, DataFrame]:
    """Fact và reply Silver của `day` tại snapshot công bố gần nhất, chỉ các measurement đã công bố."""
    pinned = {t: published_snapshot(spark, t, day) for t in (SILVER_FACT, SILVER_REPLY)}
    log_event("gold", "silver_pinned", ictrl_dt=str(day), snapshots=pinned)
    missing = [t for t, s in pinned.items() if s is None]
    if missing:
        raise RuntimeError(f"{day}: chưa có snapshot Silver công bố cho {missing}")

    def load(table: str) -> DataFrame:
        day_filter = (F.col("ictrl_dt") == F.lit(day)) & F.col("measurement_id").isin(measurement_ids)
        return read_published(spark, table, pinned[table]).filter(day_filter)

    return load(SILVER_FACT), load(SILVER_REPLY)


def ingested_hours(spark: SparkSession, day: date) -> DataFrame:
    """(probe_set_version, measurement_id, event_hour_utc) có manifest results đã nạp vào Bronze trong ngày `day`.

    Lọc theo partition (entity, ictrl_dt) của ingest_manifest nên chi phí không tăng theo lịch sử.
    """
    loaded = (F.col("entity") == "ping_result_raw") & (F.col("ictrl_dt") == F.lit(day)) & (F.col("bronze_status") == "loaded")
    return (
        spark.table("nessie.bronze.ingest_manifest")
        .filter(loaded)
        .groupBy("probe_set_version", "measurement_id", F.col("window_start").alias("event_hour_utc"))
        .agg(F.countDistinct("manifest_path").alias("manifests"), F.max("unchanged").alias("overlap_unchanged_skipped"))
    )


def coverage_inputs(spark: SparkSession, day: date, silver_status: dict[int, str]):
    """Đầu vào phụ cho mart coverage: interval đo, trạng thái Silver, số bản ghi quarantine theo giờ."""
    interval = {
        r["measurement_id"]: r["interval_s"]
        for r in spark.table("nessie.silver.dim_measurement")
        .filter("is_current AND NOT is_unknown_member")
        .select("measurement_id", "interval_s")
        .collect()
    }
    status = spark.createDataFrame(
        [(day, m, s) for m, s in silver_status.items()] or [(day, -1, "none")], "ictrl_dt date, measurement_id int, silver_status string"
    )
    quarantine = (
        spark.table("nessie.bronze.quarantine")
        .filter((F.col("entity") == "ping_result_raw") & (F.col("ictrl_dt") == F.lit(day)) & F.col("event_ts").isNotNull())
        .groupBy("measurement_id", F.date_trunc("hour", F.timestamp_seconds("event_ts")).alias("event_hour_utc"))
        .agg(F.count("*").alias("quarantine_records"))
    )
    return interval, status, quarantine


# ---------------------------------------------------------------------------- ghi
def write_mart(spark: SparkSession, name: str, df: DataFrame, day: date) -> int | None:
    table = f"nessie.gold.{name}"
    if not spark.catalog.tableExists(table):
        (
            df.writeTo(table)
            .using("iceberg")
            .partitionedBy(F.col("ictrl_dt"))
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .create()
        )
    else:
        df.select(*[f.name for f in spark.table(table).schema.fields]).writeTo(table).overwrite(F.col("ictrl_dt") == F.lit(day))
    return latest_snapshot(spark, table)


def mark_marts_blocked(spark: SparkSession, day: date, measurement_ids: list[int], status: str, reason: str, run_id: str, dq_run_id: str):
    """Đánh dấu mọi mart của ngày `day` là bị chặn; snapshot Gold đã công bố trước đó được giữ nguyên."""
    now = datetime.now(timezone.utc)
    rows = [
        PartitionStatus("gold", f"nessie.gold.{k}", day, m, status, None, run_id, dq_run_id, None, reason, now)
        for k in MARTS
        for m in measurement_ids
    ]
    set_partition_status(spark, rows)


# ---------------------------------------------------------------------------- một ngày
def build_day(spark: SparkSession, day: date, run_id: str, dq: DqRecorder) -> str:
    cached: list[DataFrame] = []
    try:
        return _build_day(spark, day, run_id, dq, cached)
    finally:  # mọi nhánh đều giải phóng cache, tránh dồn bộ nhớ driver khi backfill nhiều ngày
        for df in cached:
            df.unpersist()


def _build_day(spark: SparkSession, day: date, run_id: str, dq: DqRecorder, cached: list[DataFrame]) -> str:
    def cache(df: DataFrame) -> DataFrame:
        cached.append(df.cache())
        return cached[-1]

    built_at = datetime.now(timezone.utc)
    silver_status = partition_statuses(spark, SILVER_FACT, day)
    readable = readable_measurements(spark, SILVER_FACT, day)
    log_event("gold", "day_start", ictrl_dt=str(day), silver_status=silver_status, readable=readable)
    if not readable:
        # Không measurement nào có bản Silver công bố: KHÔNG ghi đè mart (bản Gold cũ, nếu có, giữ nguyên).
        reason = "no published silver snapshot"
        mark_marts_blocked(spark, day, sorted(silver_status) or [-1], "blocked_upstream", reason, run_id, dq.dq_run_id)
        log_event("gold", "day_skipped", ictrl_dt=str(day), reason=reason)
        return "blocked_upstream"

    facts, replies = read_published_silver(spark, day, readable)
    hours = cache(ingested_hours(spark, day))
    all_members = gold.membership(spark, gold.load_probe_sets(PROBE_SET_DIR))
    members = cache(all_members.join(hours.select("probe_set_version").distinct(), "probe_set_version", "left_semi"))
    cohort = cache(gold.cohort_results(facts, members, hours))
    eligible = cache(cohort.filter("is_eligible"))

    interval, status_df, quarantine = coverage_inputs(spark, day, silver_status)
    marts: dict[str, DataFrame] = {
        "quality_hourly_target": gold.quality_target(eligible),
        "quality_hourly_country": gold.quality_country(eligible),
        "quality_hourly_asn": gold.quality_asn(eligible),
        "quality_hourly_probe": gold.quality_probe(eligible),
        "rtt_histogram_hourly": gold.rtt_histogram(replies, eligible),
        "ipv4_ipv6_paired_hourly": gold.paired(eligible),
        "coverage_hourly": gold.coverage(spark, day, cohort, members, interval, hours, status_df, quarantine),
    }
    marts = {k: cache(gold.with_meta(v, run_id, built_at)) for k, v in marts.items()}

    checks = gold_consistency_checks(
        target=marts["quality_hourly_target"],
        country=marts["quality_hourly_country"],
        asn=marts["quality_hourly_asn"],
        probe=marts["quality_hourly_probe"],
        hist=marts["rtt_histogram_hourly"],
        cov=marts["coverage_hourly"],
        pairs=marts["ipv4_ipv6_paired_hourly"],
    ) + run_suite(marts["quality_hourly_target"], "gold_quality_hourly_target", gx_suites.gold_quality_target(), run_id)
    scope = f"ictrl_dt={day}"
    measurement_ids = sorted(set(silver_status) | set(readable))

    if has_critical_failure(checks):
        dq.write("gold", "nessie.gold.*", scope, checks, None)
        failed = failed_names(checks, CRITICAL)
        mark_marts_blocked(spark, day, measurement_ids, "blocked", "critical: " + "; ".join(failed)[:900], run_id, dq.dq_run_id)
        log_event("gold", "day_blocked", ictrl_dt=str(day), failed=failed)
        return "blocked"

    snapshots, counts = {}, {}
    for name in MARTS:
        counts[name] = marts[name].count()
        snapshots[name] = write_mart(spark, name, marts[name], day)
    dq.write("gold", "nessie.gold.*", scope, checks, snapshots["quality_hourly_target"])

    status = "published_with_warnings" if has_warning(checks) else "published"
    now = datetime.now(timezone.utc)
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
    set_partition_status(spark, rows)
    log_event("gold", "day_written", ictrl_dt=str(day), status=status, rows=counts)
    return status


# ---------------------------------------------------------------------------- metric vận hành
def push_freshness(spark: SparkSession, m: StageMetrics) -> None:
    """Độ tươi và độ phủ theo cohort × measurement, đọc từ Gold đã công bố (mọi ngày)."""
    if not spark.catalog.tableExists("nessie.gold.coverage_hourly"):
        return
    cov = spark.table("nessie.gold.coverage_hourly")
    for r in (
        cov.filter("data_state = 'measured'")
        .groupBy("probe_set_version", "measurement_id")
        .agg(F.max("max_event_time").alias("mx"))
        .collect()
    ):
        m.set(
            "ripe_gold_max_event_timestamp_seconds",
            "event_time lớn nhất đã có trong Gold (Unix giây)",
            r["mx"].timestamp(),
            probe_set_version=r["probe_set_version"],
            measurement_id=str(r["measurement_id"]),
        )
    latest = (
        cov.filter("data_state <> 'not_processed'").groupBy("probe_set_version", "measurement_id").agg(F.max("event_hour_utc").alias("h"))
    )
    for r in cov.join(latest, ["probe_set_version", "measurement_id"]).filter(F.col("event_hour_utc") == F.col("h")).collect():
        labels = {"probe_set_version": r["probe_set_version"], "measurement_id": str(r["measurement_id"])}
        m.set("ripe_gold_latest_hour_results", "Số result của giờ gần nhất đã xử lý", r["results_total_raw"], **labels)
        if r["expected_results"]:  # expected = 0 (vd probe set không có probe IPv6): độ đầy đủ không xác định, không đẩy
            m.set(
                "ripe_gold_latest_hour_completeness_ratio",
                "Độ đầy đủ ước tính của giờ gần nhất đã xử lý",
                r["results_total_raw"] / r["expected_results"],
                **labels,
            )
        m.set("ripe_gold_latest_hour_timestamp_seconds", "Giờ gần nhất đã xử lý (Unix giây)", r["h"].timestamp(), **labels)
        m.set(
            "ripe_gold_latest_hour_measured", "1 nếu giờ gần nhất đã xử lý có dữ liệu", 1 if r["data_state"] == "measured" else 0, **labels
        )


# ---------------------------------------------------------------------------- entry point
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dates", help="Danh sách ngày Asia/Ho_Chi_Minh, phân tách bằng dấu phẩy")
    group.add_argument("--start", type=parse_instant)
    parser.add_argument("--end", type=parse_instant)
    add_run_type_arg(parser)
    args = parser.parse_args()
    if args.start and not args.end:
        parser.error("--end is required with --start")
    return args


def main() -> int:
    args = parse_args()
    spark = SparkSession.builder.appName("gold_build").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    run_id = new_run_id("gold")
    with job_metrics("gold", args.run_type) as m:
        try:
            run_sql_file(spark, SQL_DIR / "ops.sql")
            spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
            days = parse_dates(args.dates) if args.dates else days_for_range(args.start, args.end)
            dq = DqRecorder(spark, run_id)
            try:
                results = {str(d): build_day(spark, d, run_id, dq) for d in days}
                log_event("gold", "run_finished", run_id=run_id, results=results)
                for status in ("published", "published_with_warnings", "blocked", "blocked_upstream"):
                    m.set(
                        "ripe_partition_days",
                        "Số ngày theo trạng thái công bố, lần chạy gần nhất",
                        sum(1 for r in results.values() if r == status),
                        layer="gold",
                        status=status,
                    )
                push_freshness(spark, m)
                m.ok = "blocked" not in results.values()
                return 0 if m.ok else 2
            finally:
                for severity, n in dq.failed.items():
                    m.set("ripe_dq_failed_checks", "Số kiểm tra chất lượng thất bại, lần chạy gần nhất", n, layer="gold", severity=severity)
        finally:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
