"""Silver -> Gold marts, chỉ đọc partition Silver đã công bố; kiểm tra nhất quán trước khi ghi.

  spark-submit /opt/app/spark/jobs/gold_build.py --dates 2026-11-19
  spark-submit /opt/app/spark/jobs/gold_build.py --start 2026-11-19T00:00:00Z --end 2026-11-19T03:00:00Z

Mỗi ngày (giờ VN) được tính lại toàn bộ cho mọi mart, ghi bằng overwrite theo điều kiện ictrl_dt = ngày
(kể cả khi kết quả rỗng, để không sót dữ liệu cũ). Mã thoát 2 nếu có ngày bị chặn.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import gold, quality
from lakehouse.common import SQL_DIR, days_for_range, log_event, new_run_id, run_sql_file
from lakehouse.metrics import StageMetrics, job_metrics
from lakehouse.ops import DqRecorder, latest_snapshot, set_partition_status

PROBE_SET_DIR = Path("/opt/app/config/probe_sets")
PUBLISHED = ("published", "published_with_warnings")
MARTS = ["quality_hourly_target", "quality_hourly_country", "quality_hourly_asn", "quality_hourly_probe",
         "rtt_histogram_hourly", "ipv4_ipv6_paired_hourly", "coverage_hourly"]


def parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include an offset")
    return parsed.astimezone(timezone.utc)


def write_mart(spark: SparkSession, name: str, df: DataFrame, day: date) -> int | None:
    table = f"nessie.gold.{name}"
    if not spark.catalog.tableExists(table):
        (df.writeTo(table).using("iceberg").partitionedBy(F.col("ictrl_dt"))
           .tableProperty("format-version", "2").tableProperty("write.parquet.compression-codec", "zstd").create())
    else:
        df.select(*[f.name for f in spark.table(table).schema.fields]).writeTo(table).overwrite(F.col("ictrl_dt") == F.lit(day))
    return latest_snapshot(spark, table)


def gx_target_checks(df: DataFrame, run_id: str) -> list[dict]:
    import great_expectations.expectations as gxe

    exps = [(gxe.ExpectColumnValuesToNotBeNull(column=c), quality.CRITICAL)
            for c in ("ictrl_dt", "event_hour_utc", "event_hour_local", "probe_set_version", "measurement_id")]
    exps += [
        (gxe.ExpectCompoundColumnsToBeUnique(column_list=["probe_set_version", "measurement_id", "event_hour_utc"]), quality.CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="packets_lost", min_value=0), quality.CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="rtt_sum_ms", min_value=0), quality.CRITICAL),
        (gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="packets_sent", column_B="packets_lost", or_equal=True), quality.CRITICAL),
    ]
    return quality.run_gx(df, "gold_quality_hourly_target", exps, run_id)


def build_day(spark: SparkSession, day: date, run_id: str, dq: DqRecorder) -> str:
    built_at = datetime.now(timezone.utc)
    status_rows = (spark.table("nessie.ops.partition_status")
                   .filter((F.col("table_name") == "nessie.silver.fact_ping_result") & (F.col("ictrl_dt") == F.lit(day)))
                   .select("measurement_id", F.col("status").alias("silver_status")).collect())
    silver_status = {r["measurement_id"]: r["silver_status"] for r in status_rows}
    published = [m for m, s in silver_status.items() if s in PUBLISHED]
    log_event("gold", "day_start", ictrl_dt=str(day), silver_status=silver_status)

    # Đọc Silver ĐÚNG snapshot đã công bố (time travel), không đọc bản mới nhất: nếu một lần chạy Silver sau đó chết
    # giữa hai commit (fact đã ghi, reply chưa), Gold vẫn thấy cặp fact/reply nhất quán đã qua kiểm tra.
    # (Sự cố 2026-11-19: Silver OOM sau commit fact -> fact/reply lệch; Gold đọc bản mới nhất bị chặn bởi kiểm tra.)
    pinned = {}
    for table in ("nessie.silver.fact_ping_result", "nessie.silver.fact_ping_reply"):
        snaps = {r["snapshot_id"] for r in spark.table("nessie.ops.partition_status")
                 .filter((F.col("table_name") == table) & (F.col("ictrl_dt") == F.lit(day)) & F.col("status").isin(*PUBLISHED))
                 .select("snapshot_id").collect()}
        if len(snaps) > 1:
            raise RuntimeError(f"{table} {day}: nhiều snapshot công bố khác nhau {snaps}; chạy lại silver_build cho ngày này")
        pinned[table] = next(iter(snaps), None)
    log_event("gold", "silver_pinned", ictrl_dt=str(day), snapshots=pinned)

    def silver(table: str) -> DataFrame:
        reader = spark.read.format("iceberg")
        if pinned[table] is not None:
            reader = reader.option("snapshot-id", str(pinned[table]))
        return reader.load(table).filter((F.col("ictrl_dt") == F.lit(day)) & F.col("measurement_id").isin(published or [-1]))

    facts = silver("nessie.silver.fact_ping_result")
    replies = silver("nessie.silver.fact_ping_reply")
    members = gold.membership(spark, gold.load_probe_sets(PROBE_SET_DIR)).cache()
    cohort = gold.cohort_results(facts, members).cache()
    eligible = cohort.filter("is_eligible").cache()

    marts: dict[str, DataFrame] = {
        "quality_hourly_target": gold.quality_target(eligible),
        "quality_hourly_country": gold.quality_country(eligible),
        "quality_hourly_asn": gold.quality_asn(eligible),
        "quality_hourly_probe": gold.quality_probe(eligible),
        "rtt_histogram_hourly": gold.rtt_histogram(replies, eligible),
        "ipv4_ipv6_paired_hourly": gold.paired(eligible),
    }

    interval = {r["measurement_id"]: r["interval_s"] for r in spark.table("nessie.silver.dim_measurement")
                .filter("is_current AND NOT is_unknown_member").select("measurement_id", "interval_s").collect()}
    processed = (spark.table("nessie.bronze.ingest_manifest")
                 .filter((F.col("entity") == "ping_result_raw") & (F.col("bronze_status") == "loaded"))
                 .groupBy("probe_set_version", "measurement_id", F.col("window_start").alias("event_hour_utc"))
                 .agg(F.countDistinct("manifest_path").alias("manifests"), F.max("unchanged").alias("overlap_unchanged_skipped")))
    status_df = spark.createDataFrame([(day, m, s) for m, s in silver_status.items()] or [(day, -1, "none")],
                                      "ictrl_dt date, measurement_id int, silver_status string")
    quarantine = (spark.table("nessie.bronze.quarantine")
                  .filter((F.col("entity") == "ping_result_raw") & (F.col("ictrl_dt") == F.lit(day)) & F.col("event_ts").isNotNull())
                  .groupBy("measurement_id", F.date_trunc("hour", F.timestamp_seconds("event_ts")).alias("event_hour_utc"))
                  .agg(F.count("*").alias("quarantine_records")))
    marts["coverage_hourly"] = gold.coverage(spark, day, cohort, members, interval, processed, status_df, quarantine)
    marts = {k: gold.with_meta(v, run_id, built_at).cache() for k, v in marts.items()}

    checks = gold.consistency_checks(
        target=marts["quality_hourly_target"], country=marts["quality_hourly_country"], asn=marts["quality_hourly_asn"],
        probe=marts["quality_hourly_probe"], hist=marts["rtt_histogram_hourly"], cov=marts["coverage_hourly"],
        pairs=marts["ipv4_ipv6_paired_hourly"],
    ) + gx_target_checks(marts["quality_hourly_target"], run_id)
    scope = f"ictrl_dt={day}"
    now = datetime.now(timezone.utc)
    msms = sorted(silver_status) or [-1]
    if quality.has_critical_failure(checks):
        dq.write("gold", "nessie.gold.*", scope, checks, None)
        failed = [c["check_name"] for c in checks if c["severity"] == quality.CRITICAL and not c["success"]]
        set_partition_status(spark, [("gold", f"nessie.gold.{k}", day, m, "blocked", None, run_id, dq.dq_run_id, None,
                                      "critical: " + "; ".join(failed)[:900], now) for k in MARTS for m in msms])
        log_event("gold", "day_blocked", ictrl_dt=str(day), failed=failed)
        return "blocked"

    snapshots, counts = {}, {}
    for name in MARTS:
        counts[name] = marts[name].count()
        snapshots[name] = write_mart(spark, name, marts[name], day)
    dq.write("gold", "nessie.gold.*", scope, checks, snapshots["quality_hourly_target"])
    status = "published_with_warnings" if quality.has_warning(checks) else "published"
    set_partition_status(spark, [("gold", f"nessie.gold.{k}", day, m, status if m in published else "blocked_upstream",
                                  counts[k], run_id, dq.dq_run_id, snapshots[k],
                                  None if m in published else f"silver status {silver_status.get(m)}", now)
                                 for k in MARTS for m in msms])
    log_event("gold", "day_written", ictrl_dt=str(day), status=status, rows=counts)
    for df in [cohort, eligible, members, *marts.values()]:
        df.unpersist()
    return status


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dates")
    group.add_argument("--start", type=parse_instant)
    parser.add_argument("--end", type=parse_instant)
    args = parser.parse_args()
    if args.start and not args.end:
        parser.error("--end is required with --start")

    spark = SparkSession.builder.appName("gold_build").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    run_id = new_run_id("gold")
    with job_metrics("gold") as m:
        try:
            run_sql_file(spark, SQL_DIR / "ops" / "001_tables.sql")
            spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
            if args.dates:
                days = [date.fromisoformat(d.strip()) for d in args.dates.split(",") if d.strip()]
            else:
                days = days_for_range(args.start, args.end)
            dq = DqRecorder(spark, run_id)
            try:
                results = {str(d): build_day(spark, d, run_id, dq) for d in days}
                log_event("gold", "run_finished", run_id=run_id, results=results)
                for status in ("published", "published_with_warnings", "blocked"):
                    m.set("ripe_partition_days", "Số ngày theo trạng thái công bố, lần chạy gần nhất",
                          sum(1 for r in results.values() if r == status), layer="gold", status=status)
                push_freshness(spark, m)
                m.ok = "blocked" not in results.values()
                return 2 if "blocked" in results.values() else 0
            finally:
                for severity, n in dq.failed.items():
                    m.set("ripe_dq_failed_checks", "Số kiểm tra chất lượng thất bại, lần chạy gần nhất", n,
                          layer="gold", severity=severity)
        finally:
            spark.stop()


def push_freshness(spark: SparkSession, m: StageMetrics) -> None:
    """Độ tươi và độ phủ theo measurement, đọc từ Gold đã công bố (toàn bộ các ngày)."""
    if not spark.catalog.tableExists("nessie.gold.coverage_hourly"):
        return
    cov = spark.table("nessie.gold.coverage_hourly")
    for r in cov.filter("data_state = 'measured'").groupBy("probe_set_version", "measurement_id").agg(
            F.max("max_event_time").alias("mx")).collect():
        m.set("ripe_gold_max_event_timestamp_seconds", "event_time lớn nhất đã có trong Gold (Unix giây)",
              r["mx"].timestamp(), probe_set_version=r["probe_set_version"], measurement_id=str(r["measurement_id"]))
    # Giờ gần nhất đã có manifest (đã xử lý) theo cohort × measurement: số result và độ đầy đủ ước tính.
    latest = (cov.filter("data_state <> 'not_processed'").groupBy("probe_set_version", "measurement_id")
              .agg(F.max("event_hour_utc").alias("h")))
    for r in cov.join(latest, ["probe_set_version", "measurement_id"]).filter(F.col("event_hour_utc") == F.col("h")).collect():
        labels = {"probe_set_version": r["probe_set_version"], "measurement_id": str(r["measurement_id"])}
        m.set("ripe_gold_latest_hour_results", "Số result của giờ gần nhất đã xử lý", r["results_total_raw"], **labels)
        m.set("ripe_gold_latest_hour_completeness_ratio", "Độ đầy đủ ước tính của giờ gần nhất đã xử lý",
              r["results_total_raw"] / r["expected_results"] if r["expected_results"] else 0, **labels)
        m.set("ripe_gold_latest_hour_timestamp_seconds", "Giờ gần nhất đã xử lý (Unix giây)", r["h"].timestamp(), **labels)
        m.set("ripe_gold_latest_hour_measured", "1 nếu giờ gần nhất đã xử lý có dữ liệu", 1 if r["data_state"] == "measured" else 0, **labels)


if __name__ == "__main__":
    sys.exit(main())
