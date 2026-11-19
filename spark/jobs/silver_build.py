"""Bronze -> Silver star schema, có kiểm tra chất lượng trước khi ghi và trạng thái công bố theo partition.

  spark-submit /opt/app/spark/jobs/silver_build.py --start 2026-11-18T22:00:00Z --end 2026-11-19T02:00:00Z
  spark-submit /opt/app/spark/jobs/silver_build.py --dates 2026-11-19
  spark-submit /opt/app/spark/jobs/silver_build.py --repair-unknown   # metadata đến muộn
  spark-submit /opt/app/spark/jobs/silver_build.py --dims-only

Mỗi ngày (ictrl_dt, Asia/Ho_Chi_Minh) bị ảnh hưởng được tính lại TOÀN BỘ từ Bronze cho cả 4 measurement,
rồi ghi bằng overwrite động theo partition (ictrl_dt, measurement_id). Mã thoát 2 khi có ngày bị chặn
vì lỗi chất lượng critical.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import quality
from lakehouse.common import SQL_DIR, log_event, new_run_id, run_sql_file
from lakehouse.silver import (
    MEASUREMENT_TRACKED, PROBE_TRACKED, asof_join, build_fact_reply, build_fact_result, build_scd2, dim_date,
    dim_time, measurement_snapshots, probe_snapshots, select_result_versions, with_unknown_member,
)

REPORT_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
FACT = "nessie.silver.fact_ping_result"
REPLY = "nessie.silver.fact_ping_reply"


def parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include an offset")
    return parsed.astimezone(timezone.utc)


def days_for_range(start: datetime, end: datetime) -> list[date]:
    first = start.astimezone(REPORT_TZ).date()
    last = (end - timedelta(microseconds=1)).astimezone(REPORT_TZ).date()
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def latest_snapshot(spark: SparkSession, table: str) -> int | None:
    row = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return row[0] if row else None


def table_columns(spark: SparkSession, table: str) -> list[str]:
    return [f.name for f in spark.table(table).schema.fields]


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
    schema = spark.table("nessie.ops.partition_status").schema
    spark.createDataFrame(rows, schema).createOrReplaceTempView("src_status")
    spark.sql("""
        MERGE INTO nessie.ops.partition_status t USING src_status s
        ON t.table_name = s.table_name AND t.ictrl_dt = s.ictrl_dt AND t.measurement_id = s.measurement_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
    """)


# ---------------------------------------------------------------------------- dims
def build_dims(spark: SparkSession, dq: DqRecorder) -> tuple[DataFrame, DataFrame]:
    probe = build_scd2(probe_snapshots(spark.table("nessie.bronze.probe_metadata_raw")), "probe_id", PROBE_TRACKED, "probe", "probe_sk")
    probe = with_unknown_member(spark, probe, "probe_id", "probe_sk").cache()
    msm = build_scd2(measurement_snapshots(spark.table("nessie.bronze.measurement_metadata_raw")),
                     "measurement_id", MEASUREMENT_TRACKED, "measurement", "measurement_sk")
    msm = with_unknown_member(spark, msm, "measurement_id", "measurement_sk").cache()

    blocked = False
    for name, df, sk, key in (("dim_probe", probe, "probe_sk", "probe_id"), ("dim_measurement", msm, "measurement_sk", "measurement_id")):
        checks = quality.run_gx(df, f"silver_{name}", quality.dim_expectations(sk, key), dq.pipeline_run_id)
        checks += quality.scd2_checks(df, key, name)
        if quality.has_critical_failure(checks):
            dq.write("silver", f"nessie.silver.{name}", "all", checks, None)
            blocked = True
            continue
        df.writeTo(f"nessie.silver.{name}").using("iceberg").tableProperty("format-version", "2").createOrReplace()
        dq.write("silver", f"nessie.silver.{name}", "all", checks, latest_snapshot(spark, f"nessie.silver.{name}"))
    if blocked:
        raise RuntimeError("Dimension quality check failed (critical); facts not rebuilt")

    for name, df in (("dim_date", dim_date(spark)), ("dim_time", dim_time(spark))):
        if not spark.catalog.tableExists(f"nessie.silver.{name}"):
            df.writeTo(f"nessie.silver.{name}").using("iceberg").createOrReplace()
    log_event("silver", "dims_written", probe_versions=probe.count() - 1, measurement_versions=msm.count() - 1)
    return probe, msm


# ---------------------------------------------------------------------------- facts
def build_day(spark: SparkSession, day: date, probe_dim: DataFrame, msm_dim: DataFrame, run_id: str, dq: DqRecorder) -> str:
    loaded_at = datetime.now(timezone.utc)
    bronze = spark.table("nessie.bronze.ping_result_raw").filter(F.col("ictrl_dt") == F.lit(day)).cache()
    if bronze.limit(1).count() == 0:
        log_event("silver", "day_empty", ictrl_dt=str(day))
        return "empty"

    facts = asof_join(build_fact_result(select_result_versions(bronze), run_id, loaded_at), probe_dim, msm_dim).cache()
    replies = build_fact_reply(facts).cache()
    fact_out = facts.select(*table_columns(spark, FACT)).cache()
    reply_out = replies.select(*table_columns(spark, REPLY))

    scope = f"ictrl_dt={day}"
    checks_fact = quality.run_gx(fact_out, "silver_fact_ping_result", quality.fact_result_expectations(), run_id)
    checks_reply = quality.run_gx(reply_out, "silver_fact_ping_reply", quality.fact_reply_expectations(), run_id)
    checks_rec = quality.reconciliation_checks(bronze, fact_out, reply_out)
    all_checks = checks_fact + checks_reply + checks_rec

    counts = {r["measurement_id"]: r["n"] for r in fact_out.groupBy("measurement_id").agg(F.count("*").alias("n")).collect()}
    now = datetime.now(timezone.utc)

    if quality.has_critical_failure(all_checks):
        dq.write("silver", FACT, scope, checks_fact + checks_rec, None)
        dq.write("silver", REPLY, scope, checks_reply, None)
        failed = [c["check_name"] for c in all_checks if c["severity"] == quality.CRITICAL and not c["success"]]
        set_partition_status(spark, [("silver", t, day, m, "blocked", n, run_id, dq.dq_run_id, None, "critical: " + "; ".join(failed)[:900], now)
                                     for t in (FACT, REPLY) for m, n in counts.items()])
        log_event("silver", "day_blocked", ictrl_dt=str(day), failed=failed)
        return "blocked"

    fact_out.writeTo(FACT).overwritePartitions()
    reply_out.writeTo(REPLY).overwritePartitions()
    fact_snapshot, reply_snapshot = latest_snapshot(spark, FACT), latest_snapshot(spark, REPLY)

    # Kiểm tra sau khi ghi: số dòng đọc lại của từng partition phải bằng số dòng vừa ghi.
    written = {r["measurement_id"]: r["n"] for r in spark.table(FACT).filter(F.col("ictrl_dt") == F.lit(day))
               .groupBy("measurement_id").agg(F.count("*").alias("n")).collect()}
    post = [quality._check("post_write:row_count_matches", "spark", quality.CRITICAL, written == counts,
                           observed={"written": written, "expected": counts})]
    dq.write("silver", FACT, scope, checks_fact + checks_rec + post, fact_snapshot)
    dq.write("silver", REPLY, scope, checks_reply, reply_snapshot)

    if not post[0]["success"]:
        status, reason = "blocked", f"post-write count mismatch; rollback with CALL nessie.system.rollback_to_snapshot"
    elif quality.has_warning(all_checks):
        status, reason = "published_with_warnings", "; ".join(c["check_name"] for c in all_checks if not c["success"])[:900]
    else:
        status, reason = "published", None
    reply_counts = {r["measurement_id"]: r["n"] for r in reply_out.groupBy("measurement_id").agg(F.count("*").alias("n")).collect()}
    set_partition_status(spark,
                         [("silver", FACT, day, m, status, n, run_id, dq.dq_run_id, fact_snapshot, reason, now) for m, n in counts.items()]
                         + [("silver", REPLY, day, m, status, n, run_id, dq.dq_run_id, reply_snapshot, reason, now) for m, n in reply_counts.items()])
    log_event("silver", "day_written", ictrl_dt=str(day), status=status, rows=counts, replies=reply_counts,
              fact_snapshot=fact_snapshot, reply_snapshot=reply_snapshot)
    for df in (bronze, facts, replies, fact_out):
        df.unpersist()
    return status


def days_needing_repair(spark: SparkSession, probe_dim: DataFrame) -> list[date]:
    if not spark.catalog.tableExists(FACT):
        return []
    known = probe_dim.filter(~F.col("is_unknown_member")).select("probe_id").distinct()
    rows = (spark.table(FACT).filter(F.col("probe_sk") == -1).select("ictrl_dt", "probe_id").distinct()
            .join(known, "probe_id").select("ictrl_dt").distinct().collect())
    return sorted(r["ictrl_dt"] for r in rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--start", type=parse_instant)
    group.add_argument("--dates", help="Danh sách ngày Asia/Ho_Chi_Minh, phân tách bằng dấu phẩy")
    group.add_argument("--repair-unknown", action="store_true")
    group.add_argument("--dims-only", action="store_true")
    parser.add_argument("--end", type=parse_instant)
    args = parser.parse_args()
    if args.start and not args.end:
        parser.error("--end is required with --start")

    spark = SparkSession.builder.appName("silver_build").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    run_id = new_run_id("silver")
    try:
        run_sql_file(spark, SQL_DIR / "silver" / "001_tables.sql")
        run_sql_file(spark, SQL_DIR / "ops" / "001_tables.sql")
        dq = DqRecorder(spark, run_id)
        probe_dim, msm_dim = build_dims(spark, dq)
        if args.dims_only:
            return 0
        if args.repair_unknown:
            days = days_needing_repair(spark, probe_dim)
        elif args.dates:
            days = [date.fromisoformat(d.strip()) for d in args.dates.split(",") if d.strip()]
        else:
            days = days_for_range(args.start, args.end)
        log_event("silver", "run_start", run_id=run_id, days=[str(d) for d in days])
        results = {str(d): build_day(spark, d, probe_dim, msm_dim, run_id, dq) for d in days}
        log_event("silver", "run_finished", run_id=run_id, results=results)
        return 2 if "blocked" in results.values() else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
