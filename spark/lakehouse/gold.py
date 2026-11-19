"""Silver -> Gold marts (hàm thuần). Định nghĩa metric: docs/metric-dictionary.md.

Mọi mart chỉ lưu tử số/mẫu số và thống kê cộng được; tỷ lệ được tính lại sau khi group.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

REPORT_TZ = "Asia/Ho_Chi_Minh"
BUCKET_BASE_MS = 0.1
BUCKET_RATIO = 1.02
PAIR_MAX_DT_S = 120
TARGET_PAIRS = {"k.root-servers.net": (1001, 2001), "f.root-servers.net": (1004, 2004)}
UNKNOWN_COUNTRY = "ZZ"
UNKNOWN_ASN = -1

HOUR_KEYS = ["ictrl_dt", "event_hour_utc", "event_hour_local", "probe_set_version"]
MSM_KEYS = ["measurement_id", "af", "target"]


# ---------------------------------------------------------------------------- helpers
def local_hour(col: str | Column) -> Column:
    """Giờ treo tường Asia/Ho_Chi_Minh, kiểu timestamp_ntz (không múi giờ) cho Superset/Trino."""
    return F.from_utc_timestamp(col, REPORT_TZ).cast("timestamp_ntz")


def bucket_id(rtt: str | Column) -> Column:
    return F.floor(F.log(F.greatest(F.col(rtt) if isinstance(rtt, str) else rtt, F.lit(BUCKET_BASE_MS)) / BUCKET_BASE_MS)
                   / math.log(BUCKET_RATIO)).cast("int")


def bucket_rep_ms(b: Column) -> Column:
    return F.lit(BUCKET_BASE_MS) * F.pow(F.lit(BUCKET_RATIO), b + 0.5)


def load_probe_sets(directory: Path) -> list[dict[str, Any]]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(directory.glob("*.json"))]


def membership(spark: SparkSession, probe_sets: list[dict[str, Any]]) -> DataFrame:
    rows = [(ps["version"], af, pid) for ps in probe_sets for af, key in ((4, "probes_v4"), (6, "probes_v6")) for pid in ps[key]]
    return spark.createDataFrame(rows, "probe_set_version string, af int, probe_id long")


# ---------------------------------------------------------------------------- inputs
def cohort_results(facts: DataFrame, members: DataFrame) -> DataFrame:
    """Mọi result thuộc cohort (trước khi lọc public/clock); thêm cột giờ và mã unknown cho country/asn."""
    f = facts.drop("probe_set_version").join(F.broadcast(members), ["af", "probe_id"])
    return (f.withColumn("event_hour_local", local_hour("event_hour_utc"))
             .withColumn("country_code", F.coalesce("country_code", F.lit(UNKNOWN_COUNTRY)))
             .withColumn("asn", F.coalesce("asn", F.lit(UNKNOWN_ASN)))
             .withColumn("is_eligible", (F.col("public_status") == "public") & ~F.col("clock_suspect")))


def measure_aggs() -> list[Column]:
    sent, rcvd = F.col("packets_sent"), F.col("packets_received")
    measured = sent > 0
    return [
        F.count("*").alias("results_total"),
        F.sum(measured.cast("long")).alias("results_measured"),
        F.sum((F.col("result_status") == "send_error").cast("long")).alias("results_send_error"),
        F.sum((measured & (rcvd > 0)).cast("long")).alias("results_with_response"),
        F.sum((F.col("result_status") == "all_lost").cast("long")).alias("results_all_lost"),
        F.sum((F.col("result_status") == "partial_loss").cast("long")).alias("results_partial_loss"),
        F.coalesce(F.sum(F.when(measured, sent)), F.lit(0)).cast("long").alias("packets_sent"),
        F.coalesce(F.sum(F.when(measured, rcvd)), F.lit(0)).cast("long").alias("packets_received"),
        F.coalesce(F.sum(F.when(measured, sent - rcvd)), F.lit(0)).cast("long").alias("packets_lost"),
        F.coalesce(F.sum("valid_rtt_count"), F.lit(0)).cast("long").alias("rtt_count"),
        F.sum("rtt_sum_ms").alias("rtt_sum_ms"),
        F.sum("rtt_sumsq_ms2").alias("rtt_sumsq_ms2"),
        F.min("rtt_min_ms").alias("rtt_min_ms"),
        F.max("rtt_max_ms").alias("rtt_max_ms"),
    ]


def with_meta(df: DataFrame, run_id: str, built_at) -> DataFrame:
    return df.withColumn("gold_run_id", F.lit(run_id)).withColumn("gold_built_at", F.lit(built_at).cast("timestamp"))


# ---------------------------------------------------------------------------- quality marts
def quality_target(eligible: DataFrame) -> DataFrame:
    return eligible.groupBy(*HOUR_KEYS, *MSM_KEYS).agg(
        *measure_aggs(),
        F.countDistinct("probe_id").alias("probes_observed"),
        F.countDistinct(F.when(F.col("country_code") != UNKNOWN_COUNTRY, F.col("country_code"))).alias("countries_observed"),
        F.countDistinct(F.when(F.col("asn") != UNKNOWN_ASN, F.col("asn"))).alias("asns_observed"))


def quality_country(eligible: DataFrame) -> DataFrame:
    return eligible.groupBy(*HOUR_KEYS, *MSM_KEYS, "country_code").agg(
        *measure_aggs(), F.countDistinct("probe_id").alias("probes_observed"),
        F.countDistinct(F.when(F.col("asn") != UNKNOWN_ASN, F.col("asn"))).alias("asns_observed"))


def quality_asn(eligible: DataFrame) -> DataFrame:
    return eligible.groupBy(*HOUR_KEYS, *MSM_KEYS, "asn").agg(
        *measure_aggs(), F.countDistinct("probe_id").alias("probes_observed"),
        F.countDistinct("country_code").alias("asn_country_count"),
        F.min("country_code").alias("asn_primary_country"))


def quality_probe(eligible: DataFrame) -> DataFrame:
    return eligible.groupBy(*HOUR_KEYS, *MSM_KEYS, "probe_id").agg(
        *measure_aggs(), F.max("probe_sk").alias("probe_sk"),
        F.first("country_code").alias("country_code"), F.first("asn").alias("asn"))


def rtt_histogram(replies: DataFrame, eligible: DataFrame) -> DataFrame:
    keys = eligible.select("result_id", *HOUR_KEYS, *MSM_KEYS, "country_code", "asn")
    valid = replies.filter("is_valid_rtt").select("result_id", "rtt_ms").withColumn("bucket_id", bucket_id("rtt_ms"))
    h = valid.join(keys, "result_id").groupBy(*HOUR_KEYS, *MSM_KEYS, "country_code", "asn", "bucket_id") \
        .agg(F.count("*").alias("reply_count"))
    b = F.col("bucket_id")
    return (h.withColumn("bucket_rep_ms", bucket_rep_ms(b))
             .withColumn("bucket_lower_ms", F.when(b == 0, F.lit(0.0)).otherwise(F.lit(BUCKET_BASE_MS) * F.pow(F.lit(BUCKET_RATIO), b)))
             .withColumn("bucket_upper_ms", F.lit(BUCKET_BASE_MS) * F.pow(F.lit(BUCKET_RATIO), b + 1)))


# ---------------------------------------------------------------------------- IPv4 / IPv6 pairs
def paired(eligible: DataFrame) -> DataFrame:
    cols = ["probe_set_version", "target", "probe_id", "event_ts", "event_hour_utc", "event_hour_local", "ictrl_dt",
            "packets_sent", "packets_lost", "rtt_sum_ms", "valid_rtt_count", "country_code", "asn", "result_id"]
    v4 = eligible.filter("af = 4").select(*[F.col(c).alias(f"v4_{c}") if c not in ("probe_set_version", "target", "probe_id") else c for c in cols])
    v6 = eligible.filter("af = 6").select(*[F.col(c).alias(f"v6_{c}") if c not in ("probe_set_version", "target", "probe_id") else c for c in cols])
    cand = v4.join(v6, ["probe_set_version", "target", "probe_id"]).withColumn("dt", F.abs(F.col("v6_event_ts") - F.col("v4_event_ts"))) \
        .filter(F.col("dt") <= PAIR_MAX_DT_S)
    r4 = Window.partitionBy("v4_result_id").orderBy("dt", "v6_event_ts")
    r6 = Window.partitionBy("v6_result_id").orderBy("dt", "v4_event_ts")
    pairs = cand.withColumn("r4", F.row_number().over(r4)).withColumn("r6", F.row_number().over(r6)).filter("r4 = 1 AND r6 = 1")

    keys = ["probe_set_version", "target", "probe_id", "ictrl_dt", "event_hour_utc", "event_hour_local"]
    # Cặp được xếp vào giờ của result v4.
    agg_pairs = (pairs.withColumn("ictrl_dt", F.col("v4_ictrl_dt"))
                 .withColumn("event_hour_utc", F.col("v4_event_hour_utc"))
                 .withColumn("event_hour_local", F.col("v4_event_hour_local"))
                 ).groupBy(*keys).agg(
        F.count("*").alias("pairs"), F.avg("dt").alias("pair_dt_mean_s"),
        F.first("v4_country_code").alias("country_code"), F.first("v4_asn").alias("asn_v4"), F.first("v6_asn").alias("asn_v6"),
        *[F.coalesce(F.sum(f"{s}_{m}"), F.lit(0)).cast("long" if m != "rtt_sum_ms" else "double").alias(f"{s}_{m}")
          for s in ("v4", "v6") for m in ("packets_sent", "packets_lost", "valid_rtt_count")],
        F.sum("v4_rtt_sum_ms").alias("v4_rtt_sum_ms"), F.sum("v6_rtt_sum_ms").alias("v6_rtt_sum_ms"))

    paired_ids = pairs.select(F.col("v4_result_id").alias("result_id")).unionByName(pairs.select(F.col("v6_result_id").alias("result_id")))
    unpaired = (eligible.join(paired_ids, "result_id", "left_anti")
                .groupBy(*keys).agg(F.sum((F.col("af") == 4).cast("long")).alias("unpaired_v4_results"),
                                    F.sum((F.col("af") == 6).cast("long")).alias("unpaired_v6_results"),
                                    F.first("country_code").alias("_cc")))
    out = agg_pairs.join(unpaired, keys, "full_outer")
    out = (out.withColumn("country_code", F.coalesce("country_code", "_cc")).drop("_cc")
              .fillna(0, subset=["pairs", "unpaired_v4_results", "unpaired_v6_results", "v4_packets_sent", "v4_packets_lost",
                                 "v4_valid_rtt_count", "v6_packets_sent", "v6_packets_lost", "v6_valid_rtt_count"]))
    msm = F.create_map(*[x for t, (a, b) in TARGET_PAIRS.items() for x in (F.lit(t), F.array(F.lit(a), F.lit(b)))])
    return (out.withColumnRenamed("v4_valid_rtt_count", "v4_rtt_count").withColumnRenamed("v6_valid_rtt_count", "v6_rtt_count")
               .withColumn("measurement_id_v4", msm[F.col("target")][0]).withColumn("measurement_id_v6", msm[F.col("target")][1]))


# ---------------------------------------------------------------------------- coverage
def coverage(spark: SparkSession, day, cohort: DataFrame, members: DataFrame, interval_s: dict[int, int],
             processed: DataFrame, silver_status: DataFrame, quarantine: DataFrame) -> DataFrame:
    """Lưới đủ 24 giờ (giờ VN) × cohort × measurement, kể cả giờ không có dữ liệu."""
    from lakehouse.silver import MEASUREMENTS

    hours = spark.sql(f"""SELECT explode(sequence(TIMESTAMP_NTZ'{day} 00:00:00', TIMESTAMP_NTZ'{day} 23:00:00', INTERVAL 1 HOUR)) AS event_hour_local""")
    hours = hours.withColumn("event_hour_utc", F.to_utc_timestamp(F.col("event_hour_local").cast("timestamp"), REPORT_TZ)) \
        .withColumn("ictrl_dt", F.lit(day).cast("date"))
    msm = spark.createDataFrame([(m, af, t, interval_s.get(m, 240)) for m, (af, t) in MEASUREMENTS.items()],
                                "measurement_id int, af int, target string, interval_s int")
    expected = members.groupBy("probe_set_version", "af").agg(F.count("*").alias("expected_probes"))
    grid = hours.crossJoin(msm).join(expected, "af")

    observed = cohort.groupBy("probe_set_version", "measurement_id", "event_hour_utc").agg(
        F.count("*").alias("results_total_raw"),
        F.sum(F.col("is_eligible").cast("long")).alias("results_eligible"),
        F.sum((F.col("public_status") != "public").cast("long")).alias("results_excluded_not_public"),
        F.sum(((F.col("public_status") == "public") & F.col("clock_suspect")).cast("long")).alias("results_excluded_clock"),
        F.countDistinct(F.when(F.col("is_eligible"), F.col("probe_id"))).alias("probes_observed"),
        F.sum(F.col("is_source_corrected").cast("long")).alias("results_source_corrected"),
        F.max("event_time").alias("max_event_time"),
        F.max("silver_loaded_at").alias("silver_loaded_at"))

    g = (grid.join(observed, ["probe_set_version", "measurement_id", "event_hour_utc"], "left")
             .join(processed, ["probe_set_version", "measurement_id", "event_hour_utc"], "left")
             .join(silver_status, ["ictrl_dt", "measurement_id"], "left")
             .join(quarantine, ["measurement_id", "event_hour_utc"], "left"))
    g = g.fillna(0, subset=["results_total_raw", "results_eligible", "results_excluded_not_public", "results_excluded_clock",
                            "probes_observed", "results_source_corrected", "quarantine_records", "overlap_unchanged_skipped",
                            "manifests"])
    expected_results = F.col("expected_probes") * (F.lit(3600) / F.col("interval_s"))
    return (g.withColumn("expected_results", expected_results.cast("long"))
             .withColumn("data_state", F.when(F.col("results_total_raw") > 0, "measured")
                         .when(F.col("manifests") > 0, "processed_no_results").otherwise("not_processed"))
             .withColumn("silver_status", F.coalesce("silver_status", F.lit("not_built"))))


# ---------------------------------------------------------------------------- consistency checks
def consistency_checks(target: DataFrame, country: DataFrame, asn: DataFrame, probe: DataFrame, hist: DataFrame,
                       cov: DataFrame, pairs: DataFrame) -> list[dict[str, Any]]:
    from lakehouse.quality import CRITICAL, _check

    keys = HOUR_KEYS + ["measurement_id"]
    measures = ["results_total", "results_measured", "results_with_response", "packets_sent", "packets_lost", "rtt_count"]

    def totals(df, name):
        return df.groupBy(*keys).agg(*[F.sum(m).alias(f"{name}_{m}") for m in measures])

    t = totals(target, "t")
    checks = []
    for name, df in (("country", country), ("asn", asn), ("probe", probe)):
        diff = t.join(totals(df, name), keys, "full_outer").filter(" OR ".join(
            f"NOT (t_{m} <=> {name}_{m})" for m in measures)).count()
        checks.append(_check(f"gold:target_eq_sum_{name}", "spark", CRITICAL, diff == 0, unexpected=diff))
    h = hist.groupBy(*keys).agg(F.sum("reply_count").alias("h"))
    bad_hist = target.select(*keys, "rtt_count").join(h, keys, "full_outer").filter("NOT (coalesce(rtt_count,0) = coalesce(h,0))").count()
    checks.append(_check("gold:histogram_total_eq_rtt_count", "spark", CRITICAL, bad_hist == 0, unexpected=bad_hist))
    bad_rel = target.filter("packets_lost <> packets_sent - packets_received OR results_with_response > results_measured "
                            "OR results_measured > results_total OR packets_lost < 0").count()
    checks.append(_check("gold:measure_relations", "spark", CRITICAL, bad_rel == 0, unexpected=bad_rel))
    cov_mismatch = (cov.select("probe_set_version", "measurement_id", "event_hour_utc", "results_eligible")
                    .join(target.select("probe_set_version", "measurement_id", "event_hour_utc", "results_total"),
                          ["probe_set_version", "measurement_id", "event_hour_utc"], "full_outer")
                    .filter("NOT (coalesce(results_eligible,0) = coalesce(results_total,0))").count())
    checks.append(_check("gold:coverage_eligible_eq_target_results", "spark", CRITICAL, cov_mismatch == 0, unexpected=cov_mismatch))
    grid = cov.count()
    expected_grid = cov.select("probe_set_version").distinct().count() * 24 * 4
    checks.append(_check("gold:coverage_full_hour_grid", "spark", CRITICAL, grid == expected_grid,
                         observed={"rows": grid, "expected": expected_grid}))
    bad_pairs = pairs.filter("pairs < 0 OR v4_packets_lost > v4_packets_sent OR v6_packets_lost > v6_packets_sent").count()
    checks.append(_check("gold:pair_relations", "spark", CRITICAL, bad_pairs == 0, unexpected=bad_pairs))
    return checks
