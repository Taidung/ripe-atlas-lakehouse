"""Kiểm tra chất lượng viết bằng Spark (những gì GX không diễn đạt gọn): SCD2, đối soát Bronze/Silver, nhất quán Gold."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq.results import CRITICAL, CheckResult, make_check


# ---------------------------------------------------------------------------- Silver: dimension SCD2
def scd2_checks(dim: DataFrame, key: str, name: str) -> list[CheckResult]:
    """Không chồng lấn hiệu lực, đúng một bản current mỗi key, các phiên bản liền mạch, có đúng một unknown member."""
    real = dim.filter(~F.col("is_unknown_member"))
    a, b = real.alias("a"), real.alias("b")
    overlaps = a.join(
        b,
        (F.col(f"a.{key}") == F.col(f"b.{key}"))
        & (F.col("a.version_no") < F.col("b.version_no"))
        & (F.col("a.valid_from") < F.col("b.valid_to"))
        & (F.col("b.valid_from") < F.col("a.valid_to")),
    ).count()
    not_one_current = real.groupBy(key).agg(F.sum(F.col("is_current").cast("int")).alias("n")).filter("n != 1").count()
    gaps = (
        real.alias("x")
        .join(real.alias("y"), (F.col(f"x.{key}") == F.col(f"y.{key}")) & (F.col("y.version_no") == F.col("x.version_no") + 1), "inner")
        .filter(F.col("x.valid_to") != F.col("y.valid_from"))
        .count()
    )
    unknown_members = dim.filter(F.col("is_unknown_member")).count()
    return [
        make_check(f"{name}:scd2_no_overlap", "spark", CRITICAL, overlaps == 0, unexpected=overlaps),
        make_check(f"{name}:scd2_one_current_per_key", "spark", CRITICAL, not_one_current == 0, unexpected=not_one_current),
        make_check(f"{name}:scd2_contiguous_versions", "spark", CRITICAL, gaps == 0, unexpected=gaps),
        make_check(f"{name}:unknown_member_present", "spark", CRITICAL, unknown_members == 1, observed=unknown_members),
    ]


# ---------------------------------------------------------------------------- Silver: đối soát với Bronze
def silver_reconciliation_checks(bronze: DataFrame, facts: DataFrame, replies: DataFrame) -> list[CheckResult]:
    """Số khóa phân biệt ở Bronze = số dòng fact; số reply = tổng số phần tử; reply hợp lệ = valid_rtt_count."""
    bronze_keys = bronze.select("measurement_id", "probe_id", "event_ts").distinct().count()
    fact_rows = facts.count()
    fact_totals = facts.agg(F.sum("reply_element_count").alias("elements"), F.sum("valid_rtt_count").alias("valid")).first()
    reply_totals = replies.agg(F.count("*").alias("rows"), F.sum(F.col("is_valid_rtt").cast("int")).alias("valid")).first()
    orphan_replies = replies.join(facts.select("result_id"), "result_id", "left_anti").count()
    elements, valid = fact_totals["elements"] or 0, fact_totals["valid"] or 0
    return [
        make_check(
            "reconcile:bronze_keys_eq_fact_rows",
            "spark",
            CRITICAL,
            bronze_keys == fact_rows,
            observed={"bronze_distinct_keys": bronze_keys, "fact_rows": fact_rows},
        ),
        make_check(
            "reconcile:reply_rows_eq_elements",
            "spark",
            CRITICAL,
            reply_totals["rows"] == elements,
            observed={"reply_rows": reply_totals["rows"], "result_elements": elements},
        ),
        make_check(
            "reconcile:valid_replies_eq_valid_rtt_count",
            "spark",
            CRITICAL,
            (reply_totals["valid"] or 0) == valid,
            observed={"reply_valid": reply_totals["valid"], "fact_valid_rtt_count": valid},
        ),
        make_check("reconcile:no_orphan_replies", "spark", CRITICAL, orphan_replies == 0, unexpected=orphan_replies),
    ]


# ---------------------------------------------------------------------------- Gold: nhất quán giữa các mart
GOLD_HOUR_KEYS = ["ictrl_dt", "event_hour_utc", "event_hour_local", "probe_set_version", "measurement_id"]
GOLD_ADDITIVE_MEASURES = ["results_total", "results_measured", "results_with_response", "packets_sent", "packets_lost", "rtt_count"]


def gold_consistency_checks(
    target: DataFrame, country: DataFrame, asn: DataFrame, probe: DataFrame, hist: DataFrame, cov: DataFrame, pairs: DataFrame
) -> list[CheckResult]:
    """Các mart cùng tính từ một tập result nên tổng phải khớp nhau; tránh lặp logic sai lệch giữa các mart."""

    def totals(df: DataFrame, prefix: str) -> DataFrame:
        return df.groupBy(*GOLD_HOUR_KEYS).agg(*[F.sum(m).alias(f"{prefix}_{m}") for m in GOLD_ADDITIVE_MEASURES])

    checks = []
    target_totals = totals(target, "t")
    for name, mart in (("country", country), ("asn", asn), ("probe", probe)):
        mismatch = (
            target_totals.join(totals(mart, name), GOLD_HOUR_KEYS, "full_outer")
            .filter(" OR ".join(f"NOT (t_{m} <=> {name}_{m})" for m in GOLD_ADDITIVE_MEASURES))
            .count()
        )
        checks.append(make_check(f"gold:target_eq_sum_{name}", "spark", CRITICAL, mismatch == 0, unexpected=mismatch))

    histogram_totals = hist.groupBy(*GOLD_HOUR_KEYS).agg(F.sum("reply_count").alias("h"))
    bad_histogram = (
        target.select(*GOLD_HOUR_KEYS, "rtt_count")
        .join(histogram_totals, GOLD_HOUR_KEYS, "full_outer")
        .filter("NOT (coalesce(rtt_count,0) = coalesce(h,0))")
        .count()
    )
    checks.append(make_check("gold:histogram_total_eq_rtt_count", "spark", CRITICAL, bad_histogram == 0, unexpected=bad_histogram))

    bad_relations = target.filter(
        "packets_lost <> packets_sent - packets_received OR results_with_response > results_measured "
        "OR results_measured > results_total OR packets_lost < 0"
    ).count()
    checks.append(make_check("gold:measure_relations", "spark", CRITICAL, bad_relations == 0, unexpected=bad_relations))

    coverage_keys = ["probe_set_version", "measurement_id", "event_hour_utc"]
    coverage_mismatch = (
        cov.select(*coverage_keys, "results_eligible")
        .join(target.select(*coverage_keys, "results_total"), coverage_keys, "full_outer")
        .filter("NOT (coalesce(results_eligible,0) = coalesce(results_total,0))")
        .count()
    )
    checks.append(
        make_check("gold:coverage_eligible_eq_target_results", "spark", CRITICAL, coverage_mismatch == 0, unexpected=coverage_mismatch)
    )

    grid_rows = cov.count()
    expected_grid = cov.select("probe_set_version").distinct().count() * 24 * 4
    checks.append(
        make_check(
            "gold:coverage_full_hour_grid",
            "spark",
            CRITICAL,
            grid_rows == expected_grid,
            observed={"rows": grid_rows, "expected": expected_grid},
        )
    )

    bad_pairs = pairs.filter("pairs < 0 OR v4_packets_lost > v4_packets_sent OR v6_packets_lost > v6_packets_sent").count()
    checks.append(make_check("gold:pair_relations", "spark", CRITICAL, bad_pairs == 0, unexpected=bad_pairs))
    return checks
