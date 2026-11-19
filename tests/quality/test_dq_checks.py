"""Thành phần chất lượng: kiểm tra SCD2, đối soát Bronze/Silver, GX phát hiện lỗi critical và để dữ liệu sạch đi qua."""

import pytest
from pyspark.sql import functions as F

from dq import gx_runner, gx_suites
from dq.checks import scd2_checks, silver_reconciliation_checks
from dq.results import has_critical_failure
from lakehouse.silver import build_fact_reply
from tests.spark_support import bronze_rows, build_probe_dim, facts_for, ts

pytestmark = pytest.mark.spark

HISTORY = [
    (7, "2026-11-01 00:00:00", {"country_code": "VN", "asn_v4": 100}),
    (7, "2026-11-03 00:00:00", {"country_code": "VN", "asn_v4": 200}),
    (7, "2026-11-05 00:00:00", {"country_code": "VN", "asn_v4": 100}),
]


def test_scd2_checks_pass_on_valid_dimension(spark):
    assert all(c["success"] for c in scd2_checks(build_probe_dim(spark, HISTORY), "probe_id", "dim_probe"))


def test_scd2_checks_detect_overlap_and_gap(spark):
    dim = build_probe_dim(spark, HISTORY)
    broken = dim.withColumn("valid_to", F.when(F.col("version_no") == 1, F.lit(ts("2026-11-04 00:00:00"))).otherwise(F.col("valid_to")))
    failed = {c["check_name"] for c in scd2_checks(broken, "probe_id", "dim_probe") if not c["success"]}
    assert {"dim_probe:scd2_no_overlap", "dim_probe:scd2_contiguous_versions"} <= failed


def test_silver_reconciliation_passes_and_detects_missing_fact(spark, unique_results):
    facts = facts_for(spark, unique_results).cache()
    replies = build_fact_reply(facts)
    bronze = bronze_rows(spark, unique_results)
    assert all(c["success"] for c in silver_reconciliation_checks(bronze, facts.drop("_replies"), replies))
    missing = silver_reconciliation_checks(bronze, facts.drop("_replies").limit(len(unique_results) - 1), replies)
    assert not missing[0]["success"]


def test_gx_flags_critical_failure_and_passes_clean_data(spark, unique_results, tmp_path, monkeypatch):
    monkeypatch.setattr(gx_runner, "GX_ROOT", tmp_path / "gx")
    facts = facts_for(spark, unique_results).drop("_replies").cache()
    clean = gx_runner.run_suite(facts, "test_fact", gx_suites.silver_fact_result(), "test-run", build_docs=False)
    assert not has_critical_failure(clean), [c for c in clean if not c["success"]]

    broken = facts.unionByName(facts.limit(1)).withColumn(            # trùng khóa + rcvd > sent
        "packets_received", F.when(F.col("result_status") == "all_received", F.lit(99)).otherwise(F.col("packets_received")))
    bad = gx_runner.run_suite(broken, "test_fact", gx_suites.silver_fact_result(), "test-run-2", build_docs=False)
    failed = {c["check_name"].split(":")[0] for c in bad if not c["success"] and c["severity"] == "critical"}
    assert {"expect_column_values_to_be_unique", "expect_column_pair_values_a_to_be_greater_than_b"} <= failed
