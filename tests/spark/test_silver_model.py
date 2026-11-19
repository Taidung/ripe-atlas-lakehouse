import hashlib
import json
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from lakehouse import quality
from lakehouse.silver import (
    PROBE_TRACKED,
    asof_join,
    build_fact_reply,
    build_fact_result,
    build_scd2,
    dim_date,
    dim_time,
    probe_snapshots,
    select_result_versions,
    with_unknown_member,
)

pytestmark = pytest.mark.spark
LOADED = datetime(2026, 11, 19)


def ts(s):
    return datetime.fromisoformat(s)


# ---------------------------------------------------------------------------- SCD2
def probe_raw(spark, rows):
    """rows: (probe_id, observed_at, attrs dict | None)."""
    data = []
    for pid, observed, attrs in rows:
        if attrs is None:
            data.append((pid, ts(observed), "not_returned", None))
        else:
            doc = {"id": pid, "is_public": True, "is_anchor": False, "asn_v6": None, "prefix_v4": None, "prefix_v6": None,
                   "geometry": {"coordinates": [106.7, 10.8]}, **attrs}
            data.append((pid, ts(observed), "ok", json.dumps(doc, sort_keys=True)))
    return spark.createDataFrame(data, "probe_id long, observed_at timestamp, fetch_status string, raw_json string")


def build_probe_dim(spark, rows):
    return with_unknown_member(spark, build_scd2(probe_snapshots(probe_raw(spark, rows)), "probe_id", PROBE_TRACKED, "probe", "probe_sk"),
                               "probe_id", "probe_sk")


HISTORY = [
    (7, "2026-11-01 00:00:00", {"country_code": "VN", "asn_v4": 100}),
    (7, "2026-11-02 00:00:00", {"country_code": "VN", "asn_v4": 100}),   # giống hệt -> không version mới
    (7, "2026-11-03 00:00:00", {"country_code": "VN", "asn_v4": 200}),   # đổi ASN
    (7, "2026-11-04 00:00:00", None),                                    # not_returned -> bỏ qua
    (7, "2026-11-05 00:00:00", {"country_code": "VN", "asn_v4": 100}),   # quay lại giá trị cũ -> version 3
]


def test_scd2_versions_identical_snapshots_and_revert(spark):
    dim = build_probe_dim(spark, HISTORY).filter("probe_id = 7").orderBy("version_no").collect()
    assert [(r.version_no, r.asn_v4, r.observation_count) for r in dim] == [(1, 100, 2), (2, 200, 1), (3, 100, 1)]
    assert [r.valid_from for r in dim] == [ts("2026-11-01 00:00:00"), ts("2026-11-03 00:00:00"), ts("2026-11-05 00:00:00")]
    assert [r.valid_to for r in dim][:2] == [ts("2026-11-03 00:00:00"), ts("2026-11-05 00:00:00")]
    assert [r.is_current for r in dim] == [False, False, True]
    assert dim[1].prev_version_last_observed_at == ts("2026-11-02 00:00:00")  # khoảng thay đổi (02, 03]
    assert dim[0].join_from == ts("1970-01-01 00:00:00") and dim[1].join_from == dim[1].valid_from


def test_scd2_surrogate_keys_stable_on_rebuild_and_checks_pass(spark):
    a = build_probe_dim(spark, HISTORY)
    b = build_probe_dim(spark, list(reversed(HISTORY)))
    assert sorted(r.probe_sk for r in a.collect()) == sorted(r.probe_sk for r in b.collect())
    assert all(c["success"] for c in quality.scd2_checks(a, "probe_id", "dim_probe"))


def test_scd2_checks_detect_overlap(spark):
    dim = build_probe_dim(spark, HISTORY)
    broken = dim.withColumn("valid_to", F.when(F.col("version_no") == 1, F.lit(ts("2026-11-04 00:00:00"))).otherwise(F.col("valid_to")))
    failed = {c["check_name"] for c in quality.scd2_checks(broken, "probe_id", "dim_probe") if not c["success"]}
    assert "dim_probe:scd2_no_overlap" in failed and "dim_probe:scd2_contiguous_versions" in failed


# ---------------------------------------------------------------------------- facts from real fixtures
def bronze_rows(spark, records, fetched="2026-11-18 14:05:00"):
    data = []
    for r in records:
        raw = json.dumps(r, separators=(",", ":"))
        sha = hashlib.sha256(raw.encode()).hexdigest()
        data.append((r["msm_id"], r["prb_id"], r["timestamp"], datetime.utcfromtimestamp(r["timestamp"]), r.get("stored_timestamp"),
                     raw, sha, ts(fetched), f"uid-{sha[:8]}", "run-1", "dev-200-v1"))
    return spark.createDataFrame(data, "measurement_id int, probe_id long, event_ts long, event_time timestamp, stored_ts long, "
                                       "raw_json string, raw_sha256 string, fetched_at timestamp, record_uid string, "
                                       "ingest_run_id string, probe_set_version string")


def facts_for(spark, records, probe_rows=None):
    probe_rows = probe_rows or [(r["prb_id"], "2026-11-01 00:00:00", {"country_code": "VN", "asn_v4": 1, "asn_v6": 2}) for r in records]
    pdim = build_probe_dim(spark, probe_rows)
    mdim = spark.createDataFrame([(m, -m, ts("1970-01-01 00:00:00"), ts("9999-12-31 00:00:00"), False) for m in (1001, 1004, 2001, 2004)],
                                 "measurement_id long, measurement_sk long, join_from timestamp, valid_to timestamp, is_unknown_member boolean")
    facts = asof_join(build_fact_result(select_result_versions(bronze_rows(spark, records)), "silver-1", LOADED), pdim, mdim)
    return facts


def test_normalization_on_real_cases(spark, real_cases):
    facts = {r.probe_id: r for r in facts_for(spark, [real_cases[k] for k in (
        "normal_v4", "partial_loss_v4", "all_loss_timeout_v4", "send_error_sent0_v4", "duplicate_reply_v4", "dup_only_no_valid_v4",
    )]).collect()}
    normal = facts[real_cases["normal_v4"]["prb_id"]]
    assert (normal.result_status, normal.valid_rtt_count, normal.packets_lost) == ("all_received", 3, 0)
    assert normal.rtt_sum_ms == pytest.approx(sum(e["rtt"] for e in real_cases["normal_v4"]["result"]))
    assert normal.rtt_avg_ms == pytest.approx(real_cases["normal_v4"]["avg"])

    partial = facts[real_cases["partial_loss_v4"]["prb_id"]]
    assert (partial.result_status, partial.valid_rtt_count, partial.timeout_count, partial.packets_lost) == ("partial_loss", 2, 1, 1)

    lost = facts[real_cases["all_loss_timeout_v4"]["prb_id"]]
    assert (lost.result_status, lost.rtt_avg_ms, lost.src_rtt_avg, lost.rtt_sum_ms, lost.has_response) == ("all_lost", None, -1.0, None, False)

    err = facts[real_cases["send_error_sent0_v4"]["prb_id"]]
    assert (err.result_status, err.packets_lost, err.error_count) == ("send_error", None, 3)

    dup_only = facts[real_cases["dup_only_no_valid_v4"]["prb_id"]]
    assert (dup_only.result_status, dup_only.duplicate_reply_count, dup_only.valid_rtt_count, dup_only.rtt_avg_ms) == ("all_lost", 1, 0, None)
    assert all(r.counts_consistent for r in facts.values())
    assert all(r.clock_suspect is False for r in facts.values() if r.lts is not None and r.lts >= 0)


def test_result_id_canonical_and_timezone_boundary(spark, unique_results):
    base = next(r for r in unique_results if r["msm_id"] == 1001)
    before = dict(base, timestamp=1795021199)  # 2026-11-18 16:59:59 UTC = 23:59:59 giờ VN
    after = dict(base, timestamp=1795021200, prb_id=base["prb_id"] + 1)  # 17:00:00 UTC = 00:00 ngày 19 giờ VN
    rows = {r.event_ts: r for r in facts_for(spark, [before, after]).collect()}
    assert (rows[1795021199].ictrl_dt, rows[1795021199].date_key, rows[1795021199].time_key) == (date(2026, 11, 18), 20261118, 2359)
    assert (rows[1795021200].ictrl_dt, rows[1795021200].date_key, rows[1795021200].time_key) == (date(2026, 11, 19), 20261119, 0)
    expected = hashlib.sha256(f"ping_result|1001|{base['prb_id']}|1795021199".encode()).hexdigest()
    assert rows[1795021199].result_id == expected


def test_source_correction_picks_latest_fetch(spark, unique_results):
    base = next(r for r in unique_results if r["msm_id"] == 1001 and r["rcvd"] == 3)
    corrected = dict(base, avg=999.0)
    bronze = bronze_rows(spark, [base]).unionByName(bronze_rows(spark, [corrected], fetched="2026-11-18 16:00:00"))
    picked = select_result_versions(bronze).collect()
    assert len(picked) == 1 and json.loads(picked[0].raw_json)["avg"] == 999.0 and picked[0].source_version_count == 2


def test_asof_join_boundaries_unknown_and_no_duplication(spark, unique_results):
    base = next(r for r in unique_results if r["msm_id"] == 1001)
    pid = base["prb_id"]
    history = [(pid, "2026-11-18 13:00:00", {"country_code": "VN", "asn_v4": 100}),
               (pid, "2026-11-18 13:30:00", {"country_code": "VN", "asn_v4": 200})]
    events = [dict(base, timestamp=t) for t in (1795006799, 1795006800, 1795008599, 1795008600)]  # 12:59:59, 13:00, 13:29:59, 13:30
    stranger = dict(base, prb_id=99999999)
    facts = facts_for(spark, events + [stranger], probe_rows=history)
    rows = {(r.probe_id, r.event_ts): r for r in facts.collect()}
    assert facts.count() == 5
    assert [rows[(pid, t)].asn for t in (1795006799, 1795006800, 1795008599, 1795008600)] == [100, 100, 100, 200]
    assert rows[(pid, 1795006799)].metadata_basis == "assumed_from_first_snapshot"
    assert rows[(pid, 1795008600)].metadata_basis == "asof_snapshot"
    unknown = rows[(99999999, base["timestamp"])]
    assert (unknown.probe_sk, unknown.public_status, unknown.metadata_basis, unknown.country_code) == (-1, "unknown", "no_metadata", None)


def test_replies_and_reconciliation(spark, unique_results):
    facts = facts_for(spark, unique_results).cache()
    replies = build_fact_reply(facts)
    types = {(r.result_id, r.reply_seq): r for r in replies.collect()}
    assert len(types) == sum(len(r["result"]) for r in unique_results)
    dup = [r for r in types.values() if r.reply_type == "duplicate"]
    assert dup and not any(r.is_valid_rtt for r in dup)
    bronze = bronze_rows(spark, unique_results)
    checks = quality.reconciliation_checks(bronze, facts.drop("_replies"), replies)
    assert all(c["success"] for c in checks), checks
    missing = quality.reconciliation_checks(bronze, facts.drop("_replies").limit(len(unique_results) - 1), replies)
    assert not missing[0]["success"]


def test_dim_date_and_time(spark):
    d = {r.date_key: r for r in dim_date(spark, "2026-11-16", "2026-11-22").collect()}
    assert (d[20261116].day_of_week, d[20261116].is_weekend, d[20261122].day_of_week, d[20261122].is_weekend) == (1, False, 7, True)
    t = dim_time(spark)
    assert t.count() == 1440 and t.filter("time_key = 2359").first().hour == 23


def test_gx_flags_critical_failure_and_passes_clean_data(spark, unique_results, tmp_path, monkeypatch):
    monkeypatch.setattr(quality, "GX_ROOT", tmp_path / "gx")
    facts = facts_for(spark, unique_results).drop("_replies").cache()
    clean = quality.run_gx(facts, "test_fact", quality.fact_result_expectations(), "test-run", build_docs=False)
    assert not quality.has_critical_failure(clean), [c for c in clean if not c["success"]]
    broken = facts.unionByName(facts.limit(1)).withColumn(
        "packets_received", F.when(F.col("result_status") == "all_received", F.lit(99)).otherwise(F.col("packets_received")))
    bad = quality.run_gx(broken, "test_fact", quality.fact_result_expectations(), "test-run-2", build_docs=False)
    failed = {c["check_name"].split(":")[0] for c in bad if not c["success"] and c["severity"] == "critical"}
    assert {"expect_column_values_to_be_unique", "expect_column_pair_values_a_to_be_greater_than_b"} <= failed
