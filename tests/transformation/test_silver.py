"""Silver: SCD2, chuẩn hóa result thật, timezone, chọn bản ghi khi nguồn sửa, as-of join, reply, dim_date/time."""

import hashlib
import json
from datetime import date

import pytest

from lakehouse.silver import build_fact_reply, dim_date, dim_time, select_result_versions
from tests.spark_support import bronze_rows, build_probe_dim, facts_for, ts

pytestmark = pytest.mark.spark

HISTORY = [
    (7, "2026-11-01 00:00:00", {"country_code": "VN", "asn_v4": 100}),
    (7, "2026-11-02 00:00:00", {"country_code": "VN", "asn_v4": 100}),   # giống hệt -> không version mới
    (7, "2026-11-03 00:00:00", {"country_code": "VN", "asn_v4": 200}),   # đổi ASN
    (7, "2026-11-04 00:00:00", None),                                    # not_returned -> bỏ qua
    (7, "2026-11-05 00:00:00", {"country_code": "VN", "asn_v4": 100}),   # quay lại giá trị cũ -> version 3
]


# ---------------------------------------------------------------------------- SCD2
def test_scd2_versions_identical_snapshots_and_revert(spark):
    dim = build_probe_dim(spark, HISTORY).filter("probe_id = 7").orderBy("version_no").collect()
    assert [(r.version_no, r.asn_v4, r.observation_count) for r in dim] == [(1, 100, 2), (2, 200, 1), (3, 100, 1)]
    assert [r.valid_from for r in dim] == [ts("2026-11-01 00:00:00"), ts("2026-11-03 00:00:00"), ts("2026-11-05 00:00:00")]
    assert [r.valid_to for r in dim][:2] == [ts("2026-11-03 00:00:00"), ts("2026-11-05 00:00:00")]
    assert [r.is_current for r in dim] == [False, False, True]
    assert dim[1].prev_version_last_observed_at == ts("2026-11-02 00:00:00")  # khoảng thay đổi (02, 03]
    assert dim[0].join_from == ts("1970-01-01 00:00:00") and dim[1].join_from == dim[1].valid_from


def test_scd2_surrogate_keys_stable_on_rebuild(spark):
    a = build_probe_dim(spark, HISTORY)
    b = build_probe_dim(spark, list(reversed(HISTORY)))
    assert sorted(r.probe_sk for r in a.collect()) == sorted(r.probe_sk for r in b.collect())


# ---------------------------------------------------------------------------- fact từ result thật
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
    before = dict(base, timestamp=1795021199)                             # 2026-11-18 16:59:59 UTC = 23:59:59 giờ VN
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


def test_replies_one_row_per_element_and_duplicates_not_valid(spark, unique_results):
    replies = build_fact_reply(facts_for(spark, unique_results)).collect()
    assert len({(r.result_id, r.reply_seq) for r in replies}) == sum(len(r["result"]) for r in unique_results)
    duplicates = [r for r in replies if r.reply_type == "duplicate"]
    assert duplicates and not any(r.is_valid_rtt for r in duplicates)


def test_dim_date_and_time(spark):
    d = {r.date_key: r for r in dim_date(spark, "2026-11-16", "2026-11-22").collect()}
    assert (d[20261116].day_of_week, d[20261116].is_weekend, d[20261122].day_of_week, d[20261122].is_weekend) == (1, False, 7, True)
    t = dim_time(spark)
    assert t.count() == 1440 and t.filter("time_key = 2359").first().hour == 23
