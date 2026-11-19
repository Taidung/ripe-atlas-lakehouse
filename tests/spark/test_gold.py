import json
import math
import random
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from lakehouse import gold

pytestmark = pytest.mark.spark
METRICS = json.loads((Path(__file__).resolve().parents[2] / "superset" / "metrics.json").read_text(encoding="utf-8"))
H1, H2 = datetime(2026, 11, 19, 2), datetime(2026, 11, 19, 3)  # UTC (session timezone UTC)


def fact(result_id, probe, hour, sent, rcvd, rtts, *, msm=1001, af=4, cc="VN", asn=100, public="public", clock=False, ts_offset=0):
    status = "send_error" if sent == 0 else "all_lost" if rcvd == 0 else "partial_loss" if rcvd < sent else "all_received"
    ts = int(hour.replace(tzinfo=None).timestamp()) + ts_offset if False else int((hour - datetime(1970, 1, 1)).total_seconds()) + ts_offset
    return dict(result_id=result_id, measurement_id=msm, af=af, target="k.root-servers.net", probe_id=probe, event_ts=ts,
                event_time=datetime.utcfromtimestamp(ts), event_hour_utc=hour, ictrl_dt=date(2026, 11, 19),
                packets_sent=sent, packets_received=rcvd, packets_lost=(sent - rcvd) if sent > 0 else None,
                result_status=status, valid_rtt_count=len(rtts), rtt_sum_ms=sum(rtts) if rtts else None,
                rtt_sumsq_ms2=sum(x * x for x in rtts) if rtts else None, rtt_min_ms=min(rtts) if rtts else None,
                rtt_max_ms=max(rtts) if rtts else None, country_code=cc, asn=asn, public_status=public, clock_suspect=clock,
                probe_sk=probe, probe_set_version="x", is_source_corrected=False, silver_loaded_at=datetime(2026, 11, 19, 5))


SCHEMA = ("result_id string, measurement_id int, af int, target string, probe_id long, event_ts long, event_time timestamp, "
          "event_hour_utc timestamp, ictrl_dt date, packets_sent int, packets_received int, packets_lost int, result_status string, "
          "valid_rtt_count int, rtt_sum_ms double, rtt_sumsq_ms2 double, rtt_min_ms double, rtt_max_ms double, country_code string, "
          "asn long, public_status string, clock_suspect boolean, probe_sk long, probe_set_version string, "
          "is_source_corrected boolean, silver_loaded_at timestamp")


def facts_df(spark, rows):
    cols = [c.split()[0] for c in SCHEMA.split(", ")]
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], SCHEMA)


def members_df(spark, probes_v4=(1, 2, 3, 4), probes_v6=(1, 2, 3, 4), version="cohort-a"):
    return gold.membership(spark, [{"version": version, "probes_v4": list(probes_v4), "probes_v6": list(probes_v6)}])


def metric(spark, df, name, group="quality"):
    df.createOrReplaceTempView("m")
    return spark.sql(f"SELECT {METRICS[group][name]['expression']} AS v FROM m").first().v


ROWS = [
    fact("a", 1, H1, 3, 3, [10.0, 20.0, 30.0]),
    fact("b", 2, H1, 1, 0, [], cc="TH", asn=200),          # mất toàn bộ
    fact("c", 3, H1, 3, 2, [40.0, 50.0], cc="TH", asn=200),
    fact("d", 4, H1, 0, 0, []),                            # send_error: không vào mẫu số loss/phản hồi
    fact("e", 1, H2, 3, 3, [100.0, 100.0, 100.0]),
    fact("f", 2, H2, 3, 3, [1.0, 1.0, 1.0], public="not_public"),   # bị loại: không public
    fact("g", 3, H2, 3, 3, [1.0, 1.0, 1.0], clock=True),            # bị loại: đồng hồ lệch
    fact("h", 9, H2, 3, 3, [1.0, 1.0, 1.0]),                        # không thuộc cohort
]


def eligible_of(spark):
    cohort = gold.cohort_results(facts_df(spark, ROWS), members_df(spark))
    return cohort, cohort.filter("is_eligible")


def test_loss_mean_and_response_from_sums_not_average_of_ratios(spark):
    _, eligible = eligible_of(spark)
    h1 = gold.quality_target(eligible).filter(F.col("event_hour_utc") == F.lit(H1))
    row = h1.first()
    assert (row.results_total, row.results_measured, row.results_send_error, row.results_with_response, row.results_all_lost) == (4, 3, 1, 2, 1)
    assert (row.packets_sent, row.packets_lost, row.rtt_count) == (7, 2, 5)
    assert metric(spark, h1, "packet_loss_pct") == pytest.approx(100 * 2 / 7)            # 28.57, không phải TB tỷ lệ 44.4
    assert metric(spark, h1, "response_rate_observed_pct") == pytest.approx(100 * 2 / 3)  # send_error không ở mẫu số
    assert metric(spark, h1, "rtt_mean_ms") == pytest.approx(150 / 5)                     # theo packet, không theo result
    assert metric(spark, h1, "rtt_stddev_ms") == pytest.approx(math.sqrt(sum((x - 30) ** 2 for x in [10, 20, 30, 40, 50]) / 5))


def test_zero_denominators_give_null(spark):
    only_error = gold.quality_target(gold.cohort_results(facts_df(spark, [fact("z", 1, H1, 0, 0, [])]), members_df(spark)))
    assert metric(spark, only_error, "packet_loss_pct") is None
    assert metric(spark, only_error, "response_rate_observed_pct") is None
    assert metric(spark, only_error, "rtt_mean_ms") is None


def test_exclusions_and_rollup_hour_to_day_equals_direct(spark):
    cohort, eligible = eligible_of(spark)
    target = gold.quality_target(eligible)
    assert target.agg(F.sum("results_total")).first()[0] == 5  # a,b,c,d,e (f,g bị loại; h ngoài cohort)
    daily_from_hours = metric(spark, target, "packet_loss_pct")
    direct = eligible.agg((100.0 * F.sum(F.when(F.col("packets_sent") > 0, F.col("packets_sent") - F.col("packets_received")))
                           / F.sum(F.when(F.col("packets_sent") > 0, F.col("packets_sent")))).alias("v")).first().v
    assert daily_from_hours == pytest.approx(direct)
    # Rollup sai (trung bình các tỷ lệ theo giờ) cho kết quả khác: chứng minh cần lưu tử/mẫu số.
    per_hour = [r.v for r in target.groupBy("event_hour_utc").agg((100.0 * F.sum("packets_lost") / F.sum("packets_sent")).alias("v")).collect()]
    assert sum(per_hour) / len(per_hour) != pytest.approx(direct)


def test_marts_are_mutually_consistent(spark):
    cohort, eligible = eligible_of(spark)
    replies = spark.createDataFrame(
        [(r["result_id"], x, True) for r in ROWS for x in ([10.0, 20.0, 30.0] if r["result_id"] == "a" else [])] +
        [("c", 40.0, True), ("c", 50.0, True), ("e", 100.0, True), ("e", 100.0, True), ("e", 100.0, True), ("e", 7.0, False)],
        "result_id string, rtt_ms double, is_valid_rtt boolean")
    marts = [gold.quality_target(eligible), gold.quality_country(eligible), gold.quality_asn(eligible), gold.quality_probe(eligible),
             gold.rtt_histogram(replies, eligible)]
    processed = spark.createDataFrame([], "probe_set_version string, measurement_id int, event_hour_utc timestamp, manifests long, overlap_unchanged_skipped long")
    status = spark.createDataFrame([(date(2026, 11, 19), 1001, "published")], "ictrl_dt date, measurement_id int, silver_status string")
    quarantine = spark.createDataFrame([], "measurement_id int, event_hour_utc timestamp, quarantine_records long")
    cov = gold.coverage(spark, date(2026, 11, 19), cohort, members_df(spark), {1001: 240}, processed, status, quarantine)
    checks = gold.consistency_checks(*marts, cov, gold.paired(eligible))
    assert all(c["success"] for c in checks), [c for c in checks if not c["success"]]
    c = {(r.measurement_id, r.event_hour_utc): r for r in cov.collect()}
    h1 = c[(1001, H1)]
    assert (h1.expected_probes, h1.expected_results, h1.results_total_raw, h1.data_state) == (4, 60, 4, "measured")
    h2 = c[(1001, H2)]
    assert (h2.results_excluded_not_public, h2.results_excluded_clock, h2.results_eligible) == (1, 1, 1)
    assert c[(1004, H1)].data_state == "not_processed" and cov.count() == 24 * 4


def test_histogram_percentile_error_is_small():
    rng = random.Random(42)
    values = [rng.lognormvariate(math.log(40), 0.8) for _ in range(20000)] + [0.0] * 50
    counts = {}
    for v in values:
        b = math.floor(math.log(max(v, gold.BUCKET_BASE_MS) / gold.BUCKET_BASE_MS) / math.log(gold.BUCKET_RATIO))
        counts[b] = counts.get(b, 0) + 1

    def from_hist(p):
        target, acc = p * len(values), 0
        for b in sorted(counts):
            acc += counts[b]
            if acc >= target:
                return gold.BUCKET_BASE_MS * gold.BUCKET_RATIO ** (b + 0.5)

    exact = sorted(values)
    for p in (0.5, 0.95, 0.99):
        e = exact[math.ceil(p * len(exact)) - 1]
        assert abs(from_hist(p) / e - 1) < 0.011, (p, from_hist(p), e)


def test_bucket_function_matches_python(spark):
    df = spark.createDataFrame([(0.0,), (0.05,), (0.1,), (12.345,), (68869.6,)], "rtt double").select("rtt", gold.bucket_id("rtt").alias("b"))
    for r in df.collect():
        assert r.b == math.floor(math.log(max(r.rtt, 0.1) / 0.1) / math.log(1.02))


def test_pairing_mutual_nearest_within_120s_and_unpaired(spark):
    def v(rid, af, offset):
        return fact(rid, 1, H1, 3, 3, [10.0 if af == 4 else 20.0] * 3, msm=1001 if af == 4 else 2001, af=af, ts_offset=offset)
    rows = [v("v4a", 4, 0), v("v6a", 6, 60),            # cặp, dt 60
            v("v4b", 4, 240), v("v6b", 6, 300),         # cặp, dt 60
            v("v4c", 4, 480),                           # v6 gần nhất là 300 (dt 180 > 120) -> không cặp
            v("v4d", 4, 1100), v("v4e", 4, 1150), v("v6c", 6, 1120),   # v6c gần v4d (20) hơn v4e (30) -> chỉ v4d
            v("v6d", 6, 3000)]                          # không có v4 trong 120 s
    eligible = gold.cohort_results(facts_df(spark, rows), members_df(spark)).filter("is_eligible")
    out = gold.paired(eligible).collect()
    assert sum(r.pairs for r in out) == 3
    assert sum(r.unpaired_v4_results for r in out) == 2 and sum(r.unpaired_v6_results for r in out) == 1
    p = gold.paired(eligible)
    assert metric(spark, p, "delta_rtt_v6_minus_v4_ms", "paired") == pytest.approx(10.0)
