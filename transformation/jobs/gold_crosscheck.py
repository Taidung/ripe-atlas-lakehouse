"""Kiểm chứng độc lập Gold so với Silver (không dùng code lakehouse.gold), dùng làm bằng chứng nghiệm thu.

  spark-submit /opt/app/transformation/jobs/gold_crosscheck.py --date 2026-11-19

1. Tính lại packet loss, tỷ lệ phản hồi, RTT trung bình theo (cohort, measurement, ngày) trực tiếp từ Silver
   bằng SQL viết riêng, so với giá trị tính từ Gold qua chính biểu thức trong config/metrics.json.
2. Percentile chính xác (Spark percentile, trên mọi reply hợp lệ) so với percentile tính từ histogram Gold.
Thoát 1 nếu lệch vượt ngưỡng.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

METRICS = json.loads(Path("/opt/app/config/metrics.json").read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--max-rel-error-percentile", type=float, default=0.02)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("crosscheck_gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    day = args.date

    rows = [(ps["version"], af, pid) for p in sorted(Path("/opt/app/config/probe_sets").glob("*.json"))
            for ps in [json.loads(p.read_text(encoding="utf-8"))] for af, k in ((4, "probes_v4"), (6, "probes_v6")) for pid in ps[k]]
    spark.createDataFrame(rows, "v string, af int, probe_id long").createOrReplaceTempView("members")

    q = METRICS["quality"]
    direct = spark.sql(f"""
        SELECT m.v AS cohort, f.measurement_id,
               100 * CAST(SUM(CASE WHEN f.packets_sent > 0 THEN f.packets_sent - f.packets_received END) AS DOUBLE)
                   / SUM(CASE WHEN f.packets_sent > 0 THEN f.packets_sent END) AS loss,
               100 * CAST(SUM(CASE WHEN f.packets_sent > 0 AND f.packets_received > 0 THEN 1 ELSE 0 END) AS DOUBLE)
                   / SUM(CASE WHEN f.packets_sent > 0 THEN 1 ELSE 0 END) AS resp,
               SUM(f.rtt_sum_ms) / SUM(f.valid_rtt_count) AS rtt_mean, COUNT(*) AS n
        FROM nessie.silver.fact_ping_result f JOIN members m ON m.af = f.af AND m.probe_id = f.probe_id
        WHERE f.ictrl_dt = DATE'{day}' AND f.public_status = 'public' AND NOT f.clock_suspect
        GROUP BY 1, 2""")
    from_gold = spark.sql(f"""
        SELECT probe_set_version AS cohort, measurement_id, {q['packet_loss_pct']['expression']} AS loss,
               {q['response_rate_observed_pct']['expression']} AS resp, {q['rtt_mean_ms']['expression']} AS rtt_mean,
               SUM(results_total) AS n
        FROM nessie.gold.quality_hourly_target WHERE ictrl_dt = DATE'{day}' GROUP BY 1, 2""")
    ok = True
    print("cohort | msm | n silver/gold | loss% silver/gold | resp% silver/gold | rtt_mean silver/gold")
    for r in sorted(direct.join(from_gold.withColumnRenamed("loss", "g_loss").withColumnRenamed("resp", "g_resp")
                                .withColumnRenamed("rtt_mean", "g_rtt").withColumnRenamed("n", "g_n"),
                                ["cohort", "measurement_id"], "full_outer").collect(), key=lambda r: (r.cohort, r.measurement_id)):
        good = r.n == r.g_n and all(abs((a or 0) - (b or 0)) < 1e-9 for a, b in ((r.loss, r.g_loss), (r.resp, r.g_resp), (r.rtt_mean, r.g_rtt)))
        ok &= good
        print(f"{r.cohort} | {r.measurement_id} | {r.n}/{r.g_n} | {r.loss:.4f}/{r.g_loss:.4f} | {r.resp:.4f}/{r.g_resp:.4f} | "
              f"{r.rtt_mean:.4f}/{r.g_rtt:.4f} | {'OK' if good else 'MISMATCH'}")

    exact = spark.sql(f"""
        SELECT m.v AS cohort, f.measurement_id, percentile(r.rtt_ms, array(0.5, 0.95, 0.99)) AS e, COUNT(*) AS n
        FROM nessie.silver.fact_ping_reply r JOIN nessie.silver.fact_ping_result f ON f.result_id = r.result_id AND f.ictrl_dt = r.ictrl_dt
        JOIN members m ON m.af = f.af AND m.probe_id = f.probe_id
        WHERE r.ictrl_dt = DATE'{day}' AND r.is_valid_rtt AND f.public_status = 'public' AND NOT f.clock_suspect
        GROUP BY 1, 2""")
    hist = spark.sql(f"""
        SELECT probe_set_version AS cohort, measurement_id, percentile(bucket_rep_ms, array(0.5, 0.95, 0.99), reply_count) AS h,
               SUM(reply_count) AS hn
        FROM nessie.gold.rtt_histogram_hourly WHERE ictrl_dt = DATE'{day}' GROUP BY 1, 2""")
    print("\ncohort | msm | replies exact/hist | p50 exact/hist (rel err) | p95 | p99")
    worst = 0.0
    for r in sorted(exact.join(hist, ["cohort", "measurement_id"]).collect(), key=lambda r: (r.cohort, r.measurement_id)):
        errs = [abs(h / e - 1) if e else 0.0 for e, h in zip(r.e, r.h, strict=True)]
        worst = max(worst, *errs)
        ok &= r.n == r.hn
        print(f"{r.cohort} | {r.measurement_id} | {r.n}/{r.hn} | " + " | ".join(
            f"{e:.3f}/{h:.3f} ({err * 100:.2f}%)" for e, h, err in zip(r.e, r.h, errs, strict=True)))
    print(f"\nmax relative percentile error = {worst * 100:.2f}% (threshold {args.max_rel_error_percentile * 100:.1f}%)")
    ok &= worst <= args.max_rel_error_percentile
    print("PASS" if ok else "FAIL")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
