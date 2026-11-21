"""Kiểm chứng độc lập Gold so với Silver (SQL viết riêng, không dùng model dbt của Gold), làm bằng chứng nghiệm thu.

  python -m lakehouse crosscheck --date 2026-11-19

1. Tính lại packet loss, tỷ lệ phản hồi, RTT trung bình theo (cohort, measurement, ngày) trực tiếp từ Silver,
   so với giá trị tính từ Gold qua chính biểu thức trong config/metrics.json.
2. Percentile chính xác (nội suy tuyến tính trên mọi reply hợp lệ) so với percentile tính từ histogram Gold.
Cùng quy tắc chọn dữ liệu với Gold: probe public, đồng hồ không lệch, và cohort chỉ được tính cho giờ mà probe set
đó thực sự được ingest (có manifest đã nạp vào Bronze).
Nhóm chỉ có ở một phía, hoặc giá trị chỉ có ở một phía, đều được in là MISMATCH. Thoát 1 nếu có lệch.
"""

from __future__ import annotations

import json

from lakehouse.common import CONFIG_DIR
from lakehouse.trino_db import Trino

TOLERANCE = 1e-9
PERCENTILES = (0.5, 0.95, 0.99)


def fmt(value, spec: str = ".4f") -> str:
    """Định dạng số; None (nhóm chỉ có ở một phía của full outer join) in thành '-'."""
    return "-" if value is None else format(value, spec)


def same(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) < TOLERANCE


def weighted_percentiles(source_sql: str) -> str:
    """Percentile nội suy tuyến tính (như numpy/Spark `percentile`) trên (cohort, measurement_id, v, w):
    w là số lần lặp của giá trị v. Phần tử thứ i (0-based) = v nhỏ nhất có tổng tích lũy > i."""
    ps = ", ".join(str(p) for p in PERCENTILES)
    return f"""
        WITH w AS ({source_sql}),
        c AS (
            SELECT cohort, measurement_id, v,
                   sum(w) OVER (PARTITION BY cohort, measurement_id ORDER BY v ROWS UNBOUNDED PRECEDING) AS cum,
                   sum(w) OVER (PARTITION BY cohort, measurement_id) AS total
            FROM w
        ),
        pos AS (
            SELECT g.cohort, g.measurement_id, g.total, p, p * (g.total - 1) AS k
            FROM (SELECT DISTINCT cohort, measurement_id, total FROM c) g CROSS JOIN UNNEST(ARRAY[{ps}]) AS t(p)
        ),
        bounds AS (
            SELECT pos.cohort, pos.measurement_id, pos.total, pos.p, pos.k,
                   min(CASE WHEN c.cum > floor(pos.k) THEN c.v END) AS v_lo,
                   min(CASE WHEN c.cum > ceil(pos.k) THEN c.v END) AS v_hi
            FROM pos JOIN c ON c.cohort = pos.cohort AND c.measurement_id = pos.measurement_id
            GROUP BY 1, 2, 3, 4, 5
        )
        SELECT cohort, measurement_id, max(total) AS n,
               array_agg(v_lo + (k - floor(k)) * (v_hi - v_lo) ORDER BY p) AS q
        FROM bounds GROUP BY 1, 2
    """


def add_args(parser) -> None:
    parser.add_argument("--date", required=True)
    parser.add_argument("--max-rel-error-percentile", type=float, default=0.02)


def stage_name(args) -> None:
    return None  # lệnh kiểm chứng, không đẩy metric


def run(args, m) -> int:
    from lakehouse.gold import ensure_probe_set_seed

    db = Trino.connect()
    ensure_probe_set_seed(db)
    metrics = json.loads((CONFIG_DIR / "metrics.json").read_text(encoding="utf-8"))
    day = args.date
    cohort = f"""
        JOIN work.probe_set_member m ON m.af = f.af AND m.probe_id = f.probe_id
        JOIN (SELECT DISTINCT probe_set_version AS v, measurement_id, window_start AS event_hour_utc
              FROM bronze.ingest_manifest
              WHERE entity = 'ping_result_raw' AND bronze_status = 'loaded' AND ictrl_dt = DATE '{day}') i
          ON i.v = m.probe_set_version AND i.measurement_id = f.measurement_id AND i.event_hour_utc = f.event_hour_utc
    """
    q = metrics["quality"]
    direct = {
        (r["cohort"], r["measurement_id"]): r
        for r in db.dicts(f"""
            SELECT m.probe_set_version AS cohort, f.measurement_id,
                   100 * CAST(sum(CASE WHEN f.packets_sent > 0 THEN f.packets_sent - f.packets_received END) AS double)
                       / sum(CASE WHEN f.packets_sent > 0 THEN f.packets_sent END) AS loss,
                   100 * CAST(sum(CASE WHEN f.packets_sent > 0 AND f.packets_received > 0 THEN 1 ELSE 0 END) AS double)
                       / sum(CASE WHEN f.packets_sent > 0 THEN 1 ELSE 0 END) AS resp,
                   sum(f.rtt_sum_ms) / sum(f.valid_rtt_count) AS rtt_mean, count(*) AS n
            FROM silver.fact_ping_result f {cohort}
            WHERE f.ictrl_dt = DATE '{day}' AND f.public_status = 'public' AND NOT f.clock_suspect
            GROUP BY 1, 2""")
    }
    gold = {
        (r["cohort"], r["measurement_id"]): r
        for r in db.dicts(f"""
            SELECT probe_set_version AS cohort, measurement_id, {q["packet_loss_pct"]["expression"]} AS loss,
                   {q["response_rate_observed_pct"]["expression"]} AS resp, {q["rtt_mean_ms"]["expression"]} AS rtt_mean,
                   sum(results_total) AS n
            FROM gold.quality_hourly_target WHERE ictrl_dt = DATE '{day}' GROUP BY 1, 2""")
    }
    ok = True
    print("cohort | msm | n silver/gold | loss% silver/gold | resp% silver/gold | rtt_mean silver/gold")
    for key in sorted(set(direct) | set(gold)):
        d, g = direct.get(key, {}), gold.get(key, {})
        pairs = [(d.get(k), g.get(k)) for k in ("loss", "resp", "rtt_mean")]
        good = d.get("n") == g.get("n") and all(same(a, b) for a, b in pairs)
        ok &= good
        values = " | ".join(f"{fmt(a)}/{fmt(b)}" for a, b in pairs)
        print(f"{key[0]} | {key[1]} | {d.get('n')}/{g.get('n')} | {values} | {'OK' if good else 'MISMATCH'}")

    exact = {
        (r["cohort"], r["measurement_id"]): r
        for r in db.dicts(
            weighted_percentiles(f"""
            SELECT m.probe_set_version AS cohort, f.measurement_id, r.rtt_ms AS v, count(*) AS w
            FROM silver.fact_ping_reply r
            JOIN silver.fact_ping_result f ON f.result_id = r.result_id AND f.ictrl_dt = r.ictrl_dt
            {cohort}
            WHERE r.ictrl_dt = DATE '{day}' AND r.is_valid_rtt AND f.public_status = 'public' AND NOT f.clock_suspect
            GROUP BY 1, 2, 3""")
        )
    }
    hist = {
        (r["cohort"], r["measurement_id"]): r
        for r in db.dicts(
            weighted_percentiles(f"""
            SELECT probe_set_version AS cohort, measurement_id, bucket_rep_ms AS v, sum(reply_count) AS w
            FROM gold.rtt_histogram_hourly WHERE ictrl_dt = DATE '{day}' GROUP BY 1, 2, 3""")
        )
    }
    print("\ncohort | msm | replies exact/hist | p50 exact/hist (rel err) | p95 | p99")
    worst = 0.0
    for key in sorted(set(exact) | set(hist)):
        e, h = exact.get(key), hist.get(key)
        if e is None or h is None:  # nhóm chỉ có ở Silver hoặc chỉ có ở Gold
            ok = False
            print(f"{key[0]} | {key[1]} | {e and e['n']}/{h and h['n']} | MISMATCH (chỉ có ở một phía)")
            continue
        errs = [abs(hv / ev - 1) if ev else 0.0 for ev, hv in zip(e["q"], h["q"], strict=True)]
        worst = max(worst, *errs)
        ok &= e["n"] == h["n"]
        cells = " | ".join(
            f"{fmt(ev, '.3f')}/{fmt(hv, '.3f')} ({err * 100:.2f}%)" for ev, hv, err in zip(e["q"], h["q"], errs, strict=True)
        )
        print(f"{key[0]} | {key[1]} | {e['n']}/{h['n']} | {cells}")
    print(f"\nmax relative percentile error = {worst * 100:.2f}% (threshold {args.max_rel_error_percentile * 100:.1f}%)")
    ok &= worst <= args.max_rel_error_percentile
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1
