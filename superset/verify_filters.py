"""Kiểm tra tổ hợp filter / rollup / timezone qua chính API truy vấn của Superset (POST /api/v1/chart/data).

  docker compose exec -T superset /app/.venv/bin/python /app/project/verify_filters.py

Mỗi kiểm tra so hai đường tính độc lập (hai mart khác nhau, hoặc hai time grain) cho cùng một câu hỏi.
"""

from __future__ import annotations

import sys

from provision import Api, DB_NAME, axis

COHORT = "strat-k2-c100-v1"


def ds_id(api: Api, name: str) -> int:
    return api.find("dataset", "table_name", name)["id"]


def run(api: Api, ds: int, metrics: list, columns: list | None = None, filters: list | None = None,
        time_range: str = "No filter", grain: str | None = None) -> list[dict]:
    q = {"columns": columns or [], "metrics": metrics, "filters": filters or [], "row_limit": 100000, "orderby": [],
         "extras": {"time_grain_sqla": grain} if grain else {}, "time_range": time_range,
         "granularity": "event_hour_local"}
    body = {"datasource": {"id": ds, "type": "table"}, "queries": [q], "result_format": "json", "result_type": "full", "form_data": {}}
    res = api.call("POST", "/api/v1/chart/data", json=body)["result"][0]
    if res.get("error"):
        raise RuntimeError(res["error"])
    return res["data"]


def close(a, b, tol=1e-9) -> bool:
    return a is None and b is None or (a is not None and b is not None and abs(a - b) <= tol * max(1.0, abs(a), abs(b)))


def main() -> int:
    api = Api()
    P, T, C, CV, H = (ds_id(api, n) for n in ("quality_hourly_probe", "quality_hourly_target", "quality_hourly_country",
                                              "coverage_hourly", "rtt_histogram_hourly"))
    cohort = {"col": "probe_set_version", "op": "IN", "val": [COHORT]}
    metrics = ["results", "packet_loss_pct", "rtt_mean_ms", "response_rate_observed_pct"]
    checks = []

    def check(name, a, b):
        ok = all(close(a[k], b[k]) for k in a)
        checks.append(ok)
        print(f"{'PASS' if ok else 'FAIL'} {name}: probe-mart={ {k: round(v, 4) if isinstance(v, float) else v for k, v in a.items()} }"
              f" other={ {k: round(v, 4) if isinstance(v, float) else v for k, v in b.items()} }")

    # 1. Lọc theo measurement / af / target: mart probe == mart target
    for flt in ([{"col": "measurement_id", "op": "IN", "val": [2001]}], [{"col": "af", "op": "IN", "val": [6]}],
                [{"col": "target", "op": "IN", "val": ["f.root-servers.net"]}, {"col": "af", "op": "IN", "val": [4]}]):
        a = run(api, P, metrics, filters=[cohort, *flt])[0]
        b = run(api, T, metrics, filters=[cohort, *flt])[0]
        check(f"filter {[(f['col'], f['val']) for f in flt]} probe vs target mart", a, b)

    # 2. Lọc quốc gia: mart probe == mart country
    for cc in ("VN", "US", "ZZ"):
        f = [cohort, {"col": "country_code", "op": "IN", "val": [cc]}]
        check(f"country={cc} probe vs country mart", run(api, P, metrics, filters=f)[0], run(api, C, metrics, filters=f)[0])

    # 3. Rollup: tổng theo grain ngày == tổng theo grain giờ (tính lại từ tử/mẫu số), cho từng af
    for af in (4, 6):
        f = [cohort, {"col": "af", "op": "IN", "val": [af]}]
        day = run(api, P, metrics, columns=[axis("event_hour_local", "P1D")], filters=f, grain="P1D")
        hours = run(api, P, ["results", "packet_loss_pct"], columns=[axis("event_hour_local", "PT1H")], filters=f, grain="PT1H")
        total = run(api, P, metrics, filters=f)[0]
        ok = len(day) == 1 and close(day[0]["packet_loss_pct"], total["packet_loss_pct"]) and day[0]["results"] == sum(h["results"] for h in hours)
        naive = sum(h["packet_loss_pct"] for h in hours if h["packet_loss_pct"] is not None) / max(1, len(hours))
        checks.append(ok)
        print(f"{'PASS' if ok else 'FAIL'} rollup af={af}: day loss={day[0]['packet_loss_pct']:.4f} = total {total['packet_loss_pct']:.4f}; "
              f"{len(hours)} giờ, TB tỷ lệ theo giờ (cách SAI) = {naive:.4f}")

    # 4. Timezone: khoảng giờ VN 09:00-10:00 == giờ UTC 02:00 trong coverage (results_eligible)
    vn = run(api, P, ["results"], filters=[cohort], time_range="2026-11-19T09:00:00 : 2026-11-19T10:00:00")[0]["results"]
    cov = run(api, CV, ["results_excluded_clock"], columns=["event_hour_utc"], filters=[cohort])
    cov_09 = run(api, CV, [{"expressionType": "SQL", "sqlExpression": "SUM(results_eligible)", "label": "eligible"}],
                 filters=[cohort], time_range="2026-11-19T09:00:00 : 2026-11-19T10:00:00")[0]["eligible"]
    ok = vn == cov_09 and vn > 0
    checks.append(ok)
    print(f"{'PASS' if ok else 'FAIL'} timezone: results giờ VN 09:00 (probe mart) = {vn}, coverage eligible cùng giờ = {cov_09}")

    # 5. p95 với filter quốc gia + af đi qua mart histogram
    p95 = run(api, H, ["rtt_p95_ms", "rtt_samples"], filters=[cohort, {"col": "country_code", "op": "IN", "val": ["VN"]},
                                                              {"col": "af", "op": "IN", "val": [4]}])[0]
    rtt_n = run(api, P, [{"expressionType": "SQL", "sqlExpression": "SUM(rtt_count)", "label": "n"}],
                filters=[cohort, {"col": "country_code", "op": "IN", "val": ["VN"]}, {"col": "af", "op": "IN", "val": [4]}])[0]["n"]
    ok = p95["rtt_samples"] == rtt_n and p95["rtt_p95_ms"] is not None
    checks.append(ok)
    print(f"{'PASS' if ok else 'FAIL'} p95 VN af=4: {p95['rtt_p95_ms']:.2f} ms trên {p95['rtt_samples']} reply (= rtt_count {rtt_n})")

    print(f"\n{sum(checks)}/{len(checks)} checks passed")
    return 0 if all(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
