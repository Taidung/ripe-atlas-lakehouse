"""Kiểm tra tổ hợp filter / rollup / timezone qua chính API truy vấn của Superset (POST /api/v1/chart/data).

  docker compose exec -T -w /app/project superset /app/.venv/bin/python verify_filters.py --day 2026-11-21 [--cohort dev-200-v2]

`--day` là một ngày (giờ VN) đã có Gold; mọi kiểm tra chỉ xét ngày đó, vì phép rollup "ngày = tổng các giờ" cần
đúng một ngày. Không có dữ liệu cho (cohort, ngày) thì dừng với mã 2 thay vì so None với None.

Mỗi kiểm tra so hai đường tính độc lập (hai mart khác nhau, hoặc hai time grain) cho cùng một câu hỏi.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone  # Superset chạy Python 3.10: chưa có datetime.UTC

from provision import Api, axis


def ds_id(api: Api, name: str) -> int:
    return api.find("dataset", "table_name", name)["id"]


def run(
    api: Api,
    ds: int,
    metrics: list,
    columns: list | None = None,
    filters: list | None = None,
    time_range: str = "No filter",
    grain: str | None = None,
) -> list[dict]:
    q = {
        "columns": columns or [],
        "metrics": metrics,
        "filters": filters or [],
        "row_limit": 100000,
        "orderby": [],
        "extras": {"time_grain_sqla": grain} if grain else {},
        "time_range": time_range,
        "granularity": "event_hour_local",
    }
    body = {"datasource": {"id": ds, "type": "table"}, "queries": [q], "result_format": "json", "result_type": "full", "form_data": {}}
    res = api.call("POST", "/api/v1/chart/data", json=body)["result"][0]
    if res.get("error"):
        raise RuntimeError(res["error"])
    return res["data"]


def close(a, b, tol=1e-9) -> bool:
    return a is None and b is None or (a is not None and b is not None and abs(a - b) <= tol * max(1.0, abs(a), abs(b)))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--day", type=date.fromisoformat, required=True, help="ngày theo giờ VN, ví dụ 2026-11-21")
    parser.add_argument("--cohort", default="dev-200-v2", help="probe_set_version (mặc định dev-200-v2)")
    args = parser.parse_args()
    day_range = f"{args.day}T00:00:00 : {args.day + timedelta(days=1)}T00:00:00"
    api = Api()
    P, T, C, CV, H = (
        ds_id(api, n)
        for n in ("quality_hourly_probe", "quality_hourly_target", "quality_hourly_country", "coverage_hourly", "rtt_histogram_hourly")
    )
    cohort = {"col": "probe_set_version", "op": "IN", "val": [args.cohort]}
    metrics = ["results", "packet_loss_pct", "rtt_mean_ms", "response_rate_observed_pct"]

    def run_day(ds, metrics, **kw):
        return run(api, ds, metrics, time_range=day_range, **kw)

    if not run_day(P, ["results"], filters=[cohort])[0]["results"]:
        print(f"Không có dữ liệu Gold cho cohort={args.cohort} ngày {args.day} (giờ VN); chọn --day/--cohort khác.")
        return 2
    checks = []

    def check(name, a, b):
        ok = all(close(a[k], b[k]) for k in a)
        checks.append(ok)
        print(
            f"{'PASS' if ok else 'FAIL'} {name}: probe-mart={ {k: round(v, 4) if isinstance(v, float) else v for k, v in a.items()} }"
            f" other={ {k: round(v, 4) if isinstance(v, float) else v for k, v in b.items()} }"
        )

    # 1. Lọc theo measurement / af / target: mart probe == mart target
    for flt in (
        [{"col": "measurement_id", "op": "IN", "val": [2001]}],
        [{"col": "af", "op": "IN", "val": [6]}],
        [{"col": "target", "op": "IN", "val": ["f.root-servers.net"]}, {"col": "af", "op": "IN", "val": [4]}],
    ):
        a = run_day(P, metrics, filters=[cohort, *flt])[0]
        b = run_day(T, metrics, filters=[cohort, *flt])[0]
        check(f"filter {[(f['col'], f['val']) for f in flt]} probe vs target mart", a, b)

    # 2. Lọc quốc gia: mart probe == mart country
    for cc in ("VN", "US", "ZZ"):
        f = [cohort, {"col": "country_code", "op": "IN", "val": [cc]}]
        check(f"country={cc} probe vs country mart", run_day(P, metrics, filters=f)[0], run_day(C, metrics, filters=f)[0])

    # 3. Rollup: tổng theo grain ngày == tổng theo grain giờ (tính lại từ tử/mẫu số), cho từng af
    for af in (4, 6):
        f = [cohort, {"col": "af", "op": "IN", "val": [af]}]
        day = run_day(P, metrics, columns=[axis("event_hour_local", "P1D")], filters=f, grain="P1D")
        hours = run_day(P, ["results", "packet_loss_pct"], columns=[axis("event_hour_local", "PT1H")], filters=f, grain="PT1H")
        total = run_day(P, metrics, filters=f)[0]
        ok = (
            len(day) == 1
            and close(day[0]["packet_loss_pct"], total["packet_loss_pct"])
            and day[0]["results"] == sum(h["results"] for h in hours)
        )
        naive = sum(h["packet_loss_pct"] for h in hours if h["packet_loss_pct"] is not None) / max(1, len(hours))
        checks.append(ok)
        if not day:
            print(f"FAIL rollup af={af}: không có dòng theo ngày")
            continue
        print(
            f"{'PASS' if ok else 'FAIL'} rollup af={af}: day loss={day[0]['packet_loss_pct']:.4f} = total {total['packet_loss_pct']:.4f}; "
            f"{len(hours)} giờ, TB tỷ lệ theo giờ (cách SAI) = {naive:.4f}"
        )

    # 4. Timezone: một giờ VN (giờ sớm nhất có dữ liệu trong ngày) lọc trên mart probe == coverage (results_eligible)
    hour_rows = run_day(P, ["results"], columns=[axis("event_hour_local", "PT1H")], filters=[cohort], grain="PT1H")
    first = datetime.fromtimestamp(min(r["event_hour_local"] for r in hour_rows) / 1000, timezone.utc).replace(tzinfo=None)  # noqa: UP017
    hour_range = f"{first:%Y-%m-%dT%H:%M:%S} : {first + timedelta(hours=1):%Y-%m-%dT%H:%M:%S}"
    vn = run(api, P, ["results"], filters=[cohort], time_range=hour_range)[0]["results"]
    cov = run(
        api,
        CV,
        [{"expressionType": "SQL", "sqlExpression": "SUM(results_eligible)", "label": "eligible"}],
        filters=[cohort],
        time_range=hour_range,
    )[0]["eligible"]
    ok = vn == cov and vn > 0
    checks.append(ok)
    print(f"{'PASS' if ok else 'FAIL'} timezone: results giờ VN {first:%H:00} (probe mart) = {vn}, coverage eligible cùng giờ = {cov}")

    # 5. p95 với filter quốc gia + af đi qua mart histogram
    p95 = run_day(
        H,
        ["rtt_p95_ms", "rtt_samples"],
        filters=[cohort, {"col": "country_code", "op": "IN", "val": ["VN"]}, {"col": "af", "op": "IN", "val": [4]}],
    )[0]
    rtt_n = run_day(
        P,
        [{"expressionType": "SQL", "sqlExpression": "SUM(rtt_count)", "label": "n"}],
        filters=[cohort, {"col": "country_code", "op": "IN", "val": ["VN"]}, {"col": "af", "op": "IN", "val": [4]}],
    )[0]["n"]
    ok = p95["rtt_samples"] == rtt_n and p95["rtt_p95_ms"] is not None
    checks.append(ok)
    print(f"{'PASS' if ok else 'FAIL'} p95 VN af=4: {p95['rtt_p95_ms']:.2f} ms trên {p95['rtt_samples']} reply (= rtt_count {rtt_n})")

    print(f"\n{sum(checks)}/{len(checks)} checks passed")
    return 0 if all(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
