"""Dựng dashboard "Global Network Quality" trên Superset bằng REST API (idempotent: tạo hoặc cập nhật theo tên).

Chạy trong container superset:
  docker compose exec -T superset /app/.venv/bin/python /app/project/provision.py

Các bước:
  1. Dataset vật lý cho các Gold mart + dataset ảo (datasets/*.sql), gắn saved metric từ config/metrics.json.
  2. Chart kèm query_context; mỗi chart dùng API mới được chạy thử qua POST /api/v1/chart/data.
  3. Dashboard (position_json + native filters). Phạm vi filter tính từ cột thật của dataset.
  4. Xuất ZIP (export/global_network_quality.zip) để import ở môi trường khác.
Mã thoát 1 nếu có chart chạy truy vấn lỗi.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")
PROJECT = Path(__file__).resolve().parent
# Định nghĩa metric dùng chung (config/metrics.json, mount vào /app/config trong container).
METRICS_FILE = Path(os.environ.get("METRICS_FILE", "/app/config/metrics.json"))
METRICS = json.loads(METRICS_FILE.read_text(encoding="utf-8"))
DB_NAME = "RIPE Atlas Lakehouse (Trino)"
SCHEMA = "gold"
SLUG = "global-network-quality"
DEFAULT_COHORT = "strat-k2-c100-v1"
TIME_COL = "event_hour_local"


# ---------------------------------------------------------------------------- API helpers
def rison(value) -> str:
    if isinstance(value, dict):
        return "(" + ",".join(f"{k}:{rison(v)}" for k, v in value.items()) + ")"
    if isinstance(value, list):
        return "!(" + ",".join(rison(v) for v in value) + ")"
    if isinstance(value, bool):
        return "!t" if value else "!f"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value)
    return (
        s
        if s and all(c.isalnum() or c in "-_./~" for c in s) and not s[0].isdigit()
        else "'" + s.replace("!", "!!").replace("'", "!'") + "'"
    )


class Api:
    def __init__(self):
        self.s = requests.Session()
        r = self.s.post(
            f"{BASE}/api/v1/security/login",
            json={
                "username": os.environ.get("SUPERSET_ADMIN_USER", "admin"),
                "password": os.environ["SUPERSET_ADMIN_PASSWORD"],
                "provider": "db",
                "refresh": False,
            },
        )
        r.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        csrf = self.s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]
        self.s.headers.update({"X-CSRFToken": csrf, "Referer": BASE})

    def call(self, method: str, path: str, **kw):
        r = self.s.request(method, f"{BASE}{path}", **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:800]}")
        return r.json() if r.content and r.headers.get("content-type", "").startswith("application/json") else r

    def find(self, resource: str, col: str, value: str) -> dict | None:
        q = rison({"filters": [{"col": col, "opr": "eq", "value": value}], "page_size": 5})
        res = self.call("GET", f"/api/v1/{resource}/?q={q}")["result"]
        return res[0] if res else None


# ---------------------------------------------------------------------------- datasets
EXTRA_METRICS = {
    "probes": [
        {
            "metric_name": "probes_observed",
            "verbose_name": "Số probe quan sát (distinct)",
            "expression": "COUNT(DISTINCT probe_id)",
            "d3format": ",d",
        },
        {
            "metric_name": "countries_observed",
            "verbose_name": "Số quốc gia quan sát",
            "expression": "COUNT(DISTINCT CASE WHEN country_code <> 'ZZ' THEN country_code END)",
            "d3format": ",d",
        },
        {
            "metric_name": "asns_observed",
            "verbose_name": "Số ASN quan sát",
            "expression": "COUNT(DISTINCT CASE WHEN asn <> -1 THEN asn END)",
            "d3format": ",d",
        },
    ],
}

DATASETS = {
    "quality_hourly_probe": {
        "metric_groups": ["quality", "probes"],
        "desc": "Gold: giờ × cohort × measurement × probe. Mọi tỷ lệ tính lại từ tổng.",
    },
    "quality_hourly_country": {"metric_groups": ["quality"], "desc": "Gold: giờ × cohort × measurement × quốc gia (ZZ = chưa biết)."},
    "quality_hourly_asn": {"metric_groups": ["quality"], "desc": "Gold: giờ × cohort × measurement × ASN theo af (-1 = chưa biết)."},
    "quality_hourly_target": {"metric_groups": ["quality"], "desc": "Gold: giờ × cohort × measurement."},
    "rtt_histogram_hourly": {"metric_groups": ["histogram"], "desc": "Gold: histogram RTT log-bucket (tỉ lệ 1.02); percentile gộp được."},
    "ipv4_ipv6_paired_hourly": {"metric_groups": ["paired"], "desc": "Gold: cặp IPv4/IPv6 cùng probe, cùng target, |dt| <= 120 s."},
    "coverage_hourly": {"metric_groups": ["coverage"], "desc": "Gold: lưới đủ 24 giờ × cohort × measurement; completeness là ước tính."},
    "v_pipeline_status": {"sql": "datasets/v_pipeline_status.sql", "metric_groups": [], "desc": "Độ tươi và trạng thái công bố."},
    "v_degraded_groups": {
        "sql": "datasets/v_degraded_groups.sql",
        "metric_groups": [],
        "desc": "Quốc gia/ASN suy giảm (so cùng probe; ngưỡng đề xuất).",
    },
}


def metric_defs(groups: list[str]) -> list[dict]:
    out = []
    for g in groups:
        if g in EXTRA_METRICS:
            out += EXTRA_METRICS[g]
            continue
        for name, m in METRICS[g].items():
            out.append(
                {
                    "metric_name": name,
                    "verbose_name": m["label"],
                    "expression": m["expression"],
                    "d3format": m.get("format"),
                    "description": "Xem docs/metric-dictionary.md",
                }
            )
    return out


def ensure_dataset(api: Api, db_id: int, name: str, spec: dict) -> dict:
    existing = api.find("dataset", "table_name", name)
    body = {"database": db_id, "schema": SCHEMA, "table_name": name}
    if "sql" in spec:
        sql_lines = (PROJECT / spec["sql"]).read_text(encoding="utf-8").splitlines()
        body["sql"] = "\n".join(line for line in sql_lines if not line.strip().startswith("--"))
    if existing:
        ds_id = existing["id"]
        if "sql" in spec:
            api.call("PUT", f"/api/v1/dataset/{ds_id}", json={"sql": body["sql"]})
    else:
        ds_id = api.call("POST", "/api/v1/dataset/", json=body)["id"]
    api.call("PUT", f"/api/v1/dataset/{ds_id}/refresh")
    detail = api.call("GET", f"/api/v1/dataset/{ds_id}")["result"]
    current = {m["metric_name"]: m for m in detail.get("metrics", [])}
    metrics = []
    for m in metric_defs(spec["metric_groups"]):
        if m["metric_name"] in current:
            m = {**m, "id": current[m["metric_name"]]["id"]}
        metrics.append(m)
    columns = [c["column_name"] for c in detail["columns"]]
    update = {"description": spec["desc"], "cache_timeout": 60 if name == "v_pipeline_status" else 300}
    if metrics:
        update["metrics"] = metrics
    if TIME_COL in columns:
        update["main_dttm_col"] = TIME_COL
    api.call("PUT", f"/api/v1/dataset/{ds_id}", json=update)
    print(f"dataset {name}: id={ds_id} columns={len(columns)} metrics={len(metrics)}")
    return {"id": ds_id, "columns": set(columns)}


# ---------------------------------------------------------------------------- charts
def axis(col: str, grain: str | None = "PT1H") -> dict:
    a = {"columnType": "BASE_AXIS", "sqlExpression": col, "label": col, "expressionType": "SQL"}
    if grain:
        a["timeGrain"] = grain
    return a


def query_context(ds_id: int, form: dict, columns: list, metrics: list, orderby=None, row_limit=1000, filters=None) -> dict:
    q = {
        "filters": filters or [],
        "extras": {"having": "", "where": "", **({"time_grain_sqla": form["time_grain_sqla"]} if form.get("time_grain_sqla") else {})},
        "applied_time_extras": {},
        "columns": columns,
        "metrics": metrics,
        "orderby": orderby or [],
        "annotation_layers": [],
        "row_limit": row_limit,
        "series_limit": 0,
        "order_desc": True,
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
        "time_range": "No filter",
    }
    if form.get("viz_type", "").startswith("echarts_timeseries") or form.get("viz_type") == "heatmap_v2":
        q["post_processing"] = []
    return {
        "datasource": {"id": ds_id, "type": "table"},
        "force": False,
        "queries": [q],
        "form_data": form,
        "result_format": "json",
        "result_type": "full",
    }


def adhoc(col: str, op: str, val) -> dict:
    sql_op = {"!=": "!=", "==": "==", "IN": "IN"}[op]
    return {"expressionType": "SIMPLE", "subject": col, "operator": sql_op, "comparator": val, "clause": "WHERE"}


def charts(ds: dict) -> list[dict]:
    """Định nghĩa chart: (tên, dataset, form_data, query_context hoặc None nếu là plugin API cũ)."""
    P, H, C, PR, CV = (
        ds["quality_hourly_probe"]["id"],
        ds["rtt_histogram_hourly"]["id"],
        ds["quality_hourly_country"]["id"],
        ds["ipv4_ipv6_paired_hourly"]["id"],
        ds["coverage_hourly"]["id"],
    )
    A, ST, DG = ds["quality_hourly_asn"]["id"], ds["v_pipeline_status"]["id"], ds["v_degraded_groups"]["id"]
    out = []

    def big(name, ds_id, metric, fmt, desc, filters=None):
        form = {
            "viz_type": "big_number_total",
            "metric": metric,
            "adhoc_filters": [adhoc(*f) for f in (filters or [])],
            "time_range": "No filter",
            "y_axis_format": fmt,
            "subheader": desc,
        }
        out.append(
            (
                name,
                ds_id,
                form,
                query_context(ds_id, form, [], [metric], filters=[{"col": f[0], "op": f[1], "val": f[2]} for f in (filters or [])]),
                desc,
            )
        )

    def series(name, ds_id, metrics, groupby, desc, fmt=".2f"):
        form = {
            "viz_type": "echarts_timeseries_line",
            "x_axis": TIME_COL,
            "time_grain_sqla": "PT1H",
            "metrics": metrics,
            "groupby": groupby,
            "adhoc_filters": [],
            "time_range": "No filter",
            "row_limit": 10000,
            "show_legend": True,
            "rich_tooltip": True,
            "y_axis_format": fmt,
            "truncate_metric": True,
            "markerEnabled": True,
        }
        out.append((name, ds_id, form, query_context(ds_id, form, [axis(TIME_COL)] + groupby, metrics, row_limit=10000), desc))

    def table(name, ds_id, groupby, metrics, order, desc, limit=500, filters=None):
        form = {
            "viz_type": "table",
            "query_mode": "aggregate",
            "groupby": groupby,
            "metrics": metrics,
            "all_columns": [],
            "adhoc_filters": [adhoc(*f) for f in (filters or [])],
            "time_range": "No filter",
            "row_limit": limit,
            "order_desc": True,
            "timeseries_limit_metric": order,
            "include_search": True,
            "server_pagination": False,
        }
        out.append(
            (
                name,
                ds_id,
                form,
                query_context(
                    ds_id,
                    form,
                    groupby,
                    metrics,
                    orderby=[[order, False]],
                    row_limit=limit,
                    filters=[{"col": f[0], "op": f[1], "val": f[2]} for f in (filters or [])],
                ),
                desc,
            )
        )

    def raw(name, ds_id, columns, order, desc, limit=500, filters=None):
        form = {
            "viz_type": "table",
            "query_mode": "raw",
            "all_columns": columns,
            "groupby": [],
            "metrics": [],
            "adhoc_filters": [adhoc(*f) for f in (filters or [])],
            "time_range": "No filter",
            "row_limit": limit,
            "order_by_cols": [json.dumps([order, False])],
            "include_search": True,
        }
        out.append(
            (
                name,
                ds_id,
                form,
                query_context(
                    ds_id,
                    form,
                    columns,
                    [],
                    orderby=[[order, False]],
                    row_limit=limit,
                    filters=[{"col": f[0], "op": f[1], "val": f[2]} for f in (filters or [])],
                ),
                desc,
            )
        )

    # Hàng 1: trạng thái dữ liệu
    raw(
        "Trạng thái dữ liệu theo measurement",
        ST,
        [
            "probe_set_version",
            "measurement_id",
            "target",
            "af",
            "latest_measured_hour_local",
            "freshness_minutes",
            "freshness_state",
            "hours_not_processed_past_slo",
            "silver_status",
            "gold_status",
            "gold_built_at",
        ],
        "measurement_id",
        "Độ tươi so với SLO 120 phút (đề xuất) và trạng thái công bố Silver/Gold.",
        limit=50,
    )
    big("Số probe quan sát", P, "probes_observed", ",d", "Probe phân biệt có ≥ 1 result trong phạm vi lọc")
    big("Số quốc gia quan sát", P, "countries_observed", ",d", "Theo metadata probe (không gồm ZZ)")
    big(
        "Độ đầy đủ ước tính (giờ đã xử lý)",
        CV,
        "completeness_est_pct",
        ".1f",
        "Result / (probe cohort × 15/giờ); ước tính, có xu hướng thấp hơn thực tế",
        filters=[("data_state", "!=", "not_processed")],
    )
    # Hàng 2: KPI
    big("Packet loss (%)", P, "packet_loss_pct", ".2f", "Σ lost / Σ sent trên result đã đo")
    big("RTT trung bình theo packet (ms)", P, "rtt_mean_ms", ".1f", "Σ RTT / Σ reply hợp lệ")
    big("p95 RTT (ms, xấp xỉ histogram)", H, "rtt_p95_ms", ".1f", "Percentile theo packet; sai số đo ≤ ~1,6%")
    big(
        "Tỷ lệ result có phản hồi (trên result đã đo, %)", P, "response_rate_observed_pct", ".2f", "Không tính result send_error vào mẫu số"
    )
    big("Số result", P, "results", ",d", "Result đủ điều kiện (public, đồng hồ hợp lệ, thuộc cohort)")
    # Hàng 3: bản đồ + bảng quốc gia
    form_map = {
        "viz_type": "world_map",
        "entity": "country_code",
        "country_fieldtype": "cca2",
        "metric": "packet_loss_pct",
        "secondary_metric": "results",
        "show_bubbles": True,
        "max_bubble_size": "25",
        "adhoc_filters": [],
        "time_range": "No filter",
        "row_limit": 10000,
        "linear_color_scheme": "superset_seq_1",
        "color_picker": {"r": 0, "g": 122, "b": 135, "a": 1},
    }
    out.append(
        (
            "Bản đồ packet loss theo quốc gia probe",
            C,
            form_map,
            None,
            "Màu = packet loss; bong bóng = số result. Vị trí theo country_code trong metadata probe, không phải vị trí instance anycast.",
        )
    )
    table(
        "Chất lượng theo quốc gia",
        P,
        ["country_code"],
        ["probes_observed", "results", "packet_loss_pct", "rtt_mean_ms", "response_rate_observed_pct", "results_all_lost"],
        "results",
        "Số probe và cỡ mẫu đi kèm mọi tỷ lệ; ZZ = chưa biết quốc gia.",
    )
    # Hàng 4: time series theo target/af
    series("Packet loss theo target và address family", P, ["packet_loss_pct"], ["target", "af"], "Tính lại từ tổng theo time grain")
    series("RTT trung bình theo target và address family", P, ["rtt_mean_ms"], ["target", "af"], "RTT theo packet")
    series("p95 RTT theo target và address family", H, ["rtt_p95_ms"], ["target", "af"], "Xấp xỉ từ histogram, gộp đúng theo time grain")
    series("Tỷ lệ result có phản hồi theo target và af", P, ["response_rate_observed_pct"], ["target", "af"], "Trên result đã đo được")
    # Hàng 5: heatmap
    form_hm = {
        "viz_type": "heatmap_v2",
        "x_axis": TIME_COL,
        "time_grain_sqla": "PT1H",
        "groupby": "country_code",
        "metric": "packet_loss_pct",
        "adhoc_filters": [],
        "time_range": "No filter",
        "row_limit": 10000,
        "sort_x_axis": "alpha_asc",
        "sort_y_axis": "alpha_asc",
        "normalize_across": "heatmap",
        "linear_color_scheme": "superset_seq_1",
        "show_values": False,
        "value_bounds": [None, None],
    }
    out.append(
        (
            "Heatmap packet loss: quốc gia × giờ",
            C,
            form_hm,
            query_context(C, form_hm, [axis(TIME_COL), "country_code"], ["packet_loss_pct"], row_limit=10000),
            "Ô trống = không có result trong giờ đó cho quốc gia đó (khác với mất toàn bộ packet = 100%).",
        )
    )
    # Hàng 6: suy giảm + ASN
    raw(
        "Quốc gia / ASN suy giảm (so cùng probe)",
        DG,
        [
            "level",
            "group_key",
            "measurement_id",
            "target",
            "af",
            "common_probes",
            "recent_results",
            "baseline_results",
            "recent_loss_pct",
            "baseline_loss_pct",
            "recent_rtt_mean_ms",
            "baseline_rtt_mean_ms",
            "status",
        ],
        "recent_loss_pct",
        (
            "3 giờ gần nhất so với 24 giờ trước đó trên các probe có dữ liệu ở cả hai; "
            "ngưỡng đề xuất: +2 điểm % loss hoặc +20% RTT, ≥ 2 probe, ≥ 50 result."
        ),
        filters=[("status", "!=", "insufficient_sample")],
    )
    table(
        "Chất lượng theo ASN",
        A,
        ["asn", "asn_primary_country"],
        ["results", "packet_loss_pct", "rtt_mean_ms", "response_rate_observed_pct"],
        "results",
        "ASN theo af của measurement (asn_v4 / asn_v6); -1 = chưa biết.",
    )
    # Hàng 7: IPv4 / IPv6
    form_bar = {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "target",
        "metrics": ["v4_rtt_mean_ms", "v6_rtt_mean_ms"],
        "groupby": [],
        "adhoc_filters": [],
        "time_range": "No filter",
        "row_limit": 100,
        "show_legend": True,
        "y_axis_format": ".1f",
        "show_value": True,
    }
    out.append(
        (
            "RTT IPv4 vs IPv6 trên cặp cùng probe",
            PR,
            form_bar,
            query_context(PR, form_bar, [axis("target", None)], ["v4_rtt_mean_ms", "v6_rtt_mean_ms"], row_limit=100),
            "Chỉ tính trên cặp result cùng probe, cùng target, |Δt| ≤ 120 s.",
        )
    )
    table(
        "So sánh IPv4/IPv6 theo cặp (paired cohort)",
        PR,
        ["target"],
        [
            "pairs",
            "v4_packet_loss_pct",
            "v6_packet_loss_pct",
            "v4_rtt_mean_ms",
            "v6_rtt_mean_ms",
            "delta_rtt_v6_minus_v4_ms",
            "unpaired_v4",
            "unpaired_v6",
        ],
        "pairs",
        "Số cặp thực tế và số result không có cặp được hiển thị để đánh giá độ tin cậy.",
        limit=50,
    )
    # Hàng 8: độ phủ
    series("Độ đầy đủ ước tính theo measurement", CV, ["completeness_est_pct"], ["measurement_id"], "Giờ chưa xử lý = 0%", fmt=".1f")
    raw(
        "Tình trạng xử lý theo giờ",
        CV,
        [
            TIME_COL,
            "measurement_id",
            "data_state",
            "expected_probes",
            "probes_observed",
            "results_total_raw",
            "expected_results",
            "results_excluded_clock",
            "results_excluded_not_public",
            "quarantine_records",
            "silver_status",
        ],
        TIME_COL,
        "measured / processed_no_results / not_processed được phân biệt.",
        limit=1000,
    )
    # Hàng 9: drill-down
    table(
        "Drill-down theo probe",
        P,
        ["probe_id", "country_code", "asn", "measurement_id"],
        ["results", "packet_loss_pct", "rtt_mean_ms", "results_all_lost"],
        "packet_loss_pct",
        "Sắp theo packet loss giảm dần; lọc probe/quốc gia/ASN bằng filter của dashboard.",
    )
    return out


def ensure_chart(api: Api, name: str, ds_id: int, form: dict, qc: dict | None, desc: str) -> int:
    form = {**form, "datasource": f"{ds_id}__table"}
    body = {
        "slice_name": name,
        "viz_type": form["viz_type"],
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(form),
        "description": desc,
    }
    if qc:
        body["query_context"] = json.dumps(qc)
    existing = api.find("chart", "slice_name", name)
    if existing:
        api.call("PUT", f"/api/v1/chart/{existing['id']}", json=body)
        return existing["id"]
    return api.call("POST", "/api/v1/chart/", json=body)["id"]


def validate_chart(api: Api, qc: dict) -> tuple[bool, str]:
    try:
        res = api.call("POST", "/api/v1/chart/data", json=qc)["result"][0]
    except RuntimeError as exc:
        return False, str(exc)[:300]
    if res.get("error"):
        return False, res["error"][:300]
    return True, f"rows={res.get('rowcount')}"


# ---------------------------------------------------------------------------- dashboard
LAYOUT = [  # (row of chart names, height)
    (["Trạng thái dữ liệu theo measurement"], 40),
    (["Số probe quan sát", "Số quốc gia quan sát", "Độ đầy đủ ước tính (giờ đã xử lý)"], 24),
    (
        [
            "Packet loss (%)",
            "RTT trung bình theo packet (ms)",
            "p95 RTT (ms, xấp xỉ histogram)",
            "Tỷ lệ result có phản hồi (trên result đã đo, %)",
            "Số result",
        ],
        24,
    ),
    (["Bản đồ packet loss theo quốc gia probe", "Chất lượng theo quốc gia"], 60),
    (["Packet loss theo target và address family", "RTT trung bình theo target và address family"], 50),
    (["p95 RTT theo target và address family", "Tỷ lệ result có phản hồi theo target và af"], 50),
    (["Heatmap packet loss: quốc gia × giờ"], 90),
    (["Quốc gia / ASN suy giảm (so cùng probe)", "Chất lượng theo ASN"], 60),
    (["RTT IPv4 vs IPv6 trên cặp cùng probe", "So sánh IPv4/IPv6 theo cặp (paired cohort)"], 50),
    (["Độ đầy đủ ước tính theo measurement", "Tình trạng xử lý theo giờ"], 60),
    (["Drill-down theo probe"], 70),
]

NOTES = """### Đọc dashboard đúng cách
- **Chọn đúng một probe set (cohort).** `strat-k2-c100-v1` là mẫu phân tầng theo quốc gia × ASN; `dev-200-v1` là 200 probe phát triển.
- Kết quả **chỉ phản ánh các probe và target được quan sát** (K-root, F-root qua built-in ping).
  Không suy ra băng thông hay chất lượng toàn bộ Internet.
- Nhận định cấp quốc gia/toàn cầu phải xem kèm **số probe, số result và độ phủ**. Phân bố probe RIPE Atlas lệch mạnh về châu Âu/Bắc Mỹ.
- K-root và F-root là **anycast**: bản đồ là vị trí probe, không phải vị trí instance đã trả lời.
- p50/p95/p99 tính **theo packet** từ histogram log-bucket, xấp xỉ (sai số đo ≤ ~1,6%).
- Thời gian theo giờ **Asia/Ho_Chi_Minh**. Cache 5 phút: số liệu có thể trễ tối đa 5 phút so với Gold.
- Ngưỡng "suy giảm" và SLO độ tươi là **cấu hình đề xuất**.
"""


def position(chart_ids: dict[str, int]) -> dict:
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": "Global Network Quality"}},
    }
    rows = LAYOUT + [(["__notes__"], 40)]
    for i, (names, height) in enumerate(rows):
        row_id = f"ROW-{i}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        width = 12 // len(names)
        for j, name in enumerate(names):
            if name == "__notes__":
                cid = "MARKDOWN-notes"
                pos[cid] = {
                    "type": "MARKDOWN",
                    "id": cid,
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", row_id],
                    "meta": {"width": 12, "height": height, "code": NOTES},
                }
            else:
                cid = f"CHART-{chart_ids[name]}"
                pos[cid] = {
                    "type": "CHART",
                    "id": cid,
                    "children": [],
                    "parents": ["ROOT_ID", "GRID_ID", row_id],
                    "meta": {
                        "chartId": chart_ids[name],
                        "width": width if j < len(names) - 1 else 12 - width * (len(names) - 1),
                        "height": height,
                        "sliceName": name,
                    },
                }
            pos[row_id]["children"].append(cid)
    return pos


def native_filters(ds: dict, chart_ds: dict[int, str]) -> list[dict]:
    all_charts = sorted(chart_ds)

    def scope(column: str, excluded_names: set[str] = frozenset()) -> tuple[list[int], list[int]]:
        inside = [c for c in all_charts if column in ds[chart_ds[c]]["columns"] and chart_ds[c] not in excluded_names]
        return inside, [c for c in all_charts if c not in inside]

    def select(fid, name, column, dataset, multi=True, default=None, required=False, excluded_ds=frozenset()):
        inside, excluded = scope(column, excluded_ds)
        f = {
            "id": fid,
            "name": name,
            "filterType": "filter_select",
            "type": "NATIVE_FILTER",
            "description": "",
            "targets": [{"datasetId": ds[dataset]["id"], "column": {"name": column}}],
            "controlValues": {
                "enableEmptyFilter": required,
                "defaultToFirstItem": False,
                "multiSelect": multi,
                "searchAllOptions": False,
                "inverseSelection": False,
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded},
            "chartsInScope": inside,
            "tabsInScope": [],
            "defaultDataMask": {"filterState": {}, "extraFormData": {}},
        }
        if default is not None:
            f["defaultDataMask"] = {
                "filterState": {"value": [default]},
                "extraFormData": {"filters": [{"col": column, "op": "IN", "val": [default]}]},
            }
        return f

    no_time = {"v_pipeline_status", "v_degraded_groups"}
    time_inside, time_excluded = scope(TIME_COL, no_time)
    return [
        select(
            "NATIVE_FILTER-cohort",
            "Probe set (cohort) — chọn 1",
            "probe_set_version",
            "quality_hourly_probe",
            multi=False,
            default=DEFAULT_COHORT,
            required=True,
        ),
        {
            "id": "NATIVE_FILTER-time",
            "name": "Khoảng thời gian (giờ VN)",
            "filterType": "filter_time",
            "type": "NATIVE_FILTER",
            "description": "Theo event_hour_local (Asia/Ho_Chi_Minh)",
            "targets": [{}],
            "controlValues": {},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": time_excluded},
            "chartsInScope": time_inside,
            "tabsInScope": [],
            "defaultDataMask": {"filterState": {"value": "No filter"}, "extraFormData": {"time_range": "No filter"}},
        },
        {
            "id": "NATIVE_FILTER-grain",
            "name": "Time grain",
            "filterType": "filter_timegrain",
            "type": "NATIVE_FILTER",
            "description": "",
            "targets": [{"datasetId": ds["quality_hourly_probe"]["id"]}],
            "controlValues": {},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": time_excluded},
            "chartsInScope": time_inside,
            "tabsInScope": [],
            "defaultDataMask": {"filterState": {}, "extraFormData": {}},
        },
        select("NATIVE_FILTER-target", "Target", "target", "quality_hourly_probe"),
        select("NATIVE_FILTER-af", "Address family", "af", "quality_hourly_probe"),
        select("NATIVE_FILTER-msm", "Measurement", "measurement_id", "quality_hourly_probe"),
        select("NATIVE_FILTER-country", "Quốc gia (probe)", "country_code", "quality_hourly_probe"),
        select("NATIVE_FILTER-asn", "ASN", "asn", "quality_hourly_probe"),
        select("NATIVE_FILTER-probe", "Probe", "probe_id", "quality_hourly_probe"),
    ]


def main() -> int:
    api = Api()
    db = api.find("database", "database_name", DB_NAME)
    if not db:
        raise SystemExit(f"Database {DB_NAME!r} not found; run superset-init first")
    ds = {name: ensure_dataset(api, db["id"], name, spec) for name, spec in DATASETS.items()}
    ds_by_id = {v["id"]: k for k, v in ds.items()}

    chart_ids, chart_ds, failures = {}, {}, []
    for name, ds_id, form, qc, desc in charts(ds):
        cid = ensure_chart(api, name, ds_id, form, qc, desc)
        chart_ids[name], chart_ds[cid] = cid, ds_by_id[ds_id]
        if qc:
            ok, info = validate_chart(api, qc)
            print(f"chart {cid:>3} {'OK  ' if ok else 'FAIL'} {name}: {info}")
            if not ok:
                failures.append(name)
        else:
            print(f"chart {cid:>3} (legacy plugin, kiểm tra bằng trình duyệt) {name}")

    meta = {
        "native_filter_configuration": native_filters(ds, chart_ds),
        "cross_filters_enabled": True,
        "color_scheme": "",
        "refresh_frequency": 0,
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "label_colors": {},
        "shared_label_colors": [],
        "color_scheme_domain": [],
        "default_filters": "{}",
        "chart_configuration": {},
        "global_chart_configuration": {"scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "chartsInScope": sorted(chart_ds)},
    }
    body = {
        "dashboard_title": "Global Network Quality",
        "slug": SLUG,
        "published": True,
        "position_json": json.dumps(position(chart_ids)),
        "json_metadata": json.dumps(meta),
    }
    existing = api.find("dashboard", "slug", SLUG)
    dash_id = (
        api.call("PUT", f"/api/v1/dashboard/{existing['id']}", json=body)["id"]
        if existing
        else api.call("POST", "/api/v1/dashboard/", json=body)["id"]
    )
    for cid in chart_ds:
        api.call("PUT", f"/api/v1/chart/{cid}", json={"dashboards": [dash_id]})
    print(f"dashboard id={dash_id} url={BASE}/superset/dashboard/{SLUG}/ charts={len(chart_ids)}")

    export = api.s.get(f"{BASE}/api/v1/dashboard/export/?q={rison([dash_id])}")
    export.raise_for_status()
    out = PROJECT / "export" / "global_network_quality.zip"
    out.parent.mkdir(exist_ok=True)
    out.write_bytes(export.content)
    print(f"exported {out} ({len(export.content)} bytes)")
    if failures:
        print("FAILED charts:", failures)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
