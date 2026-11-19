"""Sinh dashboard Grafana "RIPE Atlas Pipeline" (JSON được provision từ monitoring/grafana/dashboards/).

  python monitoring/grafana/build_dashboard.py
"""

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
panels, y = [], 0
_id = 0


def panel(title, kind, targets, w=12, h=8, unit=None, thresholds=None, desc="", options=None, overrides=None):
    global _id, y
    _id += 1
    x = sum(p["gridPos"]["w"] for p in panels if p["gridPos"]["y"] == y) if panels else 0
    if x + w > 24:
        y += max(p["gridPos"]["h"] for p in panels if p["gridPos"]["y"] == y)
        x = 0
    p = {"id": _id, "type": kind, "title": title, "description": desc, "datasource": DS,
         "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": lf, "datasource": DS,
                      **({"instant": True, "format": "table"} if kind == "table" else {})} for i, (e, lf) in enumerate(targets)],
         "fieldConfig": {"defaults": {}, "overrides": overrides or []}, "options": options or {}}
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in thresholds]}
        p["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
    panels.append(p)


def row_break():
    global y
    if panels:
        y += max(p["gridPos"]["h"] for p in panels if p["gridPos"]["y"] == y)


# --- Hàng 1: độ tươi và trạng thái job
panel("Độ tươi Gold theo measurement (giây kể từ event mới nhất)", "stat",
      [("time() - max by (measurement_id) (ripe_gold_max_event_timestamp_seconds)", "{{measurement_id}}")], w=12, unit="s",
      thresholds=[(None, "green"), (5400, "orange"), (7200, "red")], desc="SLO đề xuất: ≤ 7200 s (2 giờ).")
panel("Lần thành công gần nhất theo stage (giây trước)", "stat",
      [("time() - max by (stage) (ripe_job_last_success_timestamp_seconds)", "{{stage}}")], w=12, unit="s",
      thresholds=[(None, "green"), (3900, "orange"), (7200, "red")], desc="Lịch chạy theo giờ: > 1 giờ 5 phút là trễ một lần.")
row_break()
panel("Lần chạy gần nhất thành công? (1/0)", "stat", [("max by (stage, run_type) (ripe_job_last_run_success)", "{{stage}} {{run_type}}")],
      w=12, thresholds=[(None, "red"), (1, "green")])
panel("Thời lượng job lần gần nhất", "bargauge", [("max by (stage) (ripe_job_duration_seconds)", "{{stage}}")], w=12, unit="s")
row_break()
# --- Hàng 2: API
panel("Request RIPE Atlas API theo kết quả (lần chạy gần nhất)", "bargauge",
      [("sum by (endpoint, status_class) (ripe_api_requests)", "{{endpoint}} {{status_class}}")], w=8)
panel("Retry theo lý do (429 / 5xx / mạng)", "bargauge", [("sum by (endpoint, reason) (ripe_api_retries)", "{{endpoint}} {{reason}}")], w=8)
panel("Thời gian trung bình mỗi request", "stat",
      [("sum by (endpoint) (ripe_api_request_duration_seconds_total) / sum by (endpoint) (ripe_api_request_count_total)", "{{endpoint}}")],
      w=8, unit="s")
row_break()
# --- Hàng 3: khối lượng và chất lượng
panel("Result ingest theo loại (lần chạy gần nhất)", "bargauge",
      [("sum by (kind) (ripe_ingest_records{kind=~\"fetched|staged|unchanged|changed|status_malformed_json\"})", "{{kind}}")], w=8,
      desc="unchanged = trùng nội dung khi tải chồng lấn (không stage lại).")
panel("Bronze: dòng theo loại", "bargauge", [("sum by (entity, kind) (ripe_bronze_rows)", "{{entity}} {{kind}}")], w=8,
      desc="already_present = xử lý lại; quarantined = bị cách ly.")
panel("Kiểm tra chất lượng thất bại", "stat", [("max by (layer, severity) (ripe_dq_failed_checks)", "{{layer}} {{severity}}")], w=8,
      thresholds=[(None, "green"), (1, "red")])
row_break()
panel("Độ đầy đủ ước tính giờ gần nhất", "bargauge",
      [("max by (probe_set_version, measurement_id) (ripe_gold_latest_hour_completeness_ratio)", "{{probe_set_version}} {{measurement_id}}")],
      w=12, unit="percentunit", thresholds=[(None, "red"), (0.5, "orange"), (0.9, "green")])
panel("Silver: số dòng theo measurement (ngày vừa tính lại)", "bargauge", [("max by (measurement_id) (ripe_silver_rows)", "{{measurement_id}}")], w=12)
row_break()
# --- Hàng 4: hạ tầng
panel("Service health (up)", "stat", [("min by (job) (up)", "{{job}}")], w=8, thresholds=[(None, "red"), (1, "green")])
panel("Đĩa dữ liệu Docker đã dùng", "stat",
      [("max(node_filesystem_size_bytes{mountpoint=\"/mnt/docker-desktop-disk\"} - node_filesystem_free_bytes{mountpoint=\"/mnt/docker-desktop-disk\"})", "used")],
      w=8, unit="bytes", thresholds=[(None, "green"), (40e9, "orange"), (50e9, "red")], desc="Giới hạn thật là ổ D: của host; ngưỡng đề xuất 50 GB.")
panel("RAM VM Docker đang dùng", "stat", [("1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes", "mem")], w=8,
      unit="percentunit", thresholds=[(None, "green"), (0.8, "orange"), (0.9, "red")])
row_break()
panel("CPU VM Docker", "timeseries", [("1 - avg(rate(node_cpu_seconds_total{mode=\"idle\"}[5m]))", "cpu busy")], w=8, unit="percentunit")
panel("Trino: thời gian thực thi truy vấn (5 phút gần nhất)", "timeseries",
      [("trino_execution_name_QueryManager_ExecutionTime_FiveMinutes{quantile=\"0.5\"} / 1000", "p50"),
       ("trino_execution_name_QueryManager_ExecutionTime_FiveMinutes{quantile=\"0.95\"} / 1000", "p95")], w=8, unit="s",
      desc="Độ trễ truy vấn do Superset / SQL client gửi tới Trino.")
panel("Trino: truy vấn đang chạy / thất bại", "timeseries",
      [("trino_execution_name_QueryManager_RunningQueries", "running"),
       ("increase(trino_execution_name_QueryManager_FailedQueries_TotalCount[15m])", "failed (15m)")], w=8)

dashboard = {"uid": "ripe-pipeline", "title": "RIPE Atlas Pipeline", "tags": ["ripe-atlas"], "timezone": "browser",
             "schemaVersion": 39, "version": 1, "refresh": "1m", "time": {"from": "now-6h", "to": "now"}, "panels": panels,
             "description": "Vận hành pipeline: độ tươi, job, API, khối lượng, chất lượng, hạ tầng. Runbook: docs/runbook.md"}
out = Path(__file__).resolve().parent / "dashboards" / "ripe-pipeline.json"
out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps(dashboard, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
print(f"wrote {out} with {len(panels)} panels")
