# observability/: metric, dashboard và cảnh báo (profile `monitoring`)

Job ingestion và transform đẩy metric lên **Pushgateway** khi kết thúc.
- Grouping key: job, measurement, ngày/giờ.
- Outcome (thành công / thất bại) được tách thành group riêng.

Prometheus scrape Pushgateway và node-exporter. Grafana hiển thị dashboard và đánh giá alert.

| File | Vai trò |
|---|---|
| `prometheus/prometheus.yml` | Cấu hình scrape |
| `grafana/build_dashboard.py` | **Nguồn** của dashboard "RIPE Atlas Pipeline". Sửa file này rồi chạy lại để sinh JSON |
| `grafana/dashboards/ripe-pipeline.json` | JSON sinh ra (CI kiểm tra JSON khớp với script) |
| `grafana/provisioning/alerting/rules.yaml` | 8 luật cảnh báo: job lỗi liên tiếp, Gold trễ, thiếu dữ liệu, DQ critical, đĩa sắp đầy, service không phản hồi, API 429, quarantine |
| `grafana/provisioning/{datasources,dashboards}/` | Khai báo datasource Prometheus và nơi đọc dashboard |

```bash
python observability/grafana/build_dashboard.py
```

Runbook cho từng alert: `docs/runbook.md`.
