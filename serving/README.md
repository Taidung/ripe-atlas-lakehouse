# serving/: truy vấn và dashboard

| Thành phần | Service Compose (profile `analytics`) | Vai trò |
|---|---|---|
| Trino | `trino` | SQL engine đọc Iceberg qua catalog `nessie` |
| Superset | `superset-db`, `superset-init`, `superset` | Dashboard "Global Network Quality" |

## File

| File | Vai trò |
|---|---|
| `trino/catalog/nessie.properties` | Catalog Iceberg → Nessie + SeaweedFS |
| `trino/config.properties` | Giới hạn bộ nhớ truy vấn (tránh treo máy 16 GB) |
| `superset/Dockerfile`, `requirements.txt` | Image Superset kèm driver Trino và Postgres |
| `superset/superset_config.py`, `superset-init.sh` | Cấu hình; khởi tạo DB và tài khoản admin |
| `superset/provision.py` | Dựng dataset, metric và chart bằng REST API (idempotent) |
| `superset/datasets/*.sql` | Dataset ảo: `v_degraded_groups`, `v_pipeline_status` |
| `superset/verify_filters.py` | Kiểm tra tổ hợp filter / rollup / timezone qua API truy vấn của Superset |
| `superset/export/` | ZIP dashboard đã xuất |

Định nghĩa metric **không** nằm ở đây. Gold, Superset và OpenMetadata dùng chung `config/metrics.json` (mount vào `/app/config/metrics.json`).

```bash
docker compose exec -T superset /app/.venv/bin/python /app/project/provision.py
```

Đặc tả dashboard: `docs/dashboard-spec.md`. Ví dụ SQL: `docs/sql-examples.md`.
