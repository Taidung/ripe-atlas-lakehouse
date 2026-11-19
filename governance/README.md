# governance/: catalog metadata và lineage (profile `governance`)

OpenMetadata 2.0.8 gồm các service `om-db`, `om-search`, `om-migrate`, `om-server`, `om-ingestion`.

| File | Vai trò |
|---|---|
| `openmetadata/Dockerfile` | Image `openmetadata-ingestion` cùng phiên bản với server |
| `openmetadata/run_ingestion.py` | Chạy connector Trino và Superset; khai báo lineage và mô tả qua REST; kiểm tra lại kết quả |
| `openmetadata/catalog.yaml` | Nguồn khai báo: owner, mô tả bảng, glossary, và lineage kèm `via:` (file/hàm tạo ra cạnh đó) |

Mô tả metric lấy từ `config/metrics.json`, dùng chung với Gold và Superset.

```bash
docker compose --profile governance run --rm om-ingestion
```

Chi tiết: `docs/governance.md`.
