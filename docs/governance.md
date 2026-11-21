# Governance: Nessie, dbt docs và phân quyền Trino

Bản trước dùng OpenMetadata 2.0.8 (cần khoảng 2,1 GB RAM cùng OpenSearch và Postgres riêng). Khi chuyển tầng biến đổi
sang dbt (ADR-0003), catalog nghiệp vụ và lineage được sinh thẳng từ dự án dbt, nên OpenMetadata được bỏ.

## Phân vai

| | Nessie 0.108.8 | dbt docs (dbt-core 1.12.7) |
|---|---|---|
| Vai trò | Catalog **kỹ thuật**: con trỏ tới metadata file mới nhất của từng bảng Iceberg, commit và branch | Catalog **nghiệp vụ**: mô tả bảng/cột, test gắn với từng bảng, lineage |
| Ai dùng | Trino (catalog `lakehouse` ghi, `nessie` đọc), test tích hợp (nhánh `dqtest`) | Người phân tích, người vận hành |
| Nguồn | Mọi commit Iceberg đi qua Nessie | `transformation/dbt/models/**/*.yml`, `sources.yml`, SQL của model |
| Lưu trữ | Postgres (`postgres_data`) | Sinh lại khi cần (`dbt docs generate`), không có service thường trực |

## Chạy dbt docs

```bash
docker compose --profile jobs run --rm -p 8580:8580 --entrypoint sh transform -c "cd /opt/app/transformation/dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8580 --host 0.0.0.0 --no-browser"
```

Mở http://localhost:8580.

## Những gì có trong catalog

- **Nguồn:** bảng ngoài `staging.ripe_atlas.*` (file `.jsonl.gz` trên staging), bảng runner ghi (`work.bronze_pending_object`,
  `bronze.ingest_manifest`, `ops.partition_status`).
- **Model:** Bronze, Silver (`dim_*`, `fact_*` và bản nháp `*_candidate`), Gold (7 mart và bản nháp).
- **Mô tả:** grain của từng bảng và các cột chính; data dictionary đầy đủ ở `docs/data-dictionary.md`.
- **Test:** mỗi test dbt hiện cạnh bảng mà nó kiểm tra (critical = `severity: error`, cảnh báo = `severity: warn`).
- **Lineage:** suy ra tự động từ `ref()` và `source()` trong SQL, đi từ file staging tới mart Gold, **không khai báo tay**.
  Bản OpenMetadata trước phải khai báo lineage Spark bằng `catalog.yaml` vì chưa kiểm chứng được OpenLineage với Nessie.

Chưa có: owner, classification `DataLayer`, glossary metric dạng thực thể riêng (định nghĩa metric vẫn ở
`config/metrics.json` và `docs/metric-dictionary.md`), và lineage xuống dashboard Superset.

## Phân quyền truy cập dữ liệu (Trino)

| Catalog | User | Quyền | Khóa S3 |
|---|---|---|---|
| `lakehouse` | `transform` | Ghi | `atlas-processing` (đọc staging, ghi warehouse) |
| `staging` | `transform` | Đọc file staging | `atlas-processing` |
| `lakehouse_dqtest` | `transform` | Ghi trên nhánh `dqtest` (chỉ lúc test) | `atlas-processing` |
| `nessie` | mọi user | Chỉ đọc | `atlas-reader` |

Luật nằm ở `serving/trino/rules.json`. Trino trong đồ án không bật xác thực, nên tên user do client tự khai: đây là rào
chắn nhầm lẫn (Superset, DBeaver không ghi nhầm được), chưa phải kiểm soát truy cập thật.

## Thông tin chất lượng

- Kết quả từng dbt test của mỗi lần chạy nằm ở `ops.dq_result` (`engine = dbt-1.12.7`, `severity` critical/warn).
- Trạng thái công bố theo (bảng, ngày, measurement) nằm ở `ops.partition_status`.
