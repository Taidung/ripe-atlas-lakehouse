# Governance: Nessie và OpenMetadata

## Phân vai

| | Nessie 0.108.8 | OpenMetadata 2.0.8 |
|---|---|---|
| Vai trò | Catalog **kỹ thuật**: con trỏ tới metadata file mới nhất của từng bảng Iceberg, commit và branch | Catalog **nghiệp vụ**: khám phá dữ liệu, mô tả, owner, tag, glossary, lineage |
| Ai dùng | Spark (ghi), Trino (đọc), script kiểm thử (nhánh tạm) | Người phân tích, người vận hành |
| Có nằm trên đường ghi dữ liệu không | Có: mọi commit Iceberg đi qua Nessie | Không: chỉ đọc metadata từ Trino và Superset |
| Lưu trữ | Postgres (`postgres_data`) | Postgres (`om_db_data`) + OpenSearch (`om_search_data`) |

## Chạy

```bash
# OpenMetadata cần khoảng 2,1 GB RAM. Trên máy 16 GB, hãy dừng Airflow và monitoring trước.
# Lưu ý: luôn ghi tên service cụ thể khi stop; lệnh stop chỉ kèm --profile sẽ dừng cả core.
docker compose --profile orchestration stop airflow airflow-db
docker compose --profile monitoring stop prometheus grafana pushgateway node-exporter
docker compose --profile analytics --profile governance up -d om-db om-search om-server trino superset superset-db
docker compose --profile governance run --rm om-ingestion
```

- Mở http://localhost:8585, đăng nhập `admin@open-metadata.org` / `admin`.
- Tài khoản mặc định của OM chỉ dùng cho môi trường local; hãy đổi mật khẩu trong UI nếu chia sẻ máy.

## Những gì được đưa vào (`governance/openmetadata/run_ingestion.py`)

1. **Connector Trino** (service `trino_lakehouse`, catalog `nessie`): schema `bronze`, `silver`, `gold`, `ops`. Bỏ qua các bảng legacy.
2. **Connector Superset** (service `superset_dashboards`): dashboard, chart và data model. Lineage từ dataset tới bảng Trino được connector tự suy ra.
3. **Làm giàu từ `governance/openmetadata/catalog.yaml`:**
   - Mô tả bảng và cột chính.
   - Owner team `data-platform`.
   - Classification `DataLayer` (Bronze/Silver/Gold/Ops).
   - Glossary `RIPE Atlas Metrics` gồm 28 thuật ngữ, sinh từ `config/metrics.json`, có kèm biểu thức tính.
4. **Lineage khai báo:** storage service `seaweedfs_staging` (3 container staging) → Bronze → Silver → Gold.
   - Có column-level lineage cho các cột chính.
   - Mỗi cạnh ghi `source: Manual` và mô tả `Khai báo từ code: <file/hàm> (provenance: declared-from-code)`.

## Vì sao lineage Spark là khai báo

- Tài liệu OM 2.x hướng dẫn dùng `openlineage-spark` ≥ 1.37.0, gửi tới `/api/v1/openlineage/lineage`.
- Trong đồ án này, Spark ghi bảng Iceberg qua `NessieCatalog` vào `s3://warehouse/...`. OM cần map được dataset đó sang entity bảng của service Trino. Điều này phụ thuộc cấu hình map namespace ở phía server, và **chưa được kiểm chứng** trên phiên bản đang dùng.
- Để không khẳng định điều chưa thử, lineage Spark được **khai báo từ chính mapping trong code**. Mỗi khi đổi job thì cập nhật `catalog.yaml`.
- Chỉ khẳng định table/column lineage ở phạm vi đã kiểm tra (đo ngày 2026-11-19):
  - 23 cạnh khai báo.
  - Quanh `silver.fact_ping_result`: 8 cạnh upstream, 22 cạnh downstream, 171 cặp column-level.
  - `gold.quality_hourly_probe` → 2 node Superset do connector tạo.

## Thông tin chất lượng

- Kết quả kiểm tra chất lượng nằm ở `ops.dq_result`; trạng thái công bố nằm ở `ops.partition_status`. Cả hai đều là bảng được ingest vào OM, có mô tả.
- **Chưa** đẩy kết quả GX thành test case của OM. Việc này cần tích hợp `openmetadata-ingestion[great-expectations]` vào checkpoint của GX.
