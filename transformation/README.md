# transformation/: Spark + Iceberg, staging → Bronze → Silver → Gold

Mọi job chạy trong container `spark-iceberg`. Image build từ `Dockerfile` ở thư mục này (jar S3A được pin SHA-256).
Thư mục này được mount vào `/opt/app/transformation`, và `PYTHONPATH` của image có sẵn `lakehouse` (thư mục này) cùng `dq` (`quality/`).

```bash
docker compose exec -T spark-iceberg spark-submit /opt/app/transformation/jobs/<job>.py --help
```

## Job (`jobs/`), theo thứ tự chạy

| Job | Đầu vào → đầu ra | Ghi chú |
|---|---|---|
| `bronze_ingest.py` | staging (manifest `complete`) → `bronze.*` | MERGE theo `record_uid`; bản ghi hỏng đưa vào `bronze.quarantine` |
| `silver_build.py` | `bronze.*` → `silver.*` (star schema, SCD2) | Kiểm tra chất lượng **trước** khi ghi; công bố theo partition |
| `gold_build.py` | Silver **đã công bố** → `gold.*` marts | Chỉ lưu tử số và mẫu số cộng được; có histogram RTT |
| `gold_crosscheck.py` | Silver ↔ Gold | Kiểm chứng độc lập, không dùng `lakehouse.gold` |
| `iceberg_maintenance.py` | `rewrite_data_files` | Gộp file nhỏ cho các ngày đã đóng |

## Thư viện (`lakehouse/`): hàm thuần trên DataFrame, test được mà không cần S3

| Module | Vai trò |
|---|---|
| `common.py` | `SQL_DIR`, chạy file DDL, log JSON, run id, parse ngày và giờ |
| `bronze.py` | Schema staging, tách bản ghi hợp lệ / quarantine, kiểm tra toàn vẹn batch |
| `silver.py` | SCD2 (`build_scd2`), fact result và reply, as-of join, `dim_date` / `dim_time` |
| `gold.py` | Mart chất lượng theo target / country / ASN / probe, histogram, ghép cặp IPv4/IPv6, coverage |
| `publication.py` | Giao thức công bố: `ops.partition_status` + snapshot đã pin |
| `metrics.py` | Đẩy metric job lên Pushgateway |

## DDL (`sql/`)

`bronze.sql`, `silver.sql`, `ops.sql`: job tự chạy `CREATE … IF NOT EXISTS` lúc khởi động.

## Khác

- `spark-defaults.conf`: catalog Nessie, S3A, bộ nhớ driver (Compose và CI mount vào `/opt/spark/conf/`).
- `legacy/`: job thế hệ đầu, chỉ giữ để tham khảo (xem `legacy/README.md`).

Đặc tả: `docs/silver-model.md`, `docs/metric-dictionary.md`. Test: `tests/transformation/`.
