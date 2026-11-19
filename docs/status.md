# Trạng thái đồ án (cập nhật mỗi phiên)

## Phiên 2026-11-18, nhánh `feat/design-and-api-survey`

### Đã có từ trước (commit trên `main`)

- SeaweedFS (master, volume, filer, s3) có 4 identity S3 phân quyền, bucket `staging` và `warehouse`.
- Nessie 0.108.8 lưu trên Postgres 16; Spark chạy bằng image `tabulario/spark-iceberg`.
- dlt ingest metadata measurement/probe và ping results theo giờ UTC+7 cho 200 probe cố định.

### Chưa commit lúc bắt đầu phiên (công việc của chủ đồ án; sau đó đã commit nguyên trạng ở `3cf93c5`)

- `spark/staging_to_bronze.py`, `spark/bronze_to_silver.py`
- `tests/test_bronze.py`, `tests/test_silver.py`
- Thay đổi trong `README.md`, `docker-compose.yaml`, `spark/spark-defaults.conf`

### Làm trong phiên này (có bằng chứng)

- Đo tài nguyên máy và Docker → `docs/design.md` §4.
- Khảo sát API bằng request thật: biên `start`/`stop` inclusive, results không phân trang, ngữ nghĩa `sent`/`rcvd`/`dup`/`-1`, khối lượng 5 phút → `docs/api-survey.md`.
- Đo phiên bản Spark/Scala/Java/Iceberg/SeaweedFS trong container → `docs/design.md` §5.
- Lưu 10 bản ghi thật làm fixture → `tests/fixtures/ping_results_real_sample.jsonl`.
- Viết bản nháp thiết kế, giả định và lộ trình → `docs/design.md`; tài liệu tham khảo → `docs/references.md`.

### Chưa làm hoặc chưa kiểm chứng

- Chưa đo khối lượng 24h và phân phối late arrival; chưa kiểm tra tính duy nhất của business key trên 24h.
- Chưa kiểm chứng GX trong image Spark và OpenMetadata lineage. (Trino ↔ Nessie đã kiểm chứng ở phiên 2026-11-19.)

### Quyết định của chủ đồ án (2026-11-19)

1. Chế độ `analysis` dùng **mẫu phân tầng** (country × ASN × af). Xem ADR-0001.
   - Đã đếm tầng thật.
   - **Đã duyệt k = 2, trần 100 probe/quốc gia** ≈ 4.290 probe v4 và 2.869 probe v6 (`strat-k2-c100-v1`).
2. Đĩa: chủ đồ án chọn **xóa Docker build cache** (`docker builder prune -a -f`, ngày 2026-11-19).
   - Giải phóng 6,485 GB bên trong ổ ảo WSL của Docker.
   - Ổ C: của Windows vẫn báo trống 13,2 GB, vì file `.vhdx` không tự co lại.
   - Chỗ trống khả dụng cho Docker ≈ 6,5 GB (bên trong vhdx) + 13,2 GB (C:) ≈ 19,7 GB.
   - Chuyển Docker sang D: vẫn là phương án dự phòng khi dữ liệu tăng.
3. Phần Bronze/Silver chưa commit: **đã commit nguyên trạng** (`3cf93c5`) trước khi refactor.
4. Điều phối: **Airflow standalone** (LocalExecutor), đặt ở profile `orchestration` riêng.

## Phiên 2026-11-19: Giai đoạn 1 (hạ tầng core và compatibility)

### Đã làm, có bằng chứng (chi tiết ở `docs/compatibility.md`)

- **Pin image theo digest:** Spark-Iceberg, SeaweedFS 4.53, Nessie 0.108.8, Postgres 16, aws-cli, Trino 484, Superset 6.1.0.
- **Compose profiles:** không chọn profile thì chạy core; profile `analytics` thêm Trino, Superset và `superset-db`; profile `all` chạy tất cả.
- **Trino 484:**
  - Catalog `nessie`, cấu hình `iceberg.catalog.type=nessie`.
  - Dùng credentials S3 chỉ đọc; đã thử ghi và bị từ chối.
- **Superset 6.1.0:**
  - Image build thêm driver `trino` và `psycopg2`.
  - Script khởi tạo idempotent: tạo admin và kết nối Trino.
  - Truy vấn SQL Lab qua REST API trả về đúng 22.361 dòng.
- **`scripts/smoke_compat.sh` đạt PASS:**
  - Cùng snapshot id giữa Spark và Trino.
  - 6 bảng bronze/silver có số dòng khớp nhau.
- **RAM core + analytics:** ≈ 2,9 GB.
- **Tài liệu:** thêm `.env.example`, viết lại README bằng tiếng Việt, thêm `.gitattributes` để giữ LF cho file `.sh` và `.properties`.

### Sự cố trong phiên

- Docker Desktop không khởi động được vì socket cũ `%LOCALAPPDATA%\Docker\run\dockerInference` bị kẹt.
- Chủ đồ án đã xử lý. Volume dữ liệu còn nguyên.

### Còn thiếu hoặc rủi ro (cập nhật cuối giai đoạn 1)

- Đã xử lý ở giai đoạn 2: `spark.jars.packages` tải jar từ Maven mỗi lần chạy.
- Biến tên bucket (`STAGING_BUCKET`, `WAREHOUSE_BUCKET`) mới chỉ có phía ingestion; tên bucket vẫn đang hardcode trong `s3.json` và `spark-defaults.conf`.
- `.venv` hiện có được tạo từ WSL. Ingestion nay chạy trong container nên không còn phụ thuộc `.venv`.

## Phiên 2026-11-19: Giai đoạn 2 (ingestion, staging, Bronze)

### Đã làm, có bằng chứng

- **Package `ingestion/`** (thay `ingestion/ingestion.py`; file cũ vẫn còn trong lịch sử git):
  - HTTP client có retry giới hạn, exponential backoff full jitter và Retry-After; phân loại 429, 5xx và 4xx; giới hạn kích thước response.
  - Cửa sổ `[start, end)` gọi API với `stop = end - 1` (biên inclusive đã đo).
  - Chia probe theo nhóm 500, gọi song song tối đa 2 request.
  - Hash bytes nguồn, hash index để khử trùng khi tải chồng lấn, manifest làm commit marker.
  - Lệnh `metadata`, `results`, `incremental`, `backfill`, `build-probe-set`.
- **Probe set:**
  - `dev-200-v1`: chuyển từ code sang `config/probe_sets/`.
  - `strat-k2-c100-v1`: sinh từ API thật, 4.324 probe v4 / 2.897 probe v6, 187/150 quốc gia, 2.596 probe nằm ở cả hai.
- **Image:**
  - `ingestion`: Python 3.11, dependency pin theo `uv.lock`, chạy ở profile `jobs`.
  - Spark: nhúng sẵn jar S3A (pin SHA-256) và pytest.
- **Bronze:**
  - DDL `sql/bronze/001_tables.sql` gồm 5 bảng mới.
  - Job `spark/jobs/bronze_ingest.py`: MERGE theo `record_uid`, quarantine, audit `ingest_manifest` append-only.
- **Test:** 41 unit test offline (gồm test dlt ghi thật vào thư mục tạm) và 15 test Spark. Tất cả pass, cả trên host Windows lẫn trong container.
- **Chạy thật trên stack** (dev-200-v1, 22:00Z 18/11 – 02:00Z 19/11):
  - 24 job ingestion, 0 lỗi; 44.340 result.
  - Staging khớp Bronze theo từng measurement.
  - Business key `(measurement_id, probe_id, event_ts, af)` duy nhất trên 44.340 dòng.
  - 0 bản ghi quarantine.
  - Chạy lại Bronze cho kết quả `nothing_pending`; `--reprocess` cho 0 dòng mới.
  - Overlap 3 giờ: giờ 00–01 tải lại có `staged = 0`.
- **Phát hiện mới, đã ghi vào `docs/api-survey.md`:**
  - `public_only` chỉ có tác dụng khi request có `probe_ids`.
  - Độ trễ lưu của nguồn có p99 497 s, max 8.120 s.

### Sửa trong phiên

- **Audit `ingest_manifest`:** trước đây `--reprocess` ghi đè dòng audit của lần nạp đầu; nay là append-only. Dòng audit của lần nạp đầu tiên (`bronze-20261119T024855Z-33aad06c`) đã bị ghi đè trước khi sửa, dữ liệu Bronze không bị ảnh hưởng.
- **Spark image:** `PYTHONPATH` trong Dockerfile từng che mất pyspark của image gốc; đã sửa để nối thêm thay vì thay thế.

### Còn thiếu hoặc rủi ro

- **Chưa chạy thật với `strat-k2-c100-v1`** (≈ 4.600 probe). Volume/giờ và runtime của chế độ analysis chưa đo.
- **Compaction, expire snapshot, dọn object mồ côi và retention staging:** mới là kế hoạch (`docs/data-contracts.md` §5).
- **Lock giữa các run cùng cửa sổ:** chưa có. Trùng lặp được khử ở Bronze; Airflow sẽ giới hạn `max_active_runs`.
- **Late data quá 3 giờ:** max đo được 2,25 giờ. Reconciliation định kỳ bằng `backfill` cần được lên lịch (giai đoạn 5 với Airflow).
- **Bảng legacy** (`bronze.ping_results`, `silver.fact_ping`, …) vẫn còn; cần chủ đồ án đồng ý mới xóa.

### Bước tiếp theo: giai đoạn 3 (Silver + GX)

1. ERD và data dictionary; `fact_ping_result`, `fact_ping_reply`, `dim_probe`/`dim_measurement` SCD2, `dim_date`/`dim_time`, dimension geography/ASN.
2. Chọn bản ghi khi cùng business key mà payload khác; xử lý RTT `-1`, `sent = 0`, duplicate reply.
3. GX Core: suite cho Bronze/Silver, chặn publish khi có lỗi critical.
4. Đo chế độ analysis trên 1–2 giờ dữ liệu thật.
