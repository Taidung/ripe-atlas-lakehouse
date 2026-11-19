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

### Còn thiếu hoặc rủi ro

- `spark.jars.packages` tải jar từ Maven mỗi khi container Spark được tạo lại; đã có một lần bị `Connection refused`. Sẽ build image Spark có sẵn jar.
- Biến tên bucket (`STAGING_BUCKET`, `WAREHOUSE_BUCKET`) chưa đưa vào cấu hình; tên bucket vẫn đang hardcode trong `s3.json` và `spark-defaults.conf`.
- `.venv` hiện có được tạo từ WSL nên `uv` trên Windows không dùng lại được. Lệnh trong README chạy từ WSL hoặc Git Bash; trên Windows cần đặt biến `UV_PROJECT_ENVIRONMENT` riêng.

### Bước tiếp theo

Giai đoạn 2 (ingestion và staging/Bronze):

1. dlt resources: retry/backoff có jitter, xử lý 429/`Retry-After`, giới hạn concurrency.
2. Manifest cho mỗi cửa sổ và checkpoint theo (measurement, cửa sổ).
3. Lựa chọn probe theo `probe_set_version`; sinh tập mẫu `strat-k2-c100-v1`.
4. Bronze append có `observed_at`, cùng bảng audit/manifest và bảng quarantine.
5. Chuyển test sang pytest, dùng fixture thật.
6. Build image Spark có sẵn jar.
