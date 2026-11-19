# Trạng thái đồ án (cập nhật mỗi phiên)

## Phiên 2026-11-18, nhánh `feat/design-and-api-survey`

### Đã có từ trước (commit trên `main`)

- SeaweedFS (master, volume, filer, s3) có 4 identity S3 phân quyền, bucket `staging` và `warehouse`.
- Nessie 0.108.8 lưu trên Postgres 16; Spark chạy bằng image `tabulario/spark-iceberg`.
- dlt ingest metadata measurement/probe và ping results theo giờ UTC+7 cho 200 probe cố định.

### Chưa commit (công việc của chủ đồ án, phiên này KHÔNG sửa)

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
- Chưa kiểm chứng Trino ↔ Nessie, GX trong image Spark, OpenMetadata lineage.
- Chưa có code mới cho giai đoạn 1–5.

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

### Bước tiếp theo

Giai đoạn 1:

- Pin image theo bảng compatibility.
- Tách Compose profiles.
- Thêm Trino, Superset và smoke test Spark → Nessie → Trino.
