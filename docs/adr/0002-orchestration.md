# ADR-0002: Điều phối bằng Airflow standalone + Docker SDK

- Trạng thái: **Đã chấp nhận (2026-11-19)**. Chủ đồ án chọn Airflow ở profile `orchestration` riêng.

## Bối cảnh

- Pipeline cần chạy hằng giờ, có retry, timeout, backfill, pool chống ghi đồng thời và lịch sử chạy.
- Docker được cấp 8,2 GB RAM. Core + analytics + monitoring đã dùng khoảng 4,4 GB.

## Quyết định

- **Image:** `apache/airflow:3.4.0-python3.11` (pin digest), chạy `airflow standalone` với LocalExecutor và metadata trên Postgres riêng (`airflow-db`).
  - Image đã có sẵn `apache-airflow-providers-docker` 4.6.1 và `docker` SDK 7.2.0, nên **không cần build image riêng**.
- **Cách chạy job:**
  - Ingestion: container mới từ image `ripe-atlas-ingestion` (code và probe set nằm sẵn trong image).
  - Spark: `docker exec spark-submit` trong container `spark-iceberg` đang chạy.
  - Cả hai đều có `on_kill`, nên task bị timeout hoặc hủy thì tiến trình thật cũng dừng.
- **Pool:** `lakehouse_writer` (1 slot) cho mọi task ghi Iceberg; `atlas_api` (1 slot) cho task gọi API.

## Hệ quả

- **RAM:** container Airflow ~150 MB lúc rảnh, giới hạn 2 GB; Postgres ~50 MB. Số đo khi chạy DAG ở `docs/test-report.md`.
- **Bảo mật:** Airflow mount `docker.sock`, tương đương quyền root trên Docker host. Chỉ chấp nhận cho môi trường đồ án local. Khi triển khai thật, thay bằng Spark cluster/Spark Connect và KubernetesPodOperator (hoặc tương đương), bỏ mount socket.
- **Phương án nhẹ hơn đã cân nhắc:** CLI + cron. Ít RAM hơn, nhưng mất UI, lịch sử, retry và pool có sẵn.
