# Compatibility matrix

> **Cập nhật 2026-11-21:** Spark, GX Core và OpenMetadata đã bị bỏ (ADR-0003). Các dòng về chúng bên dưới là lịch sử.
> Phiên bản hiện dùng cho tầng biến đổi:
>
> | Thành phần | Phiên bản | Trạng thái |
> |---|---|---|
> | dbt-core / dbt-trino / trino (Python client) | 1.12.7 / 1.12.0 / 0.340.0 (uv.lock, nhóm `transform`) | **Đã kiểm chứng** trên dữ liệu thật, `docs/test-report.md` §6 |
> | Trino | 484: Iceberg connector (`catalog.type=nessie`) ghi + đọc, Hive connector (metastore file) đọc `.jsonl.gz` staging, `register_partition`, `MERGE`, `FOR VERSION AS OF`, phân quyền file | **Đã kiểm chứng** |
> | Image transform | `python:3.11-slim` (pin digest) + uv, 762 MB | Đã build và chạy |

Cập nhật: 2026-11-19 (giai đoạn 1).

Quy ước trạng thái:
- **Đã kiểm chứng**: có lệnh và kết quả chạy thật, ghi ở cột bằng chứng.
- **Đã đo**: đọc phiên bản trực tiếp trong container.
- **Chưa kiểm chứng**: chưa chạy thử.

## Đang chạy trong Compose

Tất cả image đều được pin theo digest.

| Thành phần | Phiên bản | Image / digest | Trạng thái | Bằng chứng |
|---|---|---|---|---|
| Spark | 3.5.5, Scala 2.12.18, OpenJDK 17.0.14, Python 3.10.16, PySpark 3.5.5 | Image `ripe-atlas-spark:3.5.5-iceberg1.8.1`, build từ `tabulario/spark-iceberg@sha256:790a72e7…` + 11 jar S3A (pin SHA-256) + pytest 8.3.5 | Đã kiểm chứng | `spark-submit --version`; job đọc s3a mà không tải Maven (`/root/.ivy2/jars` rỗng) |
| GX Core | 1.25.0 | Cài trong image Spark, pandas/numpy/pyarrow giữ nguyên | **Đã kiểm chứng** | Checkpoint + Data Docs; test phát hiện lỗi critical |
| Ingestion | Python 3.11.18, uv 0.11.26, dlt 1.32.0, requests 2.34.2, s3fs/fsspec 2026.9.0, pytest 8.3.5 | `ripe-atlas-ingestion:0.2`, build từ `python:3.11-slim@sha256:07d48240…` | Đã kiểm chứng | 41 unit test pass trong container; ingest thật 24 job (measurement × giờ) vào SeaweedFS |
| Iceberg (phía Spark) | `iceberg-spark-runtime-3.5_2.12-1.8.1`, `iceberg-aws-bundle-1.8.1` | có sẵn trong image Spark | Đã đo | Liệt kê `/opt/spark/jars` |
| hadoop-aws | 3.3.4 + aws-java-sdk 1.12.262 (tải lúc chạy qua `spark.jars.packages`) | — | Chạy được, **dễ lỗi** | Xem mục "Rủi ro" |
| Nessie | 0.108.8, version store JDBC2 trên Postgres 16 | `ghcr.io/projectnessie/nessie:0.108.8@sha256:aee5e959…` | Đã kiểm chứng | Spark và Trino cùng đọc ref `main` |
| Postgres | 16-alpine | `postgres@sha256:aa6cb33c…` | Đang chạy | Dùng cho Nessie và Superset, mỗi service một container riêng |
| SeaweedFS | 4.53 | `chrislusf/seaweedfs@sha256:ccad23e1…` | Đã đo | Lệnh `weed version` |
| Trino | 484 | `trinodb/trino:484@sha256:2704eb53…`, `mem_limit` 3 GB, giới hạn truy vấn trong `serving/trino/config.properties` | **Đã kiểm chứng** | Xem bên dưới; `approx_percentile(x, w, p)` có trọng số dùng cho p95 |
| Superset | 6.1.0 (Python 3.10.21) | `apache/superset:6.1.0@sha256:16b50bbe…` + `psycopg2-binary==2.9.10`, `trino[sqlalchemy]==0.333.0` | **Đã kiểm chứng** | Xem bên dưới |

### Bằng chứng cho Trino 484

- Connector `iceberg.catalog.type=nessie` vẫn được hỗ trợ.
- Cùng catalog, Trino thấy schema `bronze` và `silver` do Spark tạo.
- `tests/integration/smoke_compat.sh` đạt **PASS** (2026-11-19):

  | Kiểm tra | Spark | Trino |
  |---|---|---|
  | Snapshot id hiện tại của `nessie.smoke.compat_check` | 8666977384875732147 | 8666977384875732147 |
  | count / sum(decimal) / sum(cardinality(array)) | 3 / 30.750 / 3 | 3 / 30.750 / 3 |
  | `bronze.measurements` | 4 | 4 |
  | `bronze.ping_results` | 22.361 | 22.361 |
  | `bronze.probes` | 200 | 200 |
  | `silver.dim_measurement` | 4 | 4 |
  | `silver.dim_probe` | 200 | 200 |
  | `silver.fact_ping` | 22.361 | 22.361 |

- **Trino chỉ đọc**: `CREATE TABLE nessie.smoke.trino_write_probe AS SELECT 1` thất bại với lỗi `Error committing write to Parquet file`, vì identity `atlas-reader` không có quyền ghi. Không có bảng mới nào được đăng ký trong Nessie.

### Bằng chứng cho Superset 6.1.0

- Script `superset-init.sh` tạo admin và database `RIPE Atlas Lakehouse (Trino)` (backend `trino`).
- Gọi REST API `/api/v1/sqllab/execute/` với `SELECT count(*), count(DISTINCT probe_id) FROM nessie.silver.fact_ping` trả về `success`, kết quả 22.361 dòng và 200 probe, khớp với Spark.

### RAM thực tế

Đo bằng `docker stats` với profile core + analytics, khi hệ thống nhàn rỗi sau smoke test.

| Service | RAM |
|---|---|
| trino | 1,25 GB (giới hạn 2 GB) |
| spark-iceberg | 0,67 GB |
| nessie | 0,30 GB |
| superset | 0,22 GB (giới hạn 1,5 GB) |
| SeaweedFS (4 container) | 0,36 GB |
| 2 container Postgres | 0,08 GB |
| **Tổng** | **≈ 2,9 GB** |

## Chưa kiểm chứng (các giai đoạn sau)

| Thành phần | Phiên bản dự kiến | Điểm cần kiểm chứng |
|---|---|---|
| Airflow | 3.x | RAM của chế độ standalone; cách gọi `spark-submit` trong container Spark |
| OpenMetadata | 2.0.8 | Connector Trino và Superset; OpenLineage `openlineage-spark_2.12` ≥ 1.37.0 có map được dataset Iceberg/Nessie hay không |
| Prometheus / Pushgateway / Grafana | 3.16.0 / 1.11.4 / 13.3.1 | Cấu hình provisioning |

## Rủi ro đã xử lý

**`spark.jars.packages` tải jar từ Maven lúc chạy.**
- Ngày 2026-11-19, một lần tải bị `Connection refused`.
- Đã thay bằng image Spark nhúng sẵn jar, đặt ở `/opt/spark/extra-jars` và nạp qua `spark.jars`.
- Jar **không** được chép vào `/opt/spark/jars`, vì Jackson 2.12 sẽ xung đột với Jackson 2.15.2 của image.

**dlt filesystem destination.**
- Dữ liệu nguồn được giữ thành chuỗi `raw_json`, nên cơ chế normalize của dlt không làm mất cấu trúc lồng nhau.
- Đã kiểm chứng: đọc lại object thì `sha256(raw_json)` trùng `raw_sha256` (`test_window_stages_exact_bytes_and_writes_manifest`).
- `extra_placeholders` của dlt 1.32 dùng được cho layout `msm_id=`, `window_date=`, `window_hour=`.

## Giai đoạn 5 (đã kiểm chứng 2026-11-19)

| Thành phần | Phiên bản | Bằng chứng |
|---|---|---|
| Airflow | 3.4.0-python3.11 (`sha256:be92e15d…`), providers docker 4.6.1, standard 1.21.0, SDK docker 7.2.0 | DAG `ripe_atlas_hourly` chạy thành công trọn chuỗi |
| Prometheus / Pushgateway / Grafana / node-exporter | 3.16.0 / 1.11.4 / 13.3.1 / 1.10.2 | Mọi target `up`; 8 rule cảnh báo đã thử |
| prometheus-client | 0.26.0 | Metric job lên Pushgateway |
| OpenMetadata server / ingestion | 2.0.8 (`sha256:df3c3fef…`) / `openmetadata-ingestion[trino,superset]==2.0.8.0` | Connector Trino và Superset đạt 100% |
| OpenSearch | 2.19.2 (`sha256:69588c66…`) | Backend tìm kiếm của OM |
| ruff | 0.17.5 | Lint sạch |
| cAdvisor | v0.52.1, v0.53.0 | **Không tương thích** với Docker Desktop 29 + containerd snapshotter ("failed to identify the read-write layer"); đã bỏ |

**Lưu ý tương thích đã gặp:**
- OM 2.0.8: JWT của bot lấy qua `/users/auth-mechanism/{id}`; field `authenticationMechanism` không còn.
- Airflow 3: `[api] base_url` phải là cổng **trong** container.
- Trino `/metrics` cần header `X-Trino-User`.
