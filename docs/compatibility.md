# Compatibility matrix

Cập nhật: 2026-11-19 (giai đoạn 1).

Quy ước trạng thái:
- **Đã kiểm chứng**: có lệnh và kết quả chạy thật, ghi ở cột bằng chứng.
- **Đã đo**: đọc phiên bản trực tiếp trong container.
- **Chưa kiểm chứng**: chưa chạy thử.

## Đang chạy trong Compose

Tất cả image đều được pin theo digest.

| Thành phần | Phiên bản | Image / digest | Trạng thái | Bằng chứng |
|---|---|---|---|---|
| Spark | 3.5.5, Scala 2.12.18, OpenJDK 17.0.14, Python 3.10.16, PySpark 3.5.5 | `tabulario/spark-iceberg@sha256:790a72e7…` | Đã đo | `spark-submit --version` trong container |
| Iceberg (phía Spark) | `iceberg-spark-runtime-3.5_2.12-1.8.1`, `iceberg-aws-bundle-1.8.1` | có sẵn trong image Spark | Đã đo | Liệt kê `/opt/spark/jars` |
| hadoop-aws | 3.3.4 + aws-java-sdk 1.12.262 (tải lúc chạy qua `spark.jars.packages`) | — | Chạy được, **dễ lỗi** | Xem mục "Rủi ro" |
| Nessie | 0.108.8, version store JDBC2 trên Postgres 16 | `ghcr.io/projectnessie/nessie:0.108.8@sha256:aee5e959…` | Đã kiểm chứng | Spark và Trino cùng đọc ref `main` |
| Postgres | 16-alpine | `postgres@sha256:aa6cb33c…` | Đang chạy | Dùng cho Nessie và Superset, mỗi service một container riêng |
| SeaweedFS | 4.53 | `chrislusf/seaweedfs@sha256:ccad23e1…` | Đã đo | Lệnh `weed version` |
| Trino | 484 | `trinodb/trino:484@sha256:2704eb53…` | **Đã kiểm chứng** | Xem bên dưới |
| Superset | 6.1.0 (Python 3.10.21) | `apache/superset:6.1.0@sha256:16b50bbe…` + `psycopg2-binary==2.9.10`, `trino[sqlalchemy]==0.333.0` | **Đã kiểm chứng** | Xem bên dưới |

### Bằng chứng cho Trino 484

- Connector `iceberg.catalog.type=nessie` vẫn được hỗ trợ.
- Cùng catalog, Trino thấy schema `bronze` và `silver` do Spark tạo.
- `scripts/smoke_compat.sh` đạt **PASS** (2026-11-19):

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
| dlt | ≥ 1.32 (uv.lock) | Pin chính xác; kiểm tra filesystem destination ghi vào SeaweedFS với `max_table_nesting=0` giữ nguyên cấu trúc nested |
| GX Core | 1.x | Cài được trong image Spark (Python 3.10, PySpark 3.5.5) |
| Airflow | 3.x | RAM của chế độ standalone; cách gọi `spark-submit` trong container Spark |
| OpenMetadata | 2.0.8 | Connector Trino và Superset; OpenLineage `openlineage-spark_2.12` ≥ 1.37.0 có map được dataset Iceberg/Nessie hay không |
| Prometheus / Pushgateway / Grafana | 3.16.0 / 1.11.4 / 13.3.1 | Cấu hình provisioning |

## Rủi ro đã biết

**`spark.jars.packages` tải hadoop-aws và aws-sdk từ Maven Central mỗi khi container Spark được tạo lại.**

- Ngày 2026-11-19, một lần tải bị `Connection refused` và job đầu tiên thất bại; lần chạy lại thì đạt.
- **Cách xử lý dự kiến:** build image Spark riêng, nhúng sẵn các jar vào image, rồi bỏ `spark.jars.packages`. Đổi lại phải build image, nhưng job sẽ chạy được offline.
