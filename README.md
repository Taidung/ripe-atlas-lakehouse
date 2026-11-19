# RIPE Atlas Lakehouse

Batch pipeline phân tích độ trễ và mất gói từ RIPE Atlas public probes tới K-root và F-root, qua 4 built-in measurements:

| measurement_id | Address family | Target |
|---|---|---|
| 1001 | IPv4 | k.root-servers.net |
| 1004 | IPv4 | f.root-servers.net |
| 2001 | IPv6 | k.root-servers.net |
| 2004 | IPv6 | f.root-servers.net |

Kiến trúc: dlt → SeaweedFS (S3) → Spark/Iceberg (Nessie catalog) → Trino → Superset.

Tài liệu:
- Thiết kế: `docs/design.md`
- Khảo sát API: `docs/api-survey.md`
- Phiên bản đã kiểm chứng: `docs/compatibility.md`
- Trạng thái và bước tiếp theo: `docs/status.md`

## 1. Chuẩn bị

Cần Docker Desktop (đã kiểm thử với Docker 29.6.2, Compose v5.3.1), Git Bash và [uv](https://docs.astral.sh/uv/) cho phần ingestion.

```bash
cp .env.example .env   # rồi thay mọi giá trị change-me
```

`ingestion/.dlt/secrets.toml` chứa credentials ghi vào staging (identity `atlas-ingest`). File này và `.env` đều nằm trong `.gitignore`.

## 2. Khởi động theo profile

| Lệnh | Service | RAM đo được |
|---|---|---|
| `docker compose up -d` | core: SeaweedFS, Nessie + Postgres, Spark | ≈ 1,4 GB |
| `docker compose --profile analytics up -d` | core + Trino + Superset | ≈ 2,9 GB |
| `docker compose --profile all up -d` | toàn bộ service đã khai báo | — |

Các profile `orchestration`, `governance` và `monitoring` sẽ được thêm ở các giai đoạn sau.

Cổng truy cập:

| Service | Địa chỉ |
|---|---|
| Superset | http://localhost:8088 (user `admin`, mật khẩu `SUPERSET_ADMIN_PASSWORD` trong `.env`) |
| Trino | http://localhost:8081 |
| Nessie | http://localhost:19120 |
| Spark UI | http://localhost:4040 |
| SeaweedFS S3 | http://localhost:8333 |

Dừng stack, **giữ dữ liệu**:

```bash
docker compose --profile all down
```

> ⚠️ Lệnh `docker compose --profile all down -v` **xoá toàn bộ volume**: dữ liệu SeaweedFS (staging + warehouse), lịch sử Nessie và metadata Superset. Không thể khôi phục.

## 3. Kiểm tra tương thích Spark → Nessie → Trino

```bash
docker compose --profile analytics up -d
bash scripts/smoke_compat.sh
```

Script kiểm tra 3 điều:
- Bảng do Spark ghi có cùng snapshot id và cùng số liệu khi đọc từ Trino.
- Số dòng mọi bảng `bronze` và `silver` khớp giữa Spark và Trino.
- In `PASS` khi tất cả khớp.

## 4. Ingestion và xử lý (code hiện có, sẽ refactor ở giai đoạn 2–3)

Metadata measurement và probe → staging:

```bash
uv run python -m ingestion.ingestion
```

Ping results cho một giờ đã kết thúc (giờ UTC+7) → staging:

```bash
uv run python -m ingestion.ingestion --ping-results --window-start 2026-11-18T13:00:00+07:00
```

Staging → Bronze. Chạy lại cùng giờ sẽ thay partition của giờ đó:

```bash
docker compose exec -T spark-iceberg spark-submit /opt/spark/jobs/staging_to_bronze.py
docker compose exec -T spark-iceberg spark-submit /opt/spark/jobs/staging_to_bronze.py --window-start 2026-11-18T13:00:00+07:00
```

Bronze → Silver. Chạy dimension trước, sau đó từng giờ:

```bash
docker compose exec -T spark-iceberg spark-submit /opt/spark/jobs/bronze_to_silver.py
docker compose exec -T spark-iceberg spark-submit /opt/spark/jobs/bronze_to_silver.py --window-start 2026-11-18T13:00:00+07:00
```

Unit test Spark (không ghi S3 hay Iceberg):

```bash
docker compose exec -T spark-iceberg spark-submit /opt/spark/tests/test_bronze.py
docker compose exec -T spark-iceberg spark-submit /opt/spark/tests/test_silver.py
```

## 5. Truy vấn bằng SQL client qua Trino

Thông số kết nối, dùng được với DBeaver, DataGrip hay Trino CLI:

| Thông số | Giá trị |
|---|---|
| Host | `localhost` |
| Port | `8081` |
| Catalog | `nessie` |
| User | tên bất kỳ (môi trường đồ án chưa bật xác thực Trino) |

Trino dùng credentials S3 chỉ đọc nên **không ghi được** dữ liệu.

```bash
docker compose exec trino trino --execute "SELECT measurement_id, count(*) FROM nessie.silver.fact_ping GROUP BY 1"
```
