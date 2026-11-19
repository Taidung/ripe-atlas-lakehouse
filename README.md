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

Profile `jobs` chứa container ingestion, dùng qua `docker compose --profile jobs run --rm ingestion ...`. Các profile `orchestration`, `governance` và `monitoring` sẽ được thêm ở các giai đoạn sau.

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

## 4. Ingestion → staging → Bronze

Job ingestion chạy trong container `ingestion` (profile `jobs`). Phạm vi probe chọn bằng biến `PROBE_SET_VERSION`:
- `dev-200-v1` (mặc định): 200 probe cố định.
- `strat-k2-c100-v1`: mẫu phân tầng 4.324 probe IPv4 và 2.897 probe IPv6 (ADR-0001).

Có thể đặt biến này trong `.env`, hoặc truyền `--probe-set` cho từng lệnh.

```bash
docker compose --profile jobs build ingestion

# Snapshot metadata measurement + probe (khởi tạo, sau đó hằng ngày)
docker compose --profile jobs run --rm ingestion metadata

# Một khoảng thời gian, chia theo giờ [start, end) UTC
docker compose --profile jobs run --rm ingestion results --start 2026-11-19T00:00:00Z --end 2026-11-19T02:00:00Z

# Lịch chạy hằng giờ: giờ vừa kết thúc + OVERLAP_HOURS (mặc định 3) giờ trước đó để thu dữ liệu đến muộn
docker compose --profile jobs run --rm ingestion incremental

# Backfill: cùng logic, run_type=backfill được ghi vào manifest
docker compose --profile jobs run --rm ingestion backfill --start 2026-11-18T00:00:00Z --end 2026-11-19T00:00:00Z

# Sinh probe set mới (bất biến; không ghi đè version đã có)
docker compose --profile jobs run --rm ingestion build-probe-set --version strat-k2-c100-v2 --k 2 --cap 100 --reference-end 2026-11-20T00:00:00Z
```

Chạy lại cùng cửa sổ là an toàn. Record trùng nội dung (cùng SHA-256) không bị stage lại; chỉ record mới hoặc bị nguồn sửa mới được ghi.

Staging → Bronze. Job chỉ đọc object có trong manifest hoàn tất, và bỏ qua manifest đã nạp:

```bash
docker compose exec -T spark-iceberg spark-submit /opt/app/spark/jobs/bronze_ingest.py --entity metadata
docker compose exec -T spark-iceberg spark-submit /opt/app/spark/jobs/bronze_ingest.py --entity ping_result_raw --start 2026-11-19T00:00:00Z --end 2026-11-19T02:00:00Z

# Xử lý lại manifest đã nạp (idempotent: 0 dòng mới nếu dữ liệu không đổi)
docker compose exec -T spark-iceberg spark-submit /opt/app/spark/jobs/bronze_ingest.py --entity ping_result_raw --start 2026-11-19T00:00:00Z --end 2026-11-19T02:00:00Z --reprocess
```

Hợp đồng dữ liệu, quy tắc quarantine và cách phục hồi: `docs/data-contracts.md`.

### Bronze → Silver (star schema + kiểm tra chất lượng)

```bash
# Tính lại các ngày (giờ VN) chứa khoảng [start, end). Chạy lại an toàn: kết quả giống hệt
docker compose exec -T spark-iceberg spark-submit /opt/app/spark/jobs/silver_build.py --start 2026-11-19T00:00:00Z --end 2026-11-19T03:00:00Z

# Tính lại các ngày chỉ định
docker compose exec -T spark-iceberg spark-submit /opt/app/spark/jobs/silver_build.py --dates 2026-11-19

# Metadata probe đến muộn: tính lại các ngày có fact probe_sk = -1 nay đã có metadata
docker compose exec -T spark-iceberg spark-submit /opt/app/spark/jobs/silver_build.py --repair-unknown
```

Mã thoát `2` nghĩa là có ngày bị chặn vì lỗi chất lượng critical. Khi đó partition giữ bản đã công bố trước đó.

Xem trạng thái công bố và kết quả kiểm tra:

```bash
docker compose exec trino trino --execute "SELECT table_name, ictrl_dt, measurement_id, status, row_count, reason FROM nessie.ops.partition_status ORDER BY 1,2,3"
docker compose exec trino trino --execute "SELECT checked_at, table_name, severity, check_name, observed_value FROM nessie.ops.dq_result WHERE NOT success ORDER BY checked_at DESC LIMIT 20"
```

Data Docs của GX: mở file `artifacts/gx/gx/uncommitted/data_docs/local_site/index.html` trên máy host.

Mô hình, data dictionary và quy tắc chuẩn hóa: `docs/silver-model.md`.

Integration test chặn công bố, chạy trên nhánh Nessie tạm, không làm bẩn `main`:

```bash
bash scripts/test_dq_block_on_branch.sh 2026-11-19
```

### Test

```bash
# Unit test offline (không cần mạng hay Docker; dlt ghi thật vào thư mục tạm)
docker compose --profile jobs run --rm --entrypoint python ingestion -m pytest -q tests/unit

# Test biến đổi Bronze trên Spark local trong container
docker compose exec -T -w /opt/app spark-iceberg python -m pytest -q -p no:cacheprovider tests/spark
```

> Trên Git Bash (Windows), thêm `MSYS_NO_PATHCONV=1` trước các lệnh `docker compose exec` có đường dẫn `/opt/...`. Nếu không, Git Bash tự đổi đường dẫn thành `C:/Program Files/Git/opt/...`.

### Pipeline cũ (legacy, giữ để đối chiếu)

`spark/jobs/staging_to_bronze.py` và `spark/jobs/bronze_to_silver.py` (SCD1) đọc `bronze.ping_results`, ghi `silver.legacy_*`. Đã được thay bằng `bronze_ingest.py` và `silver_build.py`.

```bash
docker compose exec -T spark-iceberg spark-submit /opt/app/spark/jobs/bronze_to_silver.py
docker compose exec -T -w /opt/app spark-iceberg spark-submit tests/test_bronze.py
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
docker compose exec trino trino --execute "SELECT measurement_id, count(*), count(DISTINCT probe_id) FROM nessie.bronze.ping_result_raw GROUP BY 1"
```
