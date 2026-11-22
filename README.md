# RIPE Atlas Lakehouse

Batch pipeline phân tích độ trễ và mất gói từ RIPE Atlas public probes tới K-root và F-root, qua 4 built-in measurements:

| measurement_id | Address family | Target |
|---|---|---|
| 1001 | IPv4 | k.root-servers.net |
| 1004 | IPv4 | f.root-servers.net |
| 2001 | IPv6 | k.root-servers.net |
| 2004 | IPv6 | f.root-servers.net |

Kiến trúc: dlt → SeaweedFS (S3) → Iceberg (Nessie catalog) ← dbt trên Trino (Bronze/Silver/Gold) → Superset.
Kiểm tra chất lượng bằng dbt test; điều phối bằng Airflow; giám sát bằng Prometheus/Pushgateway/Grafana.

Tài liệu:
- Thiết kế: `docs/design.md`
- Khảo sát API: `docs/api-survey.md`
- Phiên bản đã kiểm chứng: `docs/compatibility.md`
- Trạng thái và bước tiếp theo: `docs/status.md`

## Bản đồ mã nguồn

Mã nguồn chia theo thành phần của data platform, theo đúng chiều dữ liệu chảy qua.
Mỗi thư mục có `README.md` riêng, liệt kê từng file và vai trò của nó.

```text
RIPE Atlas API
   │  ingestion/        dlt + HTTP client: API -> jsonl.gz + manifest           (Python, container `ingestion`)
   ▼
storage/                SeaweedFS (bucket staging, warehouse) + Nessie catalog  (cấu hình S3: storage/seaweedfs/s3.json)
   │  transformation/   dbt trên Trino: staging -> Bronze -> Silver -> Gold     (model/test: dbt/, runner: lakehouse/, DDL: sql/)
   ▼
serving/                Trino (SQL) + Superset (dashboard dựng bằng code)
orchestration/          Airflow DAG: chạy container ingestion và transform theo giờ / backfill
observability/          Pushgateway + Prometheus + Grafana (dashboard và cảnh báo)
config/                 dùng chung: probe_sets/*.json, metrics.json (Superset và job crosscheck đọc cùng file)
tests/                  chia theo thành phần: ingestion/, transformation/, contract/, integration/
docs/                   thiết kế, data contract, runbook, báo cáo kiểm thử, ADR
```

Tìm nhanh:

| Muốn tìm | Mở |
|---|---|
| Gọi API, retry, phân trang | `ingestion/api_client.py` |
| Ghi staging, manifest, khử trùng | `ingestion/staging_store.py`, `ingestion/result_records.py` |
| Logic Bronze / Silver (SCD2, as-of join) / Gold | `transformation/dbt/models/{bronze,silver,gold}/` |
| Runner từng bước (chọn manifest, gọi dbt, công bố) | `transformation/lakehouse/{bronze,silver,gold}.py` |
| Giao thức công bố partition | `transformation/lakehouse/publication.py` |
| Rule chất lượng dữ liệu | `transformation/dbt/models/*/*.yml`, `transformation/dbt/tests/`, `transformation/dbt/macros/tests.sql` |
| Định nghĩa metric | `config/metrics.json`, `docs/metric-dictionary.md` |
| Dashboard Superset | `serving/superset/provision.py` |
| Lịch chạy | `orchestration/airflow/dags/ripe_atlas_dags.py` |
| Cảnh báo | `observability/grafana/provisioning/alerting/rules.yaml` |
| Service, cổng, volume | `docker-compose.yaml` |

## 1. Chuẩn bị

Cần Docker Desktop (đã kiểm thử với Docker 29.6.2, Compose v5.3.1), Git Bash và [uv](https://docs.astral.sh/uv/) cho phần ingestion.

```bash
cp .env.example .env   # rồi thay mọi giá trị change-me
```

Mọi khoá và mật khẩu chỉ nằm trong `.env`. Ví dụ, ingestion ghi vào staging bằng `S3_INGEST_ACCESS_KEY` / `S3_INGEST_SECRET_KEY`, tức danh tính `atlas-ingest` trong `storage/seaweedfs/s3.json`. Không cần tạo `ingestion/.dlt/secrets.toml`: code truyền credentials cho dlt trực tiếp từ biến môi trường. `.env` nằm trong `.gitignore`, **không bao giờ commit**; nếu lỡ commit thì đổi ngay các giá trị trong đó, vì xoá file khỏi lịch sử git không đủ.

`AIRFLOW_FERNET_KEY` phải là chuỗi base64 32 byte, có thể sinh bằng:

```bash
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

## 2. Khởi động theo profile

| Lệnh | Service | RAM đo được |
|---|---|---|
| `docker compose up -d` | core: SeaweedFS, Nessie + Postgres, Trino | ≈ 4,5 GB (Trino ≈ 2,9 GB, giới hạn 3 GB) |
| `docker compose --profile analytics up -d` | core + Superset | thêm Superset + Postgres của nó |
| `docker compose --profile all up -d` | toàn bộ service đã khai báo | — |

Profile `jobs` chứa container `ingestion` và `transform`, dùng qua `docker compose --profile jobs run --rm <service> ...`. Profile `orchestration` (Airflow) và `monitoring` (Prometheus/Grafana) ở mục 6–7. **Không đủ RAM để bật tất cả cùng lúc trên máy 16 GB**; xem `docs/test-report.md` §3.

Cổng truy cập:

| Service | Địa chỉ |
|---|---|
| Superset | http://localhost:8088 (user `admin`, mật khẩu `SUPERSET_ADMIN_PASSWORD` trong `.env`) |
| Trino | http://localhost:8081 |
| Nessie | http://localhost:19120 |
| SeaweedFS S3 | http://localhost:8333 |

Dừng stack, **giữ dữ liệu**:

```bash
docker compose --profile all down
```

> ⚠️ Lệnh `docker compose --profile all down -v` **xoá toàn bộ volume**: dữ liệu SeaweedFS (staging + warehouse), lịch sử Nessie và metadata Superset. Không thể khôi phục.

## 3. Trino: một engine cho biến đổi và truy vấn

| Catalog | Ai dùng | Quyền |
|---|---|---|
| `lakehouse` | dbt + runner (user `transform`) | Ghi Iceberg (nhánh Nessie `main`) |
| `staging` | dbt Bronze (user `transform`) | Đọc file `.jsonl.gz` trên staging (connector Hive, metastore dạng file) |
| `nessie` | Superset, DBeaver, mọi user khác | Chỉ đọc, khóa S3 `atlas-reader` |

Phân quyền theo user nằm ở `serving/trino/rules.json`. Trino không bật xác thực trong môi trường đồ án, nên đây là rào
chắn nhầm lẫn chứ không phải bảo mật.

## 4. Ingestion → staging → Bronze

Job ingestion chạy trong container `ingestion` (profile `jobs`). Phạm vi probe là probe set `dev-200-v2`
(`config/probe_sets/dev-200-v2.json`): 200 probe cố định, gồm **mọi probe Việt Nam đang hoạt động** (42 probe, 20 ASN:
Viettel, VNPT, FPT, CMC, MobiFone và các nhà cung cấp hosting) cùng probe ở các châu lục khác. Cách chọn ghi trong
trường `method` của file. Biến `PROBE_SET_VERSION` (mặc định `dev-200-v2`) hoặc `--probe-set` chọn version.

Tên nhà mạng theo ASN nằm ở seed `transformation/dbt/seeds/asn_info.csv` (tên đăng ký từ RIPEstat; tên ngắn và loại
ISP / Hosting/DC / Khác cho ASN Việt Nam). Đổi probe set, hoặc Gold hiện ASN chưa có tên, thì sinh lại seed (cần mạng):

```bash
python transformation/dbt/scripts/build_asn_info_seed.py
```

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
```

Chạy lại cùng cửa sổ là an toàn. Record trùng nội dung (cùng SHA-256) không bị stage lại; chỉ record mới hoặc bị nguồn sửa mới được ghi.

Staging → Bronze. Job chỉ đọc object có trong manifest hoàn tất, và bỏ qua manifest đã nạp:

```bash
docker compose --profile jobs build transform   # sau mỗi lần sửa transformation/
docker compose --profile jobs run --rm transform bronze --entity metadata
docker compose --profile jobs run --rm transform bronze --entity ping_result_raw --start 2026-11-19T00:00:00Z --end 2026-11-19T02:00:00Z

# Xử lý lại manifest đã nạp (idempotent: 0 dòng mới nếu dữ liệu không đổi)
docker compose --profile jobs run --rm transform bronze --entity ping_result_raw --start 2026-11-19T00:00:00Z --end 2026-11-19T02:00:00Z --reprocess
```

Hợp đồng dữ liệu, quy tắc quarantine và cách phục hồi: `docs/data-contracts.md`.

### Bronze → Silver (star schema + kiểm tra chất lượng)

```bash
# Tính lại các ngày (giờ VN) chứa khoảng [start, end). Chạy lại an toàn: kết quả giống hệt
docker compose --profile jobs run --rm transform silver --start 2026-11-19T00:00:00Z --end 2026-11-19T03:00:00Z

# Tính lại các ngày chỉ định
docker compose --profile jobs run --rm transform silver --dates 2026-11-19

# Metadata probe đến muộn: tính lại các ngày có fact probe_sk = -1 nay đã có metadata
docker compose --profile jobs run --rm transform silver --repair-unknown
```

Mã thoát `2` nghĩa là có ngày bị chặn vì lỗi chất lượng critical. Khi đó partition giữ bản đã công bố trước đó.

Xem trạng thái công bố và kết quả kiểm tra:

```bash
docker compose exec trino trino --execute "SELECT table_name, ictrl_dt, measurement_id, status, row_count, reason FROM nessie.ops.partition_status ORDER BY 1,2,3"
docker compose exec trino trino --execute "SELECT checked_at, table_name, severity, check_name, observed_value FROM nessie.ops.dq_result WHERE NOT success ORDER BY checked_at DESC LIMIT 20"
```

Mô hình, data dictionary và quy tắc chuẩn hóa: `docs/silver-model.md`.

Integration test chặn công bố, chạy trên nhánh Nessie tạm, không làm bẩn `main`:

```bash
bash tests/integration/dq_block_on_branch.sh 2026-11-20
```

### Silver → Gold

```bash
# Tính lại mọi Gold mart cho các ngày chỉ định (chỉ đọc partition Silver đã công bố)
docker compose --profile jobs run --rm transform gold --dates 2026-11-19

# Kiểm chứng độc lập Gold so với Silver, và sai số percentile so với giá trị chính xác
docker compose --profile jobs run --rm transform crosscheck --date 2026-11-19
```

Định nghĩa metric và các mart: `docs/metric-dictionary.md`.

### Dashboard Superset

```bash
docker compose --profile analytics up -d
# Tạo hoặc cập nhật dataset, chart, dashboard từ code; chạy thử truy vấn của mọi chart; xuất ZIP vào serving/superset/export/
docker compose exec -T superset /app/.venv/bin/python /app/project/provision.py
# Kiểm tra tổ hợp filter, rollup giờ → ngày, timezone qua chính API của Superset, trên một ngày (giờ VN) đã có Gold
docker compose exec -T -w /app/project superset /app/.venv/bin/python verify_filters.py --day 2026-11-22 --cohort dev-200-v2
```

Mở http://localhost:8088/superset/dashboard/global-network-quality/.

Đặc tả từng chart, filter và giới hạn: `docs/dashboard-spec.md`.

Import vào một Superset khác (Dashboards → Import, hoặc dùng CLI):

```bash
docker compose exec -T superset superset import-dashboards -p /app/project/export/global_network_quality.zip -u admin
```

### Test

```bash
# Unit test offline (không cần mạng hay Docker; dlt ghi thật vào thư mục tạm)
docker compose --profile jobs run --rm --entrypoint python ingestion -m pytest -q tests/ingestion

# Runner biến đổi (offline) + kiểm tra dự án dbt biên dịch được
uv run --frozen pytest -q tests/transformation
docker compose --profile jobs run --rm --entrypoint dbt transform parse --project-dir /opt/app/transformation/dbt --profiles-dir /opt/app/transformation/dbt
```

Logic dữ liệu được kiểm chứng trên dữ liệu thật: dbt test chạy cùng mỗi lần build, `crosscheck` so Gold với Silver,
và `dq_block_on_branch.sh` kiểm tra giao thức công bố. So sánh với bản Spark trước đó: `docs/test-report.md` §6.

> Trên Git Bash (Windows), thêm `MSYS_NO_PATHCONV=1` trước các lệnh `docker compose exec` có đường dẫn `/opt/...`. Nếu không, Git Bash tự đổi đường dẫn thành `C:/Program Files/Git/opt/...`.

## 5. Truy vấn bằng SQL client qua Trino

Thông số kết nối, dùng được với DBeaver, DataGrip hay Trino CLI:

| Thông số | Giá trị |
|---|---|
| Host | `localhost` |
| Port | `8081` |
| Catalog | `nessie` |
| User | tên bất kỳ (môi trường đồ án chưa bật xác thực Trino) |

Catalog `nessie` dùng credentials S3 chỉ đọc nên **không ghi được** dữ liệu; catalog ghi `lakehouse` chỉ mở cho user `transform`. Mỗi truy vấn bị giới hạn 5 phút và 1 GB bộ nhớ (`serving/trino/config.properties`).

Truy vấn mẫu cho Gold và drill-down Silver: `docs/sql-examples.md`.

```bash
docker compose exec trino trino --execute "SELECT measurement_id, count(*), count(DISTINCT probe_id) FROM nessie.bronze.ping_result_raw GROUP BY 1"
```

## 6. Điều phối (Airflow)

Airflow gọi Docker qua `/var/run/docker.sock` để chạy container ingestion và transform. `DOCKER_GID` trong `.env` phải là GID nhóm
sở hữu socket đó (Docker Desktop thường là 1001; cách kiểm tra ghi trong `.env.example`). Sai GID thì task báo
`Permission denied`; xem `docs/runbook.md`.

Airflow chạy image `ripe-atlas-ingestion:0.2` và `ripe-atlas-transform:0.1`; code nằm sẵn trong image, nên **sau khi sửa
`ingestion/`, `transformation/` hoặc `config/` phải build lại**: `docker compose --profile jobs build ingestion transform`.

```bash
docker compose --profile orchestration up -d
docker compose exec airflow airflow dags unpause ripe_atlas_hourly      # DAG mặc định bị tạm dừng
docker compose exec airflow airflow dags trigger ripe_atlas_backfill -c '{"start":"2026-11-22T00:00:00Z","end":"2026-11-22T03:00:00Z","probe_set":"dev-200-v2"}'
```

- Mở http://localhost:8090. User `admin`; mật khẩu tự sinh trong `/opt/airflow/logs/.auth/simple_auth_manager_passwords.json`:
  ```bash
  docker compose exec airflow cat /opt/airflow/logs/.auth/simple_auth_manager_passwords.json
  ```
- Danh sách DAG, pool và cách xử lý sự cố: `docs/runbook.md`. Lý do chọn Airflow: `docs/adr/0002-orchestration.md`.

## 7. Giám sát (Prometheus, Pushgateway, Grafana)

```bash
docker compose --profile monitoring up -d
```

| Thành phần | Địa chỉ |
|---|---|
| Grafana | http://localhost:3000 (`admin` / `GRAFANA_ADMIN_PASSWORD`), folder "RIPE Atlas Lakehouse": dashboard 18 panel và 8 cảnh báo |
| Prometheus | http://localhost:9090 |
| Pushgateway | http://localhost:9091 |

Mỗi cảnh báo có runbook riêng trong `docs/runbook.md`. Dashboard được sinh từ code:

```bash
python observability/grafana/build_dashboard.py
```

## 8. Catalog và lineage (dbt docs)

Mô tả bảng/cột, test và lineage staging → Bronze → Silver → Gold sinh từ dự án dbt (cần Trino đang chạy):

```bash
docker compose --profile jobs run --rm -p 8580:8580 --entrypoint sh transform -c "cd /opt/app/transformation/dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8580 --host 0.0.0.0 --no-browser"
```

Mở http://localhost:8580. Từ điển dữ liệu đầy đủ: `docs/data-dictionary.md`; phân quyền và lineage: `docs/governance.md`.
Khi dừng service, **luôn ghi rõ tên service**: `docker compose --profile X stop` mà không có tên sẽ dừng cả core.

## 9. CI

| Workflow | Khi chạy | Nội dung |
|---|---|---|
| `.github/workflows/ci.yml` | PR, push | Offline: ruff, kiểm tra cấu hình Compose/JSON/seed, unit + contract test, build image transform, `dbt parse` |
| `.github/workflows/integration.yml` | Thủ công hoặc hằng đêm | Stack thật: 1 giờ dữ liệu dev → Gold, crosscheck, idempotency, chặn công bố khi lỗi DQ, Superset |
| `.github/workflows/live-api.yml` | Thủ công hoặc hằng tuần | Contract test với RIPE Atlas API thật |

Chạy lint và test tại máy giống CI:

```bash
uv run --frozen ruff check ingestion transformation serving orchestration observability tests
uv run --frozen pytest -q
```

## 10. Tài liệu

| File | Nội dung |
|---|---|
| `docs/design.md` | Kiến trúc |
| `docs/api-survey.md` | Khảo sát API |
| `docs/data-contracts.md` | Data contract |
| `docs/data-dictionary.md` | Từ điển dữ liệu: mọi bảng, mọi cột (kiểu, null, ý nghĩa) |
| `docs/silver-model.md` | Mô hình Silver |
| `docs/metric-dictionary.md` | Từ điển metric |
| `docs/dashboard-spec.md` | Đặc tả dashboard |
| `docs/sql-examples.md` | Truy vấn mẫu |
| `docs/governance.md` | Governance: catalog, phân quyền, lineage |
| `docs/runbook.md` | Runbook |
| `docs/test-report.md` | Báo cáo kiểm thử |
| `docs/compatibility.md` | Phiên bản đã kiểm chứng |
| `docs/adr/` | Quyết định thiết kế |
| `docs/status.md` | Trạng thái và bước tiếp theo |
