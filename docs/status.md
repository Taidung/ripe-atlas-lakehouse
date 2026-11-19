# Trạng thái đồ án (cập nhật mỗi phiên)

## Phiên 2026-11-18, nhánh `feat/design-and-api-survey`

### Đã có từ trước (commit trên `main`)

- SeaweedFS (master, volume, filer, s3) có 4 identity S3 phân quyền, bucket `staging` và `warehouse`.
- Nessie 0.108.8 lưu trên Postgres 16; Spark chạy bằng image `tabulario/spark-iceberg`.
- dlt ingest metadata measurement/probe và ping results theo giờ UTC+7 cho 200 probe cố định.

### Chưa commit lúc bắt đầu phiên (công việc của chủ đồ án; sau đó đã commit nguyên trạng ở `3cf93c5`)

- `spark/staging_to_bronze.py`, `spark/bronze_to_silver.py`
- `tests/test_bronze.py`, `tests/test_silver.py`
- Thay đổi trong `README.md`, `docker-compose.yaml`, `transformation/spark-defaults.conf`

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
- **`tests/integration/smoke_compat.sh` đạt PASS:**
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
  - DDL `transformation/sql/bronze.sql` gồm 5 bảng mới.
  - Job `transformation/jobs/bronze_ingest.py`: MERGE theo `record_uid`, quarantine, audit `ingest_manifest` append-only.
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

- ~~Chưa chạy thật với `strat-k2-c100-v1`~~: đã đo ở giai đoạn 3.
- **Compaction, expire snapshot, dọn object mồ côi và retention staging:** mới là kế hoạch (`docs/data-contracts.md` §5).
- **Lock giữa các run cùng cửa sổ:** chưa có. Trùng lặp được khử ở Bronze; Airflow sẽ giới hạn `max_active_runs`.
- **Late data quá 3 giờ:** max đo được 2,25 giờ. Reconciliation định kỳ bằng `backfill` cần được lên lịch (giai đoạn 5 với Airflow).
- **Bảng legacy** (`bronze.ping_results`, `silver.fact_ping`, …) vẫn còn; cần chủ đồ án đồng ý mới xóa.

### Bước tiếp theo sau giai đoạn 2

Đã thực hiện ở giai đoạn 3, xem phần dưới.

## Phiên 2026-11-19: Giai đoạn 3 (Silver star schema + GX)

### Đã làm, có bằng chứng

- **Thiết kế:** `docs/silver-model.md` gồm ERD Mermaid, data dictionary, căn cứ ngữ nghĩa đo trên 44.340 result thật, quy tắc chọn bản ghi khi nguồn sửa, SCD2 và as-of join.
- **Bảng mới:**
  - `silver.fact_ping_result`, `silver.fact_ping_reply`: partition `(ictrl_dt, measurement_id)`.
  - `silver.dim_probe`, `silver.dim_measurement`: SCD2, có unknown member `-1`.
  - `silver.dim_date`, `silver.dim_time`.
  - `ops.dq_result`, `ops.partition_status`.
- **Code:** `transformation/lakehouse/silver.py`, `quality/dq/`, `transformation/jobs/silver_build.py`. GX Core 1.25.0 chạy Checkpoint và lưu Data Docs vào `artifacts/gx/`.
- **Test:** 25 test Spark pass (10 test Silver mới), gồm:
  - SCD2 với snapshot trùng, thuộc tính quay về giá trị cũ, và kiểm tra chồng lấn.
  - As-of join ở đúng mốc ranh giới, probe chưa có metadata, không nhân bản dòng.
  - Mốc nửa đêm GMT+7, chọn bản ghi khi nguồn sửa, đối soát reply.
  - GX phát hiện lỗi critical.
- **Chạy thật** (dev + strat, ngày 2026-11-19 theo giờ VN):
  - 260.442 result và 790.353 reply. Silver khớp Bronze theo từng measurement; mọi kiểm tra đối soát đạt.
  - **Idempotent:** chạy 2 lần cùng input cho checksum nội dung giống hệt (result 44.340 dòng, reply 133.125 dòng).
  - **Metadata đến muộn:** ingest 216.102 result của tập strat trước khi có metadata → 208.276 result có `probe_sk = -1`. Sau khi ingest metadata, `--repair-unknown` đưa số này về 0, số dòng không đổi.
  - **Chặn công bố khi có lỗi critical:** `tests/integration/dq_block_on_branch.sh` chạy trên nhánh Nessie tạm. Kết quả: exit 2, trạng thái `blocked`, snapshot fact giữ nguyên, `main` không bị ảnh hưởng.
  - **Đo chế độ analysis:** 216.102 result/giờ; staging 29,3 MB/giờ (≈ 0,70 GB/ngày). Ingestion 2 phút 28 giây/giờ, Bronze 47 giây, Silver cho cả ngày khoảng 2,5 phút.
- **Phát hiện từ dữ liệu:**
  - Đồng hồ probe lệch: 712/44.340 result từ 3 probe có `stored_ts < event_ts` (tới −7.725 s). Đã thêm cột `clock_suspect`.
  - Một reply có RTT 68,9 s (cảnh báo).
  - Một probe đổi `asn_v6`/`prefix_v6` giữa hai snapshot; SCD2 ghi nhận đúng thành 2 phiên bản.

### Thay đổi tên hoặc đường dẫn

- `silver.dim_probe`, `silver.dim_measurement`, `silver.fact_ping` (SCD1 cũ) được **đổi tên** thành `silver.legacy_*`. Không xóa dữ liệu; job legacy ghi vào tên mới.

### Còn thiếu hoặc rủi ro

- **Chưa có tên quốc gia/vùng hay tên ASN:** cần chủ đồ án duyệt nguồn tham chiếu trước khi tải.
- **Chạy hằng giờ ở chế độ analysis tốn thời gian:** mỗi lần tính lại cả ngày, và ingestion mỗi lần chạy phải tải 4 cửa sổ (≈ 10 phút). Cần đo lại khi đủ 24 giờ dữ liệu.
- **Ingestion chưa bắt buộc probe set khớp với metadata:** cần chạy `metadata` với cùng probe set trước, hoặc dựa vào `--repair-unknown`.
- Compaction, dọn snapshot và retention: vẫn là kế hoạch.

### Bước tiếp theo: giai đoạn 4 (Gold, Trino, Superset)

1. Metric dictionary trước khi xây marts.
2. Các Gold mart: theo giờ × measurement/af; theo quốc gia; theo ASN; theo probe; so sánh IPv4/IPv6 theo paired cohort; coverage. Lưu sẵn tử số/mẫu số và sketch để tính percentile gộp được.
3. Dashboard Superset "Global Network Quality" (provisioning tái lập được).

## Phiên 2026-11-19: Giai đoạn 4 (Gold, Trino, Superset)

### Đã làm, có bằng chứng

- **Viết trước khi xây mart:**
  - `docs/metric-dictionary.md`: tử số, mẫu số, cách xử lý `NULL`, khả năng cộng dồn, quy tắc ghép cặp IPv4/IPv6, cách tính độ đầy đủ ước tính.
  - `config/metrics.json`: biểu thức metric dùng chung cho test và dashboard.
- **7 Gold mart** (`transformation/lakehouse/gold.py`, `transformation/jobs/gold_build.py`):
  - Các mart: target, country, ASN, probe, histogram RTT, cặp IPv4/IPv6, coverage (lưới đủ 24 giờ).
  - Chỉ đọc partition Silver đã công bố. Ghi bằng overwrite theo ngày. Trạng thái công bố lưu ở `ops.partition_status`.
- **Đo đạc trước khi thiết kế:**
  - Khoảng cách thời gian giữa result v4 và v6 gần nhất của cùng probe: p50 64 s, p90 110 s, 98,5% ≤ 120 s. Từ đó chọn ngưỡng ghép cặp 120 s.
  - Histogram log ×1.02 cho sai số percentile ≤ 0,95% so với giá trị chính xác (đo bằng Spark trên ~950 nghìn reply). Qua `approx_percentile` của Trino thì sai số ≈ 1–1,6%.
- **Test:** 32 test Spark pass, trong đó 7 test Gold:
  - Loss tính từ tổng, không lấy trung bình tỷ lệ.
  - Mẫu số bằng 0 cho `NULL`; rollup giờ → ngày.
  - Percentile, ghép cặp mutual nearest, lưới coverage, tính nhất quán giữa các mart.
- **Chạy thật ngày 2026-11-19:**
  - Gold `published`, 17/17 kiểm tra đạt.
  - `gold_crosscheck.py` **PASS**: KPI của Gold khớp phép tính độc lập từ Silver ở cả 8 nhóm (cohort × measurement); sai số percentile lớn nhất 0,95%.
- **Superset:**
  - `provision.py` dựng 9 dataset, 23 chart và dashboard; 22/22 chart dùng API mới chạy truy vấn thành công. Xuất ZIP.
  - Kiểm tra bằng trình duyệt: mọi chart render, không chart nào lỗi.
  - `verify_filters.py` đạt 10/10 kiểm tra: tổ hợp filter, rollup, timezone, p95.
- **Phát hiện từ dữ liệu:**
  - Trên cặp cùng probe (K-root, 39.657 cặp): loss IPv6 10,16% so với IPv4 2,33%, RTT gần như bằng nhau.
  - Kiểm tra "suy giảm" bằng cách so tổng nhóm bị sai do thành phần probe thay đổi (104 quốc gia "giả"). Đã chuyển sang so cùng probe, còn 8.

### Sự cố trong phiên

- Một truy vấn chẩn đoán quá nặng (`approx_percentile` với accuracy 0,0001 trên ~800 nghìn reply) làm Trino chạy ở 2 GB / ~700% CPU nhiều phút.
  - Hệ thống chặn thao tác kill query, nên đã tạo lại container Trino. Trino không giữ state; dữ liệu nằm trong Iceberg/Nessie.
  - Đã thêm `serving/trino/config.properties` để không tái diễn: `query.max-execution-time=5m`, `query.max-run-time=10m`, `query.max-memory=1GB`, `query.client.timeout=2m`. `mem_limit` tăng lên 3 GB.

### Còn thiếu hoặc rủi ro

- Dữ liệu mới có khoảng 5 giờ (dev) và 1 giờ đầy đủ (strat). Ngưỡng "suy giảm" và hình dạng heatmap cần ≥ 24 giờ dữ liệu ổn định để có ý nghĩa.
- Tooltip của bản đồ chỉ có 2 metric; chi tiết nằm ở bảng quốc gia đặt cạnh.
- Filter thời gian tương đối lệch 7 giờ vì cột thời gian theo giờ VN; nên dùng khoảng tuyệt đối.
- Chưa có tên quốc gia hay tên ASN (cần chủ đồ án duyệt nguồn tham chiếu).
- Chưa có điều phối (Airflow), observability, OpenMetadata và CI: thuộc giai đoạn 5.

### Bước tiếp theo: giai đoạn 5

1. Airflow (profile `orchestration`): DAG theo giờ ingestion → Bronze → Silver → Gold, reconciliation và backfill, `--repair-unknown` sau mỗi lần cập nhật metadata.
2. Prometheus/Pushgateway/Grafana: metric job, độ tươi, DQ; alert kèm runbook.
3. OpenMetadata: metadata Trino/Superset và lineage.
4. GitHub Actions: lint, unit test, Spark test, smoke test.
5. Compaction, expire snapshot, retention staging.

## Phiên 2026-11-19: Giai đoạn 5 (điều phối, giám sát, governance, CI)

Bằng chứng chi tiết: `docs/test-report.md`.

- **Airflow 3.4.0:**
  - 5 DAG (hourly, metadata hằng ngày, đối soát, backfill, bảo trì) và 2 pool.
  - Một lần chạy hourly theo lịch thành công trọn chuỗi.
  - DAG hourly **đang tạm dừng**, Airflow **đang tắt** để tiết kiệm RAM.
- **Giám sát:**
  - Pushgateway (đã sửa persistence), Prometheus và Grafana: dashboard 18 panel, 8 cảnh báo, runbook.
  - Đã thử 5 tình huống cảnh báo, và phát hiện, sửa 1 false positive về dung lượng đĩa.
- **OpenMetadata 2.0.8:** 20 bảng, dashboard và 23 chart, glossary 28 thuật ngữ, 23 cạnh lineage khai báo, column lineage.
- **CI:** 3 workflow, lint sạch. **Chưa chạy trên GitHub.**
- **Phục hồi:** đã thử 12 tình huống (bảng §2 của test report), trong đó có 2 sự cố thật: Silver OOM và DNS lỗi do quá tải RAM.

## Phiên 2026-11-19: Refactor theo thành phần data platform

Mã nguồn được sắp xếp lại theo thành phần. Mỗi thư mục có `README.md`; bản đồ tổng nằm trong `README.md` gốc, mục "Bản đồ mã nguồn".
Mọi đường dẫn trong `docs/` đã đổi sang cấu trúc mới, kể cả ở các mục lịch sử phía trên.

| Cũ | Mới |
|---|---|
| `spark/jobs/`, `spark/lakehouse/`, `sql/*/001_tables.sql`, `spark/Dockerfile` | `transformation/jobs/`, `transformation/lakehouse/`, `transformation/sql/{bronze,silver,ops}.sql`, `transformation/Dockerfile` |
| `spark/lakehouse/quality.py` (một file) | package `quality/dq/`: `results`, `gx_suites`, `gx_runner`, `checks`, `recorder` |
| `spark/lakehouse/ops.py` | `transformation/lakehouse/publication.py` |
| `crosscheck_gold.py`, `maintenance.py` | `gold_crosscheck.py`, `iceberg_maintenance.py` |
| `spark/jobs/staging_to_bronze.py`, `bronze_to_silver.py` + test | đã xoá (xem `git show 90b7c8a:transformation/legacy/`) |
| `ingestion/{config,windows,atlas_http,records,staging,pipeline,cleanup,build_probe_set}.py` | `ingestion/{settings,time_windows,api_client,result_records,staging_store,jobs,staging_cleanup,probe_sampling}.py`; CLI ở `ingestion/cli.py` |
| `seaweedfs/` | `storage/seaweedfs/` |
| `trino/`, `superset/` | `serving/trino/`, `serving/superset/` |
| `superset/metrics.json` | `config/metrics.json` (Gold test, Superset, OpenMetadata dùng chung) |
| `airflow/dags/` | `orchestration/airflow/dags/` |
| `monitoring/` | `observability/` |
| `governance/*` | `governance/openmetadata/` |
| `tests/unit`, `tests/spark`, `tests/live`, `scripts/*.sh` | `tests/ingestion`, `tests/transformation` + `tests/quality`, `tests/contract`, `tests/integration` |

Ngoài việc di chuyển file:
- Job Silver và Gold gọi API `dq.*` mới. Hàm kiểm tra nhất quán Gold chuyển từ `lakehouse/gold.py` sang `dq/checks.py::gold_consistency_checks`.
- Helper dùng chung (`parse_instant`, `parse_dates`, `table_columns`, `count_by_measurement`) gom về `lakehouse/common.py`.
- Fixture và builder của test Spark gom về `tests/spark_support.py`.
- Image Spark: `PYTHONPATH` trỏ tới `/opt/app/transformation:/opt/app/quality`.
- `build_dashboard.py` ghi LF trên mọi OS, nên chạy trên Windows không còn tạo diff giả.

Kiểm chứng sau refactor (2026-11-19):
- `ruff` sạch.
- Unit test ingestion 42/42, chạy trên Windows và trong container `ingestion`.
- Test Spark `tests/transformation` + `tests/quality` 34/34, chạy trong container.
- Test legacy 5/5.
- `docker compose --profile all config -q` OK; dashboard Grafana sinh lại không có diff.
- Chạy thật ngày 2026-11-19, cả 4 bước exit 0:
  - `bronze_ingest`
  - `silver_build`: `published_with_warnings`. Cảnh báo duy nhất là `rtt_ms` ngoài [0, 60000] ở `fact_ping_reply`, giống lần chạy trước refactor.
  - `gold_build`: `published`
  - `gold_crosscheck`: **PASS**, sai số percentile lớn nhất 0,93%.
- CI GitHub run 44508370312 (commit `f992573`): **success**. Gồm job `lint-and-unit` (ruff, Compose, JSON/YAML, unit test) và `spark-tests` (build image, chạy `tests/transformation` + `tests/quality`).
- Chưa chạy lại Superset `provision.py` và OpenMetadata ingestion sau khi đổi mount `config/metrics.json`; mới kiểm tra cú pháp và cấu hình Compose.

## Phiên 2026-11-19: Dọn code (clean code)

Đã xoá:
- `transformation/legacy/`: job SCD1 thế hệ đầu cùng test của chúng, đã được thay thế. Xem lại bằng `git show 90b7c8a:transformation/legacy/`.
- `requirements.txt` ở gốc repo: trùng với `pyproject.toml` và không nơi nào dùng.
- Code không dùng:
  - `ingestion.metrics.summarize`
  - `lakehouse.bronze.MANIFEST_OBJECT_COLUMNS`
  - tham số thừa của `validate_chart`
  - một nhánh `if False` trong `test_gold.py`

Gom phần trùng lặp:
- Múi giờ báo cáo và danh sách measurement của phía Spark khai báo một lần trong `lakehouse/common.py` (trước đó nằm rải ở 5 file).
- Phân trang API `probes/`: một hàm `ingestion.api_client.iter_probe_pages`, dùng cho cả job metadata và lấy mẫu probe.
- Manifest ingestion: phần đầu chung (`IngestContext.manifest_header`) và bản ghi metadata (`metadata_record`).
- Dòng `ops.partition_status`: kiểu `PartitionStatus` (NamedTuple có tên trường) thay cho bộ 11 phần tử không tên.

Tách hàm dài:
- `bronze_ingest.process` (70 dòng) tách thành `load_data`, `transform`, `row_stats`, `manifest_audit_rows`.
- `ingestion.jobs.run_results_window` tách phần gọi API thành `fetch_results`.

Định dạng:
- `ruff format` cho toàn repo.
- Bật lại luật E501 (dòng tối đa 140 ký tự); CI sẽ chặn dòng dài từ nay.
- Bỏ `noqa: E712` bằng cách viết lại điều kiện Spark.

Không đổi hành vi. Kiểm chứng (2026-11-19):
- `ruff check` và `ruff format --check` sạch (có E501).
- Unit test ingestion 42/42; test Spark `tests/transformation` + `tests/quality` 34/34; dashboard Grafana sinh lại không có diff.
- Chạy thật toàn chuỗi:
  - Ingestion `results` 1 giờ (msm 1001) và `metadata`. Lấy lại 2.968 result, cả 2.968 đều `unchanged`. Manifest mới có đủ 23 trường như trước.
  - Bronze `--reprocess` 9 manifest: 11.053 dòng đọc, 0 dòng chèn mới (idempotent).
  - Silver `published_with_warnings` với số dòng giống hệt lần chạy trước khi dọn, cùng một cảnh báo `rtt_ms`.
  - Gold `published`, 7 mart đúng từng số dòng (74 / 5.786 / 20.333 / 25.316 / 247.479 / 13.319 / 192).
  - `gold_crosscheck` **PASS**, sai số percentile lớn nhất 0,93%.
- CI GitHub run 44529234412 (commit `aeb3546`): **success**. Cả `lint-and-unit` (đã có luật E501) và `spark-tests` đều pass.

## Phiên 2026-11-19: Sửa 10 lỗi từ code review

| # | Lỗi | Sửa |
|---|---|---|
| 1 | Silver bị chặn ghi đè dòng `published` thành `blocked` với `snapshot_id = NULL`, làm mất bản công bố | `set_partition_status` giữ `snapshot_id`/`row_count` khi status bị chặn; `status` là kết quả lần chạy gần nhất. Kiểm tra sau ghi sai cũng không công bố snapshot vừa ghi |
| 2 | Gold ghi đè mart rỗng khi không measurement nào được công bố | Gold đọc các measurement có snapshot công bố (`readable_measurements`); không có thì không ghi, đánh dấu `blocked_upstream` |
| 3 | Metric Spark luôn gắn `run_type=scheduled` | Mọi job Spark nhận `--run-type`; grouping key Pushgateway có `run_type`; Airflow truyền `reconcile`/`backfill` |
| 4 | 4xx, body quá lớn, JSON hỏng không vào metric | Mỗi request logic ghi đúng một mẫu với kết quả cuối: `2xx/4xx/5xx/network_error/too_large/invalid_body` |
| 5 | Coverage tính cho probe set không được ingest (completeness giả rất thấp) | Cohort chỉ tính cho (measurement, giờ) mà probe set đó có manifest đã nạp; probe set không ingest trong ngày không có dòng Gold |
| 6 | Check `coverage_full_hour_grid` sai với probe set thiếu IPv6 | Lưới coverage luôn đủ 4 measurement mỗi cohort, `expected_probes = 0` cho af không có probe |
| 7 | Nhánh blocked/empty không `unpersist` | `build_day` của Silver và Gold giải phóng cache trong `finally` |
| 8 | `gold_crosscheck` crash khi full outer join ra `None` | In `-` cho giá trị thiếu và báo MISMATCH; cùng quy tắc cohort với Gold |
| 9 | Bronze metadata quét mọi manifest; `coverage_inputs` quét cả `ingest_manifest` | Metadata chỉ xét `--lookback-days` (mặc định 3) ngày; truy vấn `ingest_manifest` lọc theo partition (entity, ictrl_dt) |
| 10 | Mỗi nhóm probe tạo một `requests.Session` không đóng | `IngestContext.http()` cho mượn client theo luồng, tái sử dụng giữa các cửa sổ; `close()` khi lệnh kết thúc |

Thay đổi số liệu (đúng ý đồ): sau lỗi 5, Gold ngày 2026-11-19 có `quality_hourly_target` 41 dòng thay vì 74.
Cohort `strat-k2-c100-v1` chỉ được ingest 4 (measurement, giờ) trong ngày, nhưng trước đây hiện "có dữ liệu" ở 37 giờ
nhờ các probe trùng với `dev-200-v1`. Hiện số dòng Gold theo cohort khớp đúng số (measurement, giờ) đã ingest (37 và 4).

Kiểm chứng:
- ruff sạch; unit test ingestion 48/48 (thêm 6 test cho lỗi 4 và 10); test Spark 36/36 (thêm 2 test cho lỗi 5 và 6).
- Chạy thật trên `main`: Bronze metadata, Silver, Gold, `gold_crosscheck` **PASS** (sai số percentile lớn nhất 0,96%).
- `tests/integration/dq_block_on_branch.sh` (mở rộng cho lỗi 1 và 2), chạy trên nhánh Nessie tạm: **PASS**.
  - Silver bị chặn, `snapshot_id` công bố trước và sau bằng nhau.
  - Gold dựng lại từ bản cũ, đủ 41 dòng như `main`.
  - Ngày không có Silver: Gold không tạo commit mới.
- CI GitHub run 44540182754 (commit `fa8ede0`): **success**. Cả `lint-and-unit` và `spark-tests` đều pass.
- Đã xoá 10 group Pushgateway cũ không có `run_type` (group số liệu và group thành công/thất bại của bronze_metadata, bronze_results, silver, gold).

## Phiên 2026-11-19: Cảnh báo sau khi tách metric theo run_type

Phát sinh từ sửa lỗi 3: mỗi `run_type` có group Pushgateway riêng, giữ giá trị tới lần chạy cùng loại kế tiếp.
4 rule không lọc `run_type` sẽ giữ giá trị cũ của backfill hoặc chạy tay: `ripe-dq-critical`, `ripe-quarantine`,
`ripe-measurement-missing-data`, và `ripe-api-rate-limited` (rule cuối bản review chưa nêu).

Sửa:
- 4 rule chỉ xét `run_type="scheduled"`, giống `ripe-job-failing`.
- Dashboard Grafana có biến `run_type` (mặc định `scheduled`, chọn được nhiều giá trị hoặc All); 12 truy vấn số liệu job lọc theo biến này.
- `dq_block_on_branch.sh` chạy job với `PUSHGATEWAY_URL` rỗng.

Kiểm chứng trên Prometheus/Grafana đang chạy:
- Biểu thức DQ cũ cho `silver = 2`, tức sẽ bắn, do metric `manual` của lần test cố ý bị chặn. Biểu thức mới không có kết quả.
- Sau khi restart Grafana, cả 5 rule số liệu job đã lọc `scheduled`; rule DQ critical ở trạng thái `inactive`.
- Hai cảnh báo đang bắn là đúng thực tế: Gold trễ (Airflow đang tắt) và service không phản hồi (Trino/Superset/Airflow đang tắt).
- CI GitHub run 44543534964 (commit `e5bfc45`): **success** (`lint-and-unit`, `spark-tests`).

## Phiên 2026-11-19: run_type cho ingestion, completeness khi kỳ vọng bằng 0

- Lệnh `metadata` gắn cứng `run_type=scheduled` (cả manifest lẫn metric), nên một lần chạy tay lỗi sẽ được tính vào
  cảnh báo của lịch chính. Đã sửa: mọi lệnh ingestion nhận `--run-type`, mặc định `manual` cho `metadata`/`results`.
  DAG truyền `scheduled` cho metadata và incremental, `reconcile` cho phần ingest của DAG reconcile (trước đây
  mang nhãn `backfill`, lệch với các job Spark cùng DAG).
- `push_freshness` đẩy completeness = 0 khi `expected_results = 0` (probe set không có probe của af đó). Đã sửa: không
  đẩy giá trị cho nhóm này.
- Kiểm chứng:
  - 7 test mới cho nhãn mặc định và giá trị truyền vào; unit test ingestion 55/55.
  - Chạy thật `metadata` không truyền `--run-type`: Pushgateway ghi group `{stage=metadata, run_type=manual}`.
  - `gold_build` 2026-11-19 chạy lại `published`, số dòng không đổi.
- Còn lại trên Pushgateway: group `{stage=metadata, run_type=scheduled}` do lần chạy tay trước đây bị gắn nhầm nhãn.
  Không che được lỗi thật (rule chỉ bắn khi lần thất bại mới hơn lần thành công), nhưng là dữ liệu sai.

## Đối chiếu tiêu chí nghiệm thu cuối

| Tiêu chí | Trạng thái |
|---|---|
| Chạy được từ checkout sạch theo README | Mọi lệnh trong README đều đã chạy ở các phiên này. **Chưa thử trên một checkout hoàn toàn sạch**: `integration.yml` làm việc đó khi được chạy |
| Ingest đủ 4 measurement theo phạm vi probe đã cấu hình | Đạt (dev và strat) |
| Silver/Gold đúng grain, KPI đúng, test rerun/backfill đạt | Đạt (crosscheck PASS, checksum giống hệt, repair-unknown) |
| Trino truy vấn được cùng dữ liệu Spark ghi | Đạt (smoke test, cùng snapshot) |
| Superset có dashboard hoạt động, filter và metric được đối chiếu | Đạt (10/10) |
| GX tạo kết quả và chặn công bố khi lỗi critical | Đạt (nhánh Nessie tạm) |
| OpenMetadata hiển thị metadata và lineage | Đạt (kiểm tra qua API; chưa chụp ảnh UI) |
| Grafana có metric thật, cảnh báo đã thử | Đạt |
| CI có workflow chạy được và có bằng chứng | Đạt cho `ci.yml`: run 44488710698 (commit d65e29b) xanh cả `lint-and-unit` và `spark-tests` trên GitHub. `integration.yml` chưa chạy |
| Báo cáo chỉ dùng số đo thật | Đạt (`docs/test-report.md`) |

## Còn lại và đề xuất tiếp theo

1. ~~Chạy `ci.yml` trên GitHub~~: đã xanh. Còn lại: chạy `integration.yml` thủ công.
2. Chọn cohort cho lịch chạy chính (`PROBE_SET_VERSION=strat-k2-c100-v1`) và chạy ≥ 24 giờ, để đo tải thật một ngày ở chế độ analysis (bộ nhớ Silver, thời gian chạy).
3. Ghi nhiều bảng nguyên tử bằng nhánh Nessie (ghi trên nhánh, kiểm tra, merge vào `main`); triển khai Nessie GC.
4. Tên quốc gia/ASN: cần duyệt nguồn tham chiếu.
5. Xóa các bảng legacy (`bronze.ping_results`, `silver.legacy_*`) nếu chủ đồ án đồng ý.
