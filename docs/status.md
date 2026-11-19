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
- **Code:** `spark/lakehouse/silver.py`, `spark/lakehouse/quality.py`, `spark/jobs/silver_build.py`. GX Core 1.25.0 chạy Checkpoint và lưu Data Docs vào `artifacts/gx/`.
- **Test:** 25 test Spark pass (10 test Silver mới), gồm:
  - SCD2 với snapshot trùng, thuộc tính quay về giá trị cũ, và kiểm tra chồng lấn.
  - As-of join ở đúng mốc ranh giới, probe chưa có metadata, không nhân bản dòng.
  - Mốc nửa đêm GMT+7, chọn bản ghi khi nguồn sửa, đối soát reply.
  - GX phát hiện lỗi critical.
- **Chạy thật** (dev + strat, ngày 2026-11-19 theo giờ VN):
  - 260.442 result và 790.353 reply. Silver khớp Bronze theo từng measurement; mọi kiểm tra đối soát đạt.
  - **Idempotent:** chạy 2 lần cùng input cho checksum nội dung giống hệt (result 44.340 dòng, reply 133.125 dòng).
  - **Metadata đến muộn:** ingest 216.102 result của tập strat trước khi có metadata → 208.276 result có `probe_sk = -1`. Sau khi ingest metadata, `--repair-unknown` đưa số này về 0, số dòng không đổi.
  - **Chặn công bố khi có lỗi critical:** `scripts/test_dq_block_on_branch.sh` chạy trên nhánh Nessie tạm. Kết quả: exit 2, trạng thái `blocked`, snapshot fact giữ nguyên, `main` không bị ảnh hưởng.
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
  - `superset/metrics.json`: biểu thức metric dùng chung cho test và dashboard.
- **7 Gold mart** (`spark/lakehouse/gold.py`, `spark/jobs/gold_build.py`):
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
  - `crosscheck_gold.py` **PASS**: KPI của Gold khớp phép tính độc lập từ Silver ở cả 8 nhóm (cohort × measurement); sai số percentile lớn nhất 0,95%.
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
  - Đã thêm `trino/config.properties` để không tái diễn: `query.max-execution-time=5m`, `query.max-run-time=10m`, `query.max-memory=1GB`, `query.client.timeout=2m`. `mem_limit` tăng lên 3 GB.

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
