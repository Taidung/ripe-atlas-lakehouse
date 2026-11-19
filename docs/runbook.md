# Runbook vận hành

Các lệnh dưới đây chạy từ thư mục gốc repo. Trên Git Bash (Windows), đặt `MSYS_NO_PATHCONV=1` trước các lệnh có đường dẫn `/opt/...`.

## Địa chỉ

| Thành phần | URL | Đăng nhập |
|---|---|---|
| Airflow | http://localhost:8090 | user `admin`; mật khẩu tự sinh trong file `/opt/airflow/logs/.auth/simple_auth_manager_passwords.json` (volume `airflow_logs`) |
| Grafana | http://localhost:3000 | `admin` / `GRAFANA_ADMIN_PASSWORD` trong `.env` |
| Prometheus | http://localhost:9090 | — |
| Pushgateway | http://localhost:9091 | — |
| Superset | http://localhost:8088 | `admin` / `SUPERSET_ADMIN_PASSWORD` |
| Trino | localhost:8081, catalog `nessie` | user tùy ý |
| Nessie | http://localhost:19120 | — |

## Lịch chạy (Airflow)

| DAG | Lịch (UTC) | Việc |
|---|---|---|
| `ripe_atlas_hourly` | `5 * * * *` | Ingest giờ vừa kết thúc + 3 giờ chồng lấn → Bronze → Silver → Gold |
| `ripe_atlas_daily_metadata` | `20 0 * * *` | Snapshot metadata → Bronze → `silver --repair-unknown` → Gold cho hôm qua và hôm nay (giờ VN) |
| `ripe_atlas_reconcile` | `30 2 * * *` | Tải lại cả ngày hôm qua (dữ liệu trễ quá 3 giờ) |
| `ripe_atlas_maintenance` | `30 3 * * *` | Compaction ngày đã đóng; xóa object mồ côi > 48 giờ; xóa log task > 14 ngày |
| `ripe_atlas_backfill` | thủ công | `airflow dags trigger ripe_atlas_backfill -c '{"start":"…Z","end":"…Z","probe_set":"strat-k2-c100-v1"}'` |

**Lưu ý khi vận hành DAG:**
- DAG mới **mặc định bị tạm dừng**. Bật bằng lệnh `docker compose exec airflow airflow dags unpause <dag_id>`.
- Mọi task ghi lakehouse đi qua pool `lakehouse_writer` (1 slot), nên backfill và lịch chạy không ghi đồng thời.
- Task Silver/Gold thất bại với mã thoát 2 nghĩa là **lỗi chất lượng critical**; task này không retry.
- Mỗi job Spark nhận `--run-type` (`scheduled`, `reconcile`, `backfill`, `repair`, `manual`; chạy tay mặc định `manual`).
  Metric được đẩy vào group Pushgateway riêng theo `run_type`, và cảnh báo "job lỗi liên tiếp" chỉ xét
  `run_type="scheduled"`: một backfill thành công không che được lỗi của lịch chạy chính, và ngược lại.
- `bronze_ingest.py --entity metadata` chỉ xét manifest của 3 ngày gần nhất (`--lookback-days`). Manifest cũ hơn
  chưa nạp thì chạy lại với `--lookback-days N` hoặc `--start/--end`.
- Sau khi nâng cấp lên bản có `run_type` trong grouping key, xoá các group cũ không có `run_type` một lần:
  `for s in bronze_results bronze_metadata silver gold maintenance; do for g in "" /outcome/success /outcome/failure; do curl -X DELETE "http://localhost:9091/metrics/job/ripe_pipeline/stage/$s$g"; done; done`
  (đã chạy trên stack local ngày 2026-11-19; group `staging_cleanup` cố ý không có `run_type`, giữ nguyên).

## Cảnh báo

<a id="ripe-job-failing"></a>
### Job pipeline lỗi liên tiếp (critical)

1. Mở Airflow → DAG `ripe_atlas_hourly` → task đỏ → Log.
2. Lọc log JSON theo `run_id`:
   ```bash
   docker compose exec airflow bash -c "grep -rh '\"event\": \"window_failed\"' /opt/airflow/logs | tail"
   ```
3. Phân loại nguyên nhân:
   - **HTTP 4xx** (ví dụ 404 measurement): lỗi cấu hình, không retry được. Metric `ripe_api_requests{status_class="4xx"}`;
     body quá lớn là `too_large`, body không parse được là `invalid_body`.
   - **429/5xx/mạng** đã hết số lần retry: chờ, rồi chạy lại.
   - **Spark mã 2:** xem mục [DQ critical](#ripe-dq-critical).
4. Sau khi sửa: **Clear** task trong Airflow, hoặc chạy backfill cho các giờ bị lỡ.

   Vì manifest là checkpoint nên chỉ phần còn thiếu được làm lại, không sinh bản ghi trùng.

<a id="ripe-gold-stale"></a>
### Gold trễ quá 2 giờ (critical)

- **SLO đề xuất:** event mới nhất trong Gold không quá 7.200 giây (2 giờ) so với hiện tại.
- **Kiểm tra theo thứ tự:**
  1. DAG hourly có chạy không: DAG có đang bị pause, scheduler có healthy (`/api/v2/monitor/health`).
  2. Task nào chậm hoặc lỗi.
  3. Bảng `ops.partition_status` có partition bị `blocked` không.
- **Bù dữ liệu:** trigger `ripe_atlas_backfill` cho khoảng giờ bị trễ.

<a id="ripe-measurement-missing-data"></a>
### Thiếu dữ liệu theo measurement (warning)

1. Xem chart "Tình trạng xử lý theo giờ" trên Superset, hoặc chạy:
   ```sql
   SELECT event_hour_local, measurement_id, data_state, results_total_raw, expected_results
   FROM nessie.gold.coverage_hourly ORDER BY event_hour_utc DESC LIMIT 40;
   ```
2. Đọc `data_state`:
   - `processed_no_results`: API trả rỗng (hiếm với built-in measurement). Thử lại sau và so với trang measurement trên RIPE Atlas.
   - Độ đầy đủ thấp nhưng `measured`: probe ngừng hoạt động hàng loạt, hoặc thiếu nhóm probe (xem `manifest.requests`).

<a id="ripe-dq-critical"></a>
### Kiểm tra chất lượng critical thất bại (critical)

- Partition **đã bị chặn công bố**. Bản công bố trước đó vẫn được giữ nguyên: `snapshot_id` trong
  `ops.partition_status` vẫn trỏ tới snapshot cũ, Gold tiếp tục dựng từ bản đó (cột `reason` của Gold ghi chú lại).
  Xem bản đang được dùng:
  ```sql
  SELECT table_name, ictrl_dt, measurement_id, status, snapshot_id, reason
  FROM nessie.ops.partition_status WHERE status IN ('blocked', 'blocked_upstream') ORDER BY ictrl_dt DESC;
  ```
- Xem chi tiết:
  ```sql
  SELECT checked_at, table_name, partition_scope, check_name, observed_value, details
  FROM nessie.ops.dq_result WHERE NOT success AND severity = 'critical' ORDER BY checked_at DESC;
  ```
- Mở Data Docs của GX: `artifacts/gx/gx/uncommitted/data_docs/local_site/index.html`.
- Có thể tái hiện an toàn trên một nhánh Nessie, xem `tests/integration/dq_block_on_branch.sh`.
- Sửa nguồn (logic hoặc dữ liệu), rồi chạy lại `silver_build.py --dates <ngày>`.

<a id="ripe-disk-almost-full"></a>
### Đĩa dữ liệu Docker sắp đầy (warning)

- Giới hạn thật là ổ **D:** của host (Docker data ở `D:\DockerImage`).
  ```powershell
  Get-PSDrive D
  ```
  ```bash
  docker system df
  ```
- Giải phóng theo thứ tự an toàn:
  1. `docker builder prune` (chỉ xóa build cache).
  2. Nếu dùng nhiều nhánh Nessie thì chạy Nessie GC (xem dưới).
  3. Rút ngắn retention staging (thao tác thủ công, có xác nhận).

<a id="ripe-service-down"></a>
### Service không phản hồi (critical)

```bash
docker compose ps
docker compose logs --tail 100 <service>
```

RAM theo container (cAdvisor không dùng được trên Docker Desktop + containerd):

```bash
docker stats --no-stream
```

Trino bị chiếm tài nguyên quá lâu: giới hạn truy vấn đã có trong `serving/trino/config.properties`. Nếu vẫn treo thì khởi động lại; Trino không giữ state:

```bash
docker compose --profile analytics restart trino
```

<a id="ripe-api-rate-limited"></a>
### RIPE Atlas API trả 429 nhiều (warning)

- Client đã tôn trọng `Retry-After`.
- Giảm `ATLAS_CONCURRENCY` (mặc định 2) hoặc `PROBE_CHUNK_SIZE` trong `.env`, rồi tạo lại container Airflow.

<a id="ripe-quarantine"></a>
### Có bản ghi bị quarantine (warning)

```sql
SELECT reason, count(*) FROM nessie.bronze.quarantine WHERE reprocess_status = 'pending' GROUP BY 1;
```

Sửa logic, rồi chạy lại:

```bash
docker compose exec spark-iceberg spark-submit /opt/app/transformation/jobs/bronze_ingest.py --entity ping_result_raw --start … --end … --reprocess
```

## Log

- **Mọi service:** Docker driver `json-file`, xoay vòng 10 MB × 5 file (tối đa ~50 MB mỗi service; cấu hình `x-logging` trong `docker-compose.yaml`).
- **Job ingestion và Spark:** mỗi sự kiện là một dòng JSON có `ts`, `stage`, `event`, `run_id`, `measurement_id`, cửa sổ, số đếm, `error`.
- **Log task Airflow:** volume `airflow_logs` (`/opt/airflow/logs`); DAG maintenance xóa file > 14 ngày.
- **Chưa có log backend tập trung** (Loki/ELK); Prometheus **không** lưu log.

Tra cứu:

```bash
docker compose logs spark-iceberg | grep '"stage": "silver"'
docker compose exec airflow bash -c "grep -rh '\"run_id\": \"<run_id>\"' /opt/airflow/logs"
```

## Retention, dọn dẹp, reset

| Dữ liệu | Retention | Cơ chế |
|---|---|---|
| Staging (object, manifest, index) | Giữ để replay | Chỉ tự động xóa **object mồ côi** > 48 giờ (`python -m ingestion cleanup --apply`) |
| Iceberg (Bronze/Silver/Gold/ops) | Giữ | Compaction hằng ngày (`iceberg_maintenance.py`) |
| Prometheus | 15 ngày hoặc 2 GB | Cờ `--storage.tsdb.retention.*` |
| Log container | 50 MB/service | json-file rotation |
| Log task Airflow | 14 ngày | DAG maintenance |

<a id="nessie-gc"></a>
**Snapshot và file Iceberg cũ:**
- **Không** dùng `expire_snapshots` hay `remove_orphan_files` của Iceberg với catalog Nessie, vì file có thể vẫn được nhánh hoặc tag khác tham chiếu.
- Dùng Nessie GC: công cụ `nessie-gc`, phiên bản khớp Nessie 0.108.8. **Chưa triển khai** trong đồ án.

**Reset môi trường:**
- `docker compose --profile all down` dừng service, **giữ dữ liệu**.
- `docker compose --profile all down -v` **xóa toàn bộ volume**: staging, warehouse, lịch sử Nessie, metadata Superset/Airflow/Grafana và dữ liệu Prometheus. Không khôi phục được.
- Chỉ xóa một phần, ví dụ dữ liệu giám sát:
  ```bash
  docker compose --profile monitoring down
  docker volume rm ripe-atlas-lakehouse_prometheus_data
  ```

## Quyền truy cập

| Thành phần | Quyền |
|---|---|
| Ingestion | identity S3 `atlas-ingest`: chỉ đọc/ghi bucket `staging` |
| Spark | `atlas-spark`: đọc `staging`, ghi `warehouse` |
| Trino | `atlas-reader`: chỉ đọc `warehouse` (đã kiểm chứng: ghi bị từ chối) |
| Airflow | Không giữ khóa S3 nào ngoài khóa ingestion để chuyển tiếp; Spark dùng khóa có sẵn trong container của nó |
| docker.sock mount vào Airflow | Quyền tương đương root trên Docker host; chỉ chấp nhận cho môi trường local (ADR-0002) |
