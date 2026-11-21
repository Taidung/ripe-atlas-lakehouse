# Báo cáo kiểm thử

> **Cập nhật 2026-11-21:** tầng biến đổi đã chuyển từ Spark sang dbt trên Trino (ADR-0003). Mục 1–5 ghi kết quả của bản
> Spark (đến 2026-11-20), giữ làm bằng chứng lịch sử. Kết quả của bản hiện tại ở **§6**.

Mọi số liệu dưới đây là **số đo thật** trong ngày 2026-11-18/19.

**Môi trường:** Windows 11, 12 CPU logic, RAM host 15,8 GB, Docker Desktop 29.6.2 với 8,2 GB cho VM WSL, Docker data trên ổ D:.

**Dữ liệu:** RIPE Atlas, 4 measurement 1001/1004/2001/2004.
- Probe set `dev-200-v1` (200 probe): 22:00Z 18/11 – 08:00Z 19/11.
- Probe set `strat-k2-c100-v1` (4.324 v4 / 2.897 v6): giờ 02:00–03:00Z 19/11.
- Ngày 19/11 theo giờ VN: Silver có **307.789 result** và **932.435 reply**.

#> **Cập nhật 2026-11-21:** tầng biến đổi đã chuyển từ Spark sang dbt trên Trino (ADR-0003). Mục 1–5 ghi kết quả của bản
> Spark (đến 2026-11-20), giữ làm bằng chứng lịch sử. Kết quả của bản hiện tại ở **§6**.

# 1. Ma trận kiểm thử

| Loại | Phạm vi | Lệnh | Kết quả |
|---|---|---|---|
| Unit (offline) | Cửa sổ thời gian/timezone, parse, hash, phân loại delta, retry/backoff/Retry-After, probe set và lấy mẫu phân tầng, dlt ghi thật vào thư mục tạm, manifest, cleanup | `pytest tests/ingestion` | **42/42 pass**, trên Windows và trong container |
| Contract (live) | Metadata 4 measurement, biên start/stop inclusive, `rcvd`/`dup`/`-1`, phân trang probes | `pytest -m live tests/contract` | **6/6 pass**, 6 request |
| Spark: Bronze | 9 lý do quarantine, trùng nội dung, cùng khóa khác payload, toàn vẹn batch | `pytest tests/transformation tests/quality` | đạt |
| Spark: Silver | SCD2 (snapshot trùng, quay lại giá trị cũ, chồng lấn), as-of ở ranh giới, unknown member, mốc nửa đêm GMT+7, chọn bản ghi khi nguồn sửa, đối soát reply, GX bắt lỗi critical | ″ | đạt |
| Spark: Gold | Loss/mean/tỷ lệ phản hồi tính từ tổng, mẫu số 0 → `NULL`, rollup, percentile từ histogram, ghép cặp mutual nearest, lưới coverage, nhất quán giữa các mart | ″ | **32/32 pass** (tổng Spark) |
| Tương thích | Spark ghi, Trino đọc cùng snapshot | `tests/integration/smoke_compat.sh` | PASS |
| Đối chiếu Gold/Silver | Tính lại KPI độc lập, sai số percentile | `gold_crosscheck.py` | PASS, khớp đến 4 chữ số thập phân; sai số percentile ≤ 0,95% |
| Superset | 22 chart chạy truy vấn qua API, render trên trình duyệt, tổ hợp filter/rollup/timezone | `provision.py`, `verify_filters.py` | 22/22, **10/10** |
| OpenMetadata | Connector Trino/Superset, mô tả, glossary, lineage | `run_ingestion.py` | Xem §4 |
| Cảnh báo | 5 tình huống thử + cảnh báo thật | Push metric giả, dừng service | Xem §5 |
| Lint | ruff | `ruff check …` | Sạch |

#> **Cập nhật 2026-11-21:** tầng biến đổi đã chuyển từ Spark sang dbt trên Trino (ADR-0003). Mục 1–5 ghi kết quả của bản
> Spark (đến 2026-11-20), giữ làm bằng chứng lịch sử. Kết quả của bản hiện tại ở **§6**.

# 2. Idempotency, dữ liệu đến muộn, backfill, phục hồi

| Tình huống | Cách thử | Kết quả đo |
|---|---|---|
| Chạy lại Bronze cùng phạm vi | Chạy lại job | `nothing_pending` |
| Xử lý lại manifest đã nạp | `--reprocess` | 22.113 dòng đọc, **0 dòng mới** |
| Tải chồng lấn 3 giờ | `incremental` sau `results` | Giờ đã có: `unchanged = fetched`, `staged = 0` |
| Silver chạy lại cùng input | 2 lần liên tiếp | Checksum nội dung **giống hệt** (44.340 result / 133.125 reply) |
| Metadata đến muộn | Ingest result tập strat trước khi có metadata | `probe_sk = -1`: 208.276 → **0** sau `--repair-unknown`, số dòng không đổi |
| Ghi được một phần staging (dlt xong, không có manifest) | Test `test_failed_load_leaves_no_manifest` | Không ai đọc object; lần sau stage lại; `cleanup` tìm và xóa object mồ côi |
| dlt xong nhưng Bronze chưa chạy | Lần chạy ngoài Airflow (container bị bỏ lại) | Bronze lần sau tự nhận 16 manifest, thêm 11.012 dòng |
| **Silver đã commit một bảng rồi chết (OOM)** | Sự cố thật lúc 07:37 | `fact_ping_result` đã commit, `fact_ping_reply` chưa. Kiểm tra `histogram_total_eq_rtt_count` của Gold **chặn công bố**. Sau khi sửa (Gold đọc đúng snapshot Silver đã công bố), Gold công bố được từ cặp snapshot nhất quán. Silver chạy lại với driver 3 GB → nhất quán |
| GX phát hiện lỗi nghiêm trọng | `tests/integration/dq_block_on_branch.sh` trên nhánh Nessie tạm | exit 2, `blocked`, snapshot fact giữ nguyên, `main` không đổi, nhánh tạm tự xóa |
| Job bị kill hoặc container restart | Dừng Airflow khi task ingestion đang chạy | Container ingestion chạy tiếp và tự xong (16/16, exit 0): **không mất và không trùng dữ liệu**, nhưng thành container mồ côi; `on_kill` không chạy khi tắt cả Airflow. Ghi nhận trong runbook |
| Hai run cùng xử lý một phạm vi | 2 ingestion song song, cửa sổ 07:00Z, msm 1001 | Mỗi run stage 2.964 (không có lock); Bronze đọc 5.928, **thêm 2.964**; khóa và `record_uid` duy nhất |
| Hạ tầng quá tải (DNS Docker lỗi) | Sự cố thật: RAM host cạn | S3 client hết 5 lần retry → Silver fail **trước** commit; bản công bố trước giữ nguyên |
| Backfill | `python -m ingestion backfill` và DAG `ripe_atlas_backfill` | Cùng code, `run_type = backfill` trong manifest và metric; pool `lakehouse_writer` cấm ghi song song |

#> **Cập nhật 2026-11-21:** tầng biến đổi đã chuyển từ Spark sang dbt trên Trino (ADR-0003). Mục 1–5 ghi kết quả của bản
> Spark (đến 2026-11-20), giữ làm bằng chứng lịch sử. Kết quả của bản hiện tại ở **§6**.

# 3. Hiệu năng (đo thật)

| Phép đo | Giá trị |
|---|---|
| Ingest 1 giờ, tập strat (4 measurement, ~4.600 probe) | **2 phút 28 giây**, 216.102 result, staging 29,3 MB gzip |
| Ingest 1 giờ, tập dev | ~3 giây mỗi (measurement, giờ) |
| Bronze 1 giờ strat | 47 giây |
| Silver 1 ngày (~300 nghìn result, ~930 nghìn reply, kèm GX) | ~2 phút 30 giây với driver 3 GB; **OOM với driver 1 GB mặc định** |
| Gold 1 ngày (7 mart) | ~1–1,5 phút |
| DAG hourly trọn chuỗi (dev, 4 cửa sổ) | Ingest 142 giây; Bronze 49 giây; cả chuỗi khoảng 6–8 phút |
| Trino: p50/p95/p99 trên histogram cả ngày | 5 giây (sau khi giới hạn truy vấn) |
| RAM nhàn rỗi | Core ≈ 1,4 GB; Trino 0,9–2,3 GB (heap tối đa 2,4 GB); Superset 0,25 GB; monitoring ≈ 0,5 GB; Airflow 0,15 GB; OpenMetadata ≈ 2,1 GB (server 1 GB + OpenSearch 1 GB + DB) |
| File Iceberg | Bronze sau 6 lần nạp theo giờ: 6–8 file mỗi partition ngày, mỗi file ~200 KB → compaction hằng ngày |

##> **Cập nhật 2026-11-21:** tầng biến đổi đã chuyển từ Spark sang dbt trên Trino (ADR-0003). Mục 1–5 ghi kết quả của bản
> Spark (đến 2026-11-20), giữ làm bằng chứng lịch sử. Kết quả của bản hiện tại ở **§6**.

# 3.1 Thử bật full stack khi Spark không chạy thường trực (2026-11-19)

Mục đích: xem bỏ container Spark thường trực (hoặc thay Spark bằng dbt + Trino) có đủ chỗ cho full stack không.

Cách đo:
- Script lấy mẫu mỗi 5 giây ba chỉ số:
  - tổng RAM container (`docker stats`)
  - `MemAvailable` của VM Docker
  - `Available MBytes` của Windows
- Bật từng nhóm service; tự dừng khi Windows hoặc VM còn dưới 400 MB.

| Thời điểm | Service đang chạy | Container | VM còn trống | Windows còn trống |
|---|---|---|---|---|
| Trước khi bật | core + monitoring, Spark **tắt** | 1,2 GiB | 5,6 GiB | 1,0 GB |
| +45 giây | + Trino + Superset | 1,9–2,3 GiB | 4,6–4,8 GiB | **45–340 MB** → dừng |
| Sau khi dừng analytics | core + monitoring | 1,2 GiB | 5,6 GiB | 1,5–1,7 GB |

Kết luận:
- **Nút thắt là RAM của Windows, không phải giới hạn 8,2 GB của Docker.** Khi container dùng 2,3 GiB, VM vẫn còn 4,6 GiB trống, nhưng Windows đã gần cạn.
  - `vmmemWSL` giữ 5,9–6,3 GB dù container chỉ dùng 1,2–2,3 GiB: Linux dùng phần còn lại làm cache và WSL không trả ngay cho Windows.
  - Các ứng dụng khác trên máy dùng khoảng 6 GB (trình duyệt, ứng dụng chat, Claude, Defender, …).
- Bỏ Spark thường trực (≈ 0,95 GiB) hay thay Spark bằng dbt + Trino **không đủ** để bật full stack trên máy này khi các ứng dụng trên vẫn mở. Chưa đo được Airflow, OpenMetadata, hay đỉnh RAM của job Spark trong cấu hình full stack, vì thử nghiệm đã dừng ở bước analytics.
- Hướng khả thi:
  - Tiếp tục chạy tuần tự từng nhóm (như các phase trước).
  - Hoặc đóng bớt ứng dụng nặng trên Windows và đặt `autoMemoryReclaim=dropCache` trong `.wslconfig` để WSL trả cache về cho Windows, rồi đo lại.

#> **Cập nhật 2026-11-21:** tầng biến đổi đã chuyển từ Spark sang dbt trên Trino (ADR-0003). Mục 1–5 ghi kết quả của bản
> Spark (đến 2026-11-20), giữ làm bằng chứng lịch sử. Kết quả của bản hiện tại ở **§6**.

# 4. OpenMetadata (2.0.8, kiểm tra bằng API)

- **Connector Trino:** 100% thành công (26 bản ghi; 9 bảng legacy bị lọc như cấu hình).
- **Connector Superset:** 100% (92 bản ghi). Có dashboard "Global Network Quality" với 23 chart.
- **Làm giàu:** 20 bảng có mô tả, owner team `data-platform` và tag `DataLayer.*`; 19 cột có mô tả; glossary 28 thuật ngữ metric.
- **Lineage:**
  - 23 cạnh khai báo (container staging → Bronze → Silver → Gold).
  - Quanh `silver.fact_ping_result`: 23 node, 8 cạnh upstream, 22 cạnh downstream, 171 cặp column-level.
  - `gold.quality_hourly_probe` có 2 node Superset ở downstream, **do connector tự suy ra**.

#> **Cập nhật 2026-11-21:** tầng biến đổi đã chuyển từ Spark sang dbt trên Trino (ADR-0003). Mục 1–5 ghi kết quả của bản
> Spark (đến 2026-11-20), giữ làm bằng chứng lịch sử. Kết quả của bản hiện tại ở **§6**.

# 5. Cảnh báo (Grafana 13.3.1, 8 rule)

| Tình huống | Thời điểm | Kết quả |
|---|---|---|
| Job lỗi liên tiếp (thử: lần thành công cuối 3 giờ trước, lỗi mới nhất bây giờ) | 07:47 → 07:57Z | Alerting, critical |
| Lỗi DQ critical (thử: 3 kiểm tra critical thất bại) | ″ | Alerting |
| Gold trễ > 2 giờ (thử: event mới nhất 4 giờ trước, `for 5m`) | ″ | Alerting; measurement thật vẫn Normal |
| Thiếu dữ liệu (thử: độ đầy đủ 10%) | ″ | Alerting |
| Service down (dừng `node-exporter`) | ″ | Alerting |
| **Thật:** Trino đã dừng | ″ | Alerting (đúng) |
| **Thật:** cohort strat thiếu dữ liệu ở giờ gần nhất (~3–4%) | ″ | Alerting (đúng: DAG đang chạy với probe set dev) |
| Sau khi xóa metric thử và bật lại node-exporter | 08:00Z | 4 cảnh báo thử trở về Normal |
| **False positive phát hiện được:** đĩa Docker | baseline | `size − avail` báo 84,9 GB vì ext4 giữ 5% của đĩa ảo 1 TB. Đã sửa thành `size − free` = 29,9 GB (khớp file vhdx 29,6 GB) |

#> **Cập nhật 2026-11-21:** tầng biến đổi đã chuyển từ Spark sang dbt trên Trino (ADR-0003). Mục 1–5 ghi kết quả của bản
> Spark (đến 2026-11-20), giữ làm bằng chứng lịch sử. Kết quả của bản hiện tại ở **§6**.

# 6. Vấn đề phát hiện khi kiểm thử và đã sửa

1. Audit `ingest_manifest` bị ghi đè khi chạy `--reprocess` → chuyển sang append-only.
2. Kiểm tra "suy giảm" bị sai lệch do thành phần probe thay đổi → chuyển sang so cùng probe (104 → 8 quốc gia).
3. Literal `100.0` là DECIMAL nên tỷ lệ có thể bị làm tròn → ép sang `DOUBLE`.
4. Một truy vấn chẩn đoán làm Trino treo → thêm giới hạn truy vấn và tăng `mem_limit` lên 3 GB.
5. Pushgateway không ghi được file persistence (volume thuộc root) nên mất toàn bộ metric sau reboot → thêm container init `chown`.
6. Spark driver 1 GB bị OOM → tăng lên 3 GB.
7. Gold đọc Silver bản mới nhất nên dễ gặp trạng thái commit nửa chừng → đọc theo snapshot đã công bố.
8. Airflow 3 `base_url` trỏ sai cổng nên task không gọi được Execution API → sửa về cổng trong container.
9. Test cleanup chập chờn (độ phân giải mtime trên Windows) → cho phép tiêm thời gian vào test.
10. cAdvisor không đọc được container trên Docker Desktop 29 + containerd → bỏ, dùng node-exporter và `docker stats`.

#> **Cập nhật 2026-11-21:** tầng biến đổi đã chuyển từ Spark sang dbt trên Trino (ADR-0003). Mục 1–5 ghi kết quả của bản
> Spark (đến 2026-11-20), giữ làm bằng chứng lịch sử. Kết quả của bản hiện tại ở **§6**.

# 7. Chưa kiểm thử hoặc còn thiếu

- **`ci.yml` đã xanh trên GitHub** (run 44488710698). Trước đó có 2 lỗi thật, đã sửa:
  - runner nhận HTTP lỗi từ Maven Central → thêm retry và mirror;
  - entrypoint tương đối và thiếu `spark-defaults.conf` khi `docker run`.
- `integration.yml` chưa chạy trên GitHub.
- **Chưa chạy toàn bộ stack cùng lúc:** RAM 15,8 GB không đủ khi bật mọi profile. Các profile đã được kiểm thử tuần tự.
- **Chưa đo một ngày đầy đủ ở chế độ analysis** (~5,2 triệu result/ngày). Bộ nhớ Silver cho quy mô đó cần đo lại; có thể phải tính theo (ngày × measurement).
- **Nessie GC** (dọn snapshot và file cũ) chưa triển khai.
- **Ghi nhiều bảng nguyên tử bằng nhánh Nessie** (ghi trên nhánh, kiểm tra, rồi merge vào `main`) chưa triển khai. Hiện dùng snapshot pinning cho chiều Silver → Gold.

## 6. Chuyển Spark → dbt + Trino (2026-11-21)

Cách kiểm: chạy bản dbt trên cùng dữ liệu Bronze mà bản Spark đã xử lý, rồi so với snapshot Spark ngay trước đó bằng
Iceberg time travel (`FOR VERSION AS OF`), `EXCEPT` hai chiều trên mọi cột (trừ run id, thời điểm nạp, khoá thay thế).

| Phép kiểm | Kết quả |
|---|---|
| Bronze: nạp lại 1 giờ Spark đã nạp (`--reprocess`, 4 manifest, 10.866 dòng) | `rows_inserted = 0`, `rows_already_present = 10.866`: `record_uid` tính giống hệt Spark |
| Bronze metadata: nạp lại 3 ngày manifest | 3 manifest đã nạp → 0 dòng mới; 1 manifest Spark chưa từng nạp → 200 dòng (đúng) |
| Bronze: test lô (số dòng, `load_id`, tỷ lệ quarantine) | 3/3 PASS mỗi entity |
| Silver `fact_ping_result` ngày 20/11 (76.810 dòng) | 0 dòng lệch mỗi chiều |
| Silver `fact_ping_reply` ngày 20/11 (230.201 dòng) | 0 dòng lệch mỗi chiều |
| `dim_probe` | Lệch 200/201 dòng: do Bronze vừa có thêm 200 snapshot metadata mà lần Spark trước chưa thấy, không phải do logic |
| Gold ngày 20/11, 7 mart (so số thực làm tròn 4 chữ số) | 6 mart khớp tuyệt đối; `ipv4_ipv6_paired_hourly` lệch 9/2.712 dòng ở chữ số thứ tư của tổng RTT (thứ tự cộng số thực) |
| `crosscheck` 19, 20, 21/11 | PASS cả 3 ngày; sai số percentile lớn nhất 0,96% / 0,95% / 0,93% (ngưỡng 2%) |
| `dq_block_on_branch.sh 2026-11-20` (nhánh `dqtest`) | PASS: Silver exit 2 + `blocked`, snapshot công bố giữ nguyên, Gold dựng từ bản cũ (28/28 dòng), ngày rỗng không ghi đè, `main` không đổi |
| Unit test runner + ingestion | 73 passed (18 mới cho runner) |
| `dbt parse` trong image | OK |

Thời gian và RAM (dev-200-v1, máy 15,8 GB, Docker 8,2 GB):

| Việc | Thời gian | RAM |
|---|---|---|
| Silver 3 ngày (19/11: 387 nghìn result, 1,17 triệu reply) | 104 giây | Trino chạm giới hạn 3 GiB (JVM giữ heap), không lỗi; container transform ≤ 168 MiB |
| Gold 3 ngày | 59 giây | như trên |
| Silver 1 ngày nhỏ (20/11) | ≈ 35 giây | |
| Image | `ripe-atlas-transform:0.1` 762 MB (image Spark trước đó ≈ 6,5 GB) | |

Khác biệt có chủ đích so với bản Spark:
- Khoá thay thế `probe_sk`/`measurement_sk` dùng `xxhash64` của Trino nên khác giá trị cũ; mọi ngày đã được tính lại.
- JSON: Trino `try_cast` chấp nhận số dạng chuỗi (`"prb_id":"12"`), Spark trước đó coi là corrupt (`docs/data-contracts.md`).
- Không còn container Spark chạy thường trực; đổi lại Trino là service core và gánh cả biến đổi.

### 6.1 Sửa sau review (2026-11-21)

| Vấn đề | Sửa | Kiểm chứng |
|---|---|---|
| Ghi lại một ngày = xóa rồi chèn (2 commit): lỗi giữa chừng để lại ngày trống | Chiến lược `replace_day`: một `MERGE` mỗi bảng | Mỗi bảng tăng đúng 1 snapshot (20 → 21, `overwrite`); dòng cũ không còn trong bản mới bị xóa (chèn dòng giả trên nhánh `dqtest` → 0 sau khi chạy Gold); không trùng khóa (87.799 / 87.799 `result_id`) |
| Bảng tạm dùng chung: chạy tay song song có thể đè nhau | Schema tạm `work_<run_id>` mỗi lượt, xóa khi xong; `maintenance` dọn bản sót. Vòng review 2: bảng `__dbt_tmp` của dbt cũng vào schema đó (`trino__make_temp_relation`), MERGE chỉ lấy dòng đúng ngày | Query MERGE đọc `work_gold_<run>.ipv4_ipv6_paired_hourly__dbt_tmp`; sau khi chạy không còn bảng `%dbt_tmp%` nào; test đơn vị cho tên schema và luật dọn |
| Dừng task Airflow không dừng query Trino | Client tag Trino riêng mỗi task (`TRINO_CLIENT_TAG`); `on_kill` chỉ hủy query mang tag đó | Hai lượt chạy song song: hủy theo tag B → B `USER_CANCELED`, C (crosscheck) vẫn PASS, schema tạm của B tự xóa. Chưa thử bằng cách giết task trong Airflow |
| MERGE có thể INSERT lại dòng cờ xóa nếu khóa có NULL | `WHEN NOT MATCHED AND NOT s.dbt_delete_flag THEN INSERT` | Silver/Gold 20/11 chạy lại: 87.799 / 87.799 `result_id`, 263.133 / 263.133 `reply_id` |
| `load_id` NULL lọt kiểm tra lô | `IS DISTINCT FROM` | `dbt parse`, Bronze chạy lại PASS |
| Metric `ripe_iceberg_files_rewritten` đo hiệu số | Đếm file cũ bị thay (tập file trước − sau) | Chưa chạy `maintenance` thật |
| `row_count` Gold là tổng cả ngày | Đếm theo measurement | `quality_hourly_target`: 8/measurement = 32 dòng |

Sau các sửa: Bronze nạp lại 0 dòng mới, Silver/Gold 20/11 công bố, crosscheck PASS (0,95%), `dq_block_on_branch.sh` PASS, 75 unit test PASS.

