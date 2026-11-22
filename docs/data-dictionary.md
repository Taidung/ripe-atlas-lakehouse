# Từ điển dữ liệu (data dictionary)

Mô tả **mọi bảng và mọi cột** trong kho (catalog Iceberg `nessie`). Schema lấy từ `nessie.information_schema.columns`
qua Trino ngày 2026-11-20: 20 bảng, 433 cột. Quy tắc chi tiết nằm ở tài liệu gốc:

- Staging, Bronze, quarantine: [`data-contracts.md`](data-contracts.md)
- Silver (SCD2, as-of join, chuẩn hoá): [`silver-model.md`](silver-model.md)
- Gold, công thức metric: [`metric-dictionary.md`](metric-dictionary.md)

## Quy ước

| Quy ước | Ý nghĩa |
|---|---|
| **Kiểu** | Kiểu như Trino hiển thị. `timestamptz` = `timestamp(6) with time zone`, lưu UTC. `timestamp` (không múi giờ) chỉ dùng cho giờ địa phương VN |
| **Null** | `có` = cột có thể NULL. Bảng Bronze/Silver/Gold do dbt tạo bằng `CREATE TABLE AS` nên mọi cột đều khai báo nullable; các cột khoá thực tế luôn có giá trị (dbt test `not_null`) |
| **Giờ VN** | Asia/Ho_Chi_Minh (UTC+7). `ictrl_dt` = **ngày theo giờ VN** của sự kiện, dùng làm partition |
| **measurement** | `1001` IPv4→K-root, `1004` IPv4→F-root, `2001` IPv6→K-root, `2004` IPv6→F-root |
| **`af`** | Address family: `4` (IPv4) hoặc `6` (IPv6) |
| **Không xác định** | Silver dùng `NULL` cho quốc gia hoặc ASN chưa biết, khoá surrogate `-1` cho unknown member. Gold dùng `ZZ` cho quốc gia và `-1` cho ASN chưa biết, để giữ được nhóm khi `GROUP BY` |
| **Cột truy vết** | `*_run_id` = mã lần chạy; `manifest_path`, `source_object` = biên nhận và file staging gốc |

## Tổng quan

| Bảng | Tầng | 1 dòng = | Khoá | Partition |
|---|---|---|---|---|
| `bronze.ping_result_raw` | Bronze | 1 phiên bản payload của 1 kết quả ping | `record_uid` | `ictrl_dt`, `measurement_id` |
| `bronze.probe_metadata_raw` | Bronze | 1 lần chụp metadata của 1 probe | `record_uid` | `ictrl_dt` |
| `bronze.measurement_metadata_raw` | Bronze | 1 lần chụp metadata của 1 measurement | `record_uid` | `ictrl_dt` |
| `bronze.quarantine` | Bronze | 1 bản ghi bị loại vì lỗi | `quarantine_uid` | `entity`, `ictrl_dt` |
| `bronze.ingest_manifest` | Bronze | 1 lần nạp 1 biên nhận (manifest) | `manifest_path` + `bronze_run_id` | `entity`, `ictrl_dt` |
| `silver.fact_ping_result` | Silver | 1 kết quả ping (probe × measurement × thời điểm) | `result_id` | `ictrl_dt`, `measurement_id` |
| `silver.fact_ping_reply` | Silver | 1 gói trong 1 kết quả ping | `reply_id` | `ictrl_dt`, `measurement_id` |
| `silver.dim_probe` | Silver | 1 phiên bản (SCD2) thông tin của 1 probe | `probe_sk` | — |
| `silver.dim_measurement` | Silver | 1 phiên bản (SCD2) cấu hình của 1 measurement | `measurement_sk` | — |
| `silver.dim_date` | Silver | 1 ngày (2010–2035) | `date_key` | — |
| `silver.dim_time` | Silver | 1 phút trong ngày (1.440 dòng) | `time_key` | — |
| `gold.quality_hourly_target` | Gold | giờ × cohort × measurement | 7 cột nhóm | `ictrl_dt` |
| `gold.quality_hourly_country` | Gold | giờ × cohort × measurement × quốc gia | | `ictrl_dt` |
| `gold.quality_hourly_asn` | Gold | giờ × cohort × measurement × ASN | | `ictrl_dt` |
| `gold.quality_hourly_probe` | Gold | giờ × cohort × measurement × probe | | `ictrl_dt` |
| `gold.rtt_histogram_hourly` | Gold | giờ × cohort × measurement × quốc gia × ASN × ô RTT | | `ictrl_dt` |
| `gold.ipv4_ipv6_paired_hourly` | Gold | giờ × cohort × target × probe | | `ictrl_dt` |
| `gold.coverage_hourly` | Gold | giờ × cohort × measurement (đủ 24 giờ, kể cả giờ trống) | | `ictrl_dt` |
| `ops.partition_status` | ops | trạng thái công bố của 1 bảng × ngày × measurement | `table_name` + `ictrl_dt` + `measurement_id` | — |
| `ops.dq_result` | ops | 1 kết quả kiểm tra chất lượng | `dq_run_id` + `check_name` + bảng | `days(checked_at)` |
| `ops.probe_set_member` | ops (seed) | 1 probe thuộc 1 probe set theo af | `probe_set_version` + `af` + `probe_id` | — |
| `ops.asn_info` | ops (seed) | 1 ASN: `as_holder` (RIPEstat), `isp_name` (PeeringDB / viết tay cho VN), `isp_type` | `asn` | — |

---

## 1. Bronze

### `bronze.ping_result_raw`

Kết quả ping **nguyên văn** từ API, đã qua kiểm tra hợp lệ. MERGE theo `record_uid`, nên chạy lại không sinh trùng.

| Cột | Kiểu | Null | Ý nghĩa |
|---|---|---|---|
| `record_uid` | varchar | không | `sha256(measurement_id \| probe_id \| event_ts \| raw_sha256)`. Cùng kết quả nhưng nội dung khác (nguồn sửa) cho `record_uid` khác |
| `measurement_id` | integer | không | Measurement của kết quả |
| `probe_id` | bigint | không | Probe đo |
| `af` | integer | không | 4 hoặc 6 |
| `event_ts` | bigint | không | Thời điểm đo, Unix giây (theo đồng hồ probe) |
| `event_time` | timestamptz | không | `event_ts` dạng timestamp UTC |
| `stored_ts` | bigint | có | Thời điểm RIPE Atlas nhận kết quả, Unix giây |
| `ictrl_dt` | date | không | Ngày giờ VN của `event_time` |
| `window_start` | timestamptz | không | Đầu khung 1 giờ (UTC) của job ingestion đã lấy kết quả này |
| `raw_json` | varchar | không | Nguyên văn dòng JSON từ API |
| `raw_sha256` | varchar | không | SHA-256 của `raw_json`; Bronze tính lại để kiểm toàn vẹn |
| `change_type` | varchar | không | `new` (lần đầu thấy) hoặc `changed` (cùng probe + thời điểm, nội dung khác) |
| `fw` | integer | có | Phiên bản firmware probe |
| `mver` | varchar | có | Phiên bản định dạng kết quả |
| `source_object` | varchar | không | File `jsonl.gz` trong staging chứa dòng này |
| `manifest_path` | varchar | không | Biên nhận của job đã ghi file đó |
| `ingest_run_id` | varchar | không | Lần chạy ingestion |
| `dlt_load_id` | varchar | không | Mã lần ghi của dlt |
| `fetched_at` | timestamptz | không | Lúc gọi API lấy về |
| `probe_set_version` | varchar | không | Probe set của job đã lấy kết quả |
| `bronze_run_id` | varchar | không | Lần chạy Bronze đã nạp |
| `bronze_loaded_at` | timestamptz | không | Lúc nạp vào Bronze |

### `bronze.probe_metadata_raw` và `bronze.measurement_metadata_raw`

Mỗi lần chụp metadata (hằng ngày). Hai bảng cùng cấu trúc; khác nhau ở cột khoá thực thể.

| Cột | Kiểu | Null | Ý nghĩa |
|---|---|---|---|
| `record_uid` | varchar | không | `sha256(id thực thể \| run_id)` |
| `probe_id` / `measurement_id` | bigint / integer | không | Thực thể được chụp |
| `fetch_status` | varchar | không | `ok`, hoặc `not_returned` khi probe được hỏi mà API không trả về (probe đã gỡ hoặc chuyển private) |
| `is_public` | boolean | có | Cờ public do API trả về |
| `raw_json` | varchar | có | Probe: JSON canonical của một phần tử trong trang kết quả. Measurement: nguyên văn body. `NULL` khi `not_returned` |
| `raw_sha256` | varchar | có | SHA-256 của `raw_json` |
| `raw_encoding` | varchar | không | `canonical_json`, `http_body` hoặc `none` |
| `observed_at` | timestamptz | không | Lúc chụp |
| `ictrl_dt` | date | không | Ngày giờ VN của lần chụp |
| `source_object`, `manifest_path`, `ingest_run_id`, `dlt_load_id`, `probe_set_version` | varchar | không | Truy vết, như `ping_result_raw` |
| `bronze_run_id`, `bronze_loaded_at` | varchar, timestamptz | không | Lần nạp Bronze |

### `bronze.quarantine`

Bản ghi bị loại vì lỗi mức bản ghi. Lỗi mức cả lô thì job dừng và không ghi gì; xem `data-contracts.md` §4.

| Cột | Kiểu | Null | Ý nghĩa |
|---|---|---|---|
| `quarantine_uid` | varchar | không | Khoá chống trùng của bản ghi lỗi |
| `entity` | varchar | không | `ping_result_raw`, `probe_metadata_raw` hoặc `measurement_metadata_raw` |
| `reason` | varchar | không | `invalid_utf8`, `hash_mismatch`, `malformed_json`, `missing_key`, `wrong_measurement`, `af_mismatch`, `wrong_type`, `out_of_window`, `id_mismatch` |
| `measurement_id` | integer | có | Measurement dự kiến của job (kết quả ping) |
| `entity_id` | bigint | có | `probe_id` hoặc id thực thể, nếu đọc được |
| `event_ts` | bigint | có | Thời điểm đo, nếu đọc được |
| `raw_json`, `raw_sha256` | varchar | có | Nội dung gốc, giữ để xử lý lại |
| `source_object`, `manifest_path`, `ingest_run_id` | varchar | có (trừ `manifest_path`) | Truy vết |
| `ictrl_dt` | date | không | Ngày của biên nhận |
| `detected_at` | timestamptz | không | Lúc phát hiện |
| `bronze_run_id` | varchar | không | Lần chạy Bronze phát hiện |
| `reprocess_status` | varchar | không | `pending` (chờ xử lý lại) |

### `bronze.ingest_manifest`

Nhật ký **append-only**: mỗi lần nạp một biên nhận là một dòng. Chạy lại với `--reprocess` thêm dòng mới, không ghi đè.

| Cột | Kiểu | Null | Ý nghĩa |
|---|---|---|---|
| `manifest_path` | varchar | không | Đường dẫn biên nhận trong staging |
| `entity` | varchar | không | `ping_result_raw`, `probe_metadata_raw`, `measurement_metadata_raw` |
| `ingest_run_id` | varchar | không | Lần chạy ingestion tạo biên nhận |
| `run_type` | varchar | có | `scheduled`, `backfill`, `reconcile`, `repair`, `manual` |
| `load_id` | varchar | có | Mã lần ghi dlt. **`NULL` khi job không có gì mới để ghi** (mọi bản ghi đều trùng, `staged = 0`) |
| `measurement_id` | integer | có | **`NULL` với biên nhận metadata** (không theo từng measurement) |
| `window_start`, `window_end` | timestamptz | có | Khung giờ của job kết quả ping; `NULL` với metadata |
| `ictrl_dt` | date | không | Ngày giờ VN của khung (hoặc của lần chụp metadata) |
| `probe_set_version`, `probes_requested` | varchar, integer | có | Probe set và số probe đã hỏi |
| `request_count` | integer | có | Số request API của job |
| `fetched` | bigint | có | Số kết quả API trả về |
| `staged` | bigint | có | Số bản ghi đã ghi vào staging |
| `unchanged` | bigint | có | Số bản ghi trùng với lần trước nên bỏ qua |
| `changed` | bigint | có | Số bản ghi nguồn đã sửa |
| `object_count`, `object_bytes` | integer, bigint | có | Số file và tổng dung lượng file của job |
| `rows_read` | bigint | có | Số dòng Bronze đọc từ các file đó |
| `rows_valid` | bigint | có | Số dòng hợp lệ |
| `rows_inserted` | bigint | có | Số dòng thực sự được thêm vào Bronze |
| `rows_already_present` | bigint | có | Số dòng đã có từ trước (`valid − inserted`) |
| `rows_quarantined` | bigint | có | Số dòng bị đưa vào quarantine |
| `ingest_started_at`, `ingest_finished_at` | timestamptz | có | Thời gian job ingestion |
| `bronze_status` | varchar | không | `loaded` |
| `bronze_run_id`, `bronze_loaded_at` | varchar, timestamptz | không | Lần nạp Bronze |
| `manifest_json` | varchar | không | Toàn văn biên nhận |

---

## 2. Silver

### `silver.fact_ping_result`

**1 dòng = 1 kết quả ping**, đã chuẩn hoá và gắn thông tin probe đúng tại thời điểm đo (as-of join).

| Cột | Kiểu | Null | Ý nghĩa |
|---|---|---|---|
| `result_id` | varchar | không | `sha256("ping_result" \| measurement_id \| probe_id \| event_ts)`. Khoá ổn định của kết quả |
| `measurement_id`, `af`, `target` | integer, integer, varchar | không | Measurement, address family, đích (`k.root-servers.net` / `f.root-servers.net`) |
| `probe_id` | bigint | không | Probe đo |
| `event_ts`, `event_time` | bigint, timestamptz | không | Thời điểm đo (Unix giây và UTC) |
| `event_hour_utc` | timestamptz | không | `event_time` làm tròn xuống giờ |
| `ictrl_dt` | date | không | Ngày giờ VN |
| `date_key`, `time_key` | integer | không | `yyyyMMdd` và `HHmm` theo giờ VN; nối với `dim_date`, `dim_time` |
| `packets_sent` | integer | có | Số gói gửi (`sent`) |
| `packets_received` | integer | có | Số gói nhận hợp lệ (`rcvd`, không tính trùng) |
| `packets_lost` | integer | có | `sent − rcvd` khi `sent > 0` và `rcvd ≤ sent`; ngược lại `NULL` |
| `duplicate_count_src` | integer | có | Trường `dup` của nguồn |
| `result_status` | varchar | không | `all_received`, `partial_loss`, `all_lost`, `send_error` (`sent = 0`), `inconsistent` (bị chặn công bố) |
| `has_response` | boolean | không | `rcvd > 0` |
| `src_rtt_min`, `src_rtt_avg`, `src_rtt_max` | double | có | Giá trị nguồn giữ nguyên, kể cả `-1` |
| `rtt_min_ms`, `rtt_avg_ms`, `rtt_max_ms` | double | có | Như trên nhưng `-1` (không có) đổi thành `NULL` |
| `reply_element_count` | integer | không | Số phần tử trong mảng `result` |
| `valid_rtt_count` | integer | không | Số gói có RTT hợp lệ (có `rtt ≥ 0`, không trùng) |
| `rtt_sum_ms`, `rtt_sumsq_ms2` | double | có | Tổng và tổng bình phương RTT hợp lệ, để tính trung bình và độ lệch chuẩn khi gộp |
| `duplicate_reply_count`, `timeout_count`, `error_count`, `unknown_element_count` | integer | không | Đếm phần tử theo loại |
| `counts_consistent` | boolean | không | `valid_rtt_count = packets_received` |
| `ttl`, `packet_size`, `proto`, `dst_addr` | integer, integer, varchar, varchar | có | Trường nguồn. IP của probe **không** được lưu |
| `fw`, `mver`, `lts` | integer, varchar, bigint | có | Firmware, phiên bản định dạng, `lts` (giây kể từ lần đồng bộ đồng hồ) |
| `stored_ts`, `ingest_delay_s` | bigint | có | Lúc RIPE nhận; `ingest_delay_s = stored_ts − event_ts` |
| `clock_suspect` | boolean | không | `lts < 0` hoặc `stored_ts < event_ts`: đồng hồ probe có thể lệch |
| `source_version_count`, `is_source_corrected` | integer, boolean | không | Số phiên bản nội dung khác nhau trong Bronze; `> 1` = nguồn đã sửa, Silver chọn bản lấy về muộn nhất |
| `record_uid`, `raw_sha256`, `ingest_run_id`, `probe_set_version` | varchar | không | Truy vết về bản Bronze được chọn |
| `silver_run_id`, `silver_loaded_at` | varchar, timestamptz | không | Lần chạy Silver |
| `probe_sk` | bigint | không | Phiên bản `dim_probe` hiệu lực tại `event_time`; `-1` = chưa có metadata |
| `measurement_sk` | bigint | không | Phiên bản `dim_measurement` hiệu lực tại `event_time` |
| `country_code`, `asn` | varchar, bigint | có | Quốc gia và ASN của probe tại thời điểm đo. `asn` = `asn_v4` nếu IPv4, `asn_v6` nếu IPv6 |
| `public_status` | varchar | không | `public`, `not_public` hoặc `unknown` |
| `metadata_basis` | varchar | không | `asof_snapshot`, `assumed_from_first_snapshot` (đo trước lần chụp metadata đầu tiên) hoặc `no_metadata` |

### `silver.fact_ping_reply`

**1 dòng = 1 gói** trong một kết quả ping (thường 3 gói mỗi kết quả). Dùng cho phân vị RTT.

| Cột | Kiểu | Null | Ý nghĩa |
|---|---|---|---|
| `reply_id` | varchar | không | `sha256(result_id \| reply_seq)` |
| `result_id` | varchar | không | Kết quả chứa gói này (nối với `fact_ping_result`) |
| `reply_seq` | integer | không | Thứ tự gói, bắt đầu từ 0 |
| `reply_type` | varchar | không | `reply` (bình thường), `duplicate` (trả lời trùng), `timeout`, `error` |
| `rtt_ms` | double | có | RTT của gói (ms); `NULL` với `timeout` và `error` |
| `is_valid_rtt` | boolean | không | `reply_type = reply` và `rtt ≥ 0`. Chỉ gói hợp lệ được tính vào RTT |
| `reply_ttl` | integer | có | TTL của gói trả lời |
| `error_message` | varchar | có | Nội dung lỗi khi `reply_type = error` |
| `measurement_id`, `af`, `probe_id`, `probe_sk` | | không | Lặp lại từ kết quả để lọc không cần join |
| `event_time`, `event_hour_utc`, `ictrl_dt`, `date_key` | | không | Như `fact_ping_result` |
| `country_code`, `asn` | varchar, bigint | có | Như `fact_ping_result` |
| `public_status` | varchar | không | Như `fact_ping_result` |
| `silver_run_id`, `silver_loaded_at` | varchar, timestamptz | không | Lần chạy Silver |

### `silver.dim_probe` và `silver.dim_measurement` (SCD Type 2)

Mỗi dòng là một **phiên bản**: chỉ khi một thuộc tính theo dõi thay đổi mới mở phiên bản mới. Mỗi bảng có thêm một
dòng *unknown member* (`*_sk = -1`) cho kết quả của thực thể chưa có metadata.

Cột chung của hai bảng:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `probe_id` / `measurement_id` | bigint / integer | Khoá tự nhiên |
| `probe_sk` / `measurement_sk` | bigint | Khoá surrogate, `xxhash64(thực thể \| id \| valid_from)`; ổn định khi dựng lại; `-1` = unknown member |
| `version_no` | bigint | Thứ tự phiên bản, bắt đầu từ 1 |
| `valid_from` | timestamptz | Lần chụp đầu tiên thấy giá trị của phiên bản này |
| `valid_to` | timestamptz | `valid_from` của phiên bản kế tiếp; phiên bản hiện tại là `9999-12-31` |
| `join_from` | timestamptz | Mốc dùng khi as-of join: phiên bản 1 là `1970-01-01` (để khớp kết quả đo trước lần chụp đầu), các phiên bản sau = `valid_from` |
| `is_current` | boolean | Phiên bản đang hiệu lực |
| `last_observed_at` | timestamptz | Lần chụp cuối cùng còn thấy giá trị này |
| `observation_count` | bigint | Số lần chụp thuộc phiên bản |
| `prev_version_last_observed_at` | timestamptz | Lần chụp cuối của phiên bản trước; thay đổi thật xảy ra trong khoảng (mốc này, `valid_from`] |
| `attr_hash` | varchar | SHA-256 các thuộc tính theo dõi, dùng phát hiện thay đổi |
| `valid_from_basis` | varchar | `observed_at`, hoặc `unknown_member` |
| `is_unknown_member` | boolean | Dòng unknown member |

Thuộc tính theo dõi riêng:

| Bảng | Cột | Kiểu | Ý nghĩa |
|---|---|---|---|
| `dim_probe` | `country_code` | varchar | Quốc gia của probe |
| | `asn_v4`, `asn_v6` | bigint | Nhà mạng (ASN) cho IPv4 và IPv6 |
| | `prefix_v4`, `prefix_v6` | varchar | Prefix mạng |
| | `is_public` | boolean | Probe public |
| | `is_anchor` | boolean | Probe là anchor (thiết bị đo cố định của RIPE) |
| | `latitude`, `longitude` | double | Toạ độ, làm tròn 4 chữ số |
| `dim_measurement` | `af`, `target`, `target_ip` | integer, varchar, varchar | Address family, đích, IP đích |
| | `interval_s` | integer | Chu kỳ đo (giây), thường 240 |
| | `packets`, `packet_size` | integer | Số gói mỗi lần đo, kích thước gói |
| | `is_public` | boolean | Measurement public |
| | `status_name`, `msm_type` | varchar | Trạng thái (vd `Ongoing`), loại (`ping`) |

### `silver.dim_date` và `silver.dim_time`

| Bảng | Cột | Kiểu | Ý nghĩa |
|---|---|---|---|
| `dim_date` | `date_key` | integer | `yyyyMMdd` |
| | `full_date` | date | Ngày |
| | `year`, `quarter`, `month`, `day` | integer | Năm, quý, tháng, ngày |
| | `month_name`, `day_name` | varchar | Tên tháng, tên thứ (tiếng Anh) |
| | `day_of_week` | integer | 1 = Thứ Hai … 7 = Chủ nhật |
| | `iso_week` | integer | Tuần ISO |
| | `is_weekend` | boolean | Thứ Bảy hoặc Chủ nhật |
| `dim_time` | `time_key` | integer | `HHmm` |
| | `hour`, `minute` | integer | Giờ, phút |
| | `hour_label` | varchar | `HH:00` |
| | `part_of_day` | varchar | `night` (0–5h), `morning` (6–11h), `afternoon` (12–17h), `evening` (18–23h) |

---

## 3. Gold

Gold chỉ tính kết quả **đủ điều kiện**: probe public, đồng hồ không lệch, thuộc cohort (probe set) đã thực sự được
ingest giờ đó. Gold **chỉ lưu tử số và mẫu số cộng được**; tỷ lệ % được tính lúc truy vấn bằng biểu thức trong
`config/metrics.json` (xem `metric-dictionary.md` §2).

### Cột nhóm thời gian dùng chung

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `ictrl_dt` | date | Ngày giờ VN (partition) |
| `event_hour_utc` | timestamptz | Giờ, UTC |
| `event_hour_local` | timestamp | Cùng giờ đó theo giờ VN, không múi giờ (cho Superset và Trino hiển thị) |
| `probe_set_version` | varchar | Cohort (probe set): hiện chỉ có `dev-200-v2`. Dashboard **phải chọn đúng một** |
| `measurement_id`, `af`, `target` | integer, integer, varchar | Measurement |
| `gold_run_id`, `gold_built_at` | varchar, timestamptz | Lần chạy Gold |

### Cột đo dùng chung của 4 mart chất lượng

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `results_total` | bigint | Số kết quả đủ điều kiện |
| `results_measured` | bigint | Số kết quả đã gửi được gói (`sent > 0`), là mẫu số của tỷ lệ có phản hồi |
| `results_send_error` | bigint | Số kết quả không gửi được gói nào |
| `results_with_response` | bigint | Số kết quả đã gửi và nhận ít nhất 1 gói |
| `results_all_lost` | bigint | Số kết quả mất toàn bộ gói |
| `results_partial_loss` | bigint | Số kết quả mất một phần |
| `packets_sent`, `packets_received`, `packets_lost` | bigint | Tổng gói (chỉ tính kết quả đã đo). Tỷ lệ mất gói = `packets_lost / packets_sent` |
| `rtt_count` | bigint | Số RTT hợp lệ |
| `rtt_sum_ms`, `rtt_sumsq_ms2` | double | Tổng và tổng bình phương RTT. RTT trung bình = `rtt_sum_ms / rtt_count` |
| `rtt_min_ms`, `rtt_max_ms` | double | RTT nhỏ nhất, lớn nhất |
| `probes_observed` | bigint | Số probe có ít nhất một kết quả đủ điều kiện |

### `gold.quality_hourly_target`

Nhóm theo giờ × cohort × measurement. Thêm:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `countries_observed` | bigint | Số quốc gia (không tính `ZZ`) |
| `asns_observed` | bigint | Số ASN (không tính `-1`) |

### `gold.quality_hourly_country`

Nhóm thêm theo `country_code` (varchar, `ZZ` = chưa biết). Thêm `asns_observed`.

### `gold.quality_hourly_asn`

Nhóm thêm theo `asn` (bigint, `-1` = chưa biết). Thêm:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `asn_country_count` | bigint | Số quốc gia có probe thuộc ASN này |
| `asn_primary_country` | varchar | Mã quốc gia nhỏ nhất theo thứ tự chữ cái trong các quốc gia đó (để hiển thị) |
| `isp_name` | varchar | Tên nhà mạng (seed `ops.asn_info`); `AS<n>` nếu ASN chưa có trong seed, `Chưa biết` nếu `asn = -1` |
| `isp_type` | varchar | Loại mạng: `ISP`, `Hosting/DC`, `Giáo dục/Nghiên cứu`, `Doanh nghiệp`, `Khác`; ASN Việt Nam phân loại tay, ASN khác theo PeeringDB; `NULL` nếu chưa rõ |

### `gold.quality_hourly_probe`

Nhóm thêm theo `probe_id`. Thêm:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `probe_sk` | bigint | Phiên bản `dim_probe` (lớn nhất trong giờ) |
| `country_code`, `asn` | varchar, bigint | Quốc gia, ASN của probe trong giờ |
| `isp_name`, `isp_type` | varchar, varchar | Nhà mạng theo ASN, như `gold.quality_hourly_asn` |

### `gold.rtt_histogram_hourly`

Đếm RTT hợp lệ (từ `fact_ping_reply`) vào các ô tăng theo cấp số nhân, mỗi ô rộng **2%**. Phân vị p50/p95/p99 được tính
bằng `approx_percentile(bucket_rep_ms, reply_count, p)`; sai số đo được dưới 1%.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| cột nhóm thời gian + `country_code`, `asn` | | Như trên |
| `bucket_id` | integer | `floor(ln(max(rtt, 0.1) / 0.1) / ln(1.02))` |
| `bucket_lower_ms`, `bucket_upper_ms` | double | Biên ô: `0.1 × 1.02^b` và `0.1 × 1.02^(b+1)` (ô 0 bắt đầu từ 0) |
| `bucket_rep_ms` | double | Giá trị đại diện ô: `0.1 × 1.02^(b+0.5)` |
| `reply_count` | bigint | Số gói có RTT rơi vào ô |

### `gold.ipv4_ipv6_paired_hourly`

So IPv4 với IPv6 trên **cùng một probe, cùng đích**: kết quả IPv4 và IPv6 gần nhau nhất (theo cả hai chiều), lệch
nhau ≤ 120 giây, được ghép thành cặp. Nhóm theo giờ × cohort × `target` × `probe_id`.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `target`, `probe_id` | varchar, bigint | Đích, probe |
| `measurement_id_v4`, `measurement_id_v6` | integer | Cặp measurement của đích (1001/2001 hoặc 1004/2004) |
| `pairs` | bigint | Số cặp ghép được |
| `pair_dt_mean_s` | double | Độ lệch thời gian trung bình giữa hai kết quả của cặp (giây) |
| `country_code`, `asn_v4`, `asn_v6` | varchar, bigint, bigint | Quốc gia, ASN IPv4 và IPv6 của probe |
| `v4_packets_sent`, `v4_packets_lost`, `v4_rtt_count`, `v4_rtt_sum_ms` | | Tổng phía IPv4 của các cặp |
| `v6_packets_sent`, `v6_packets_lost`, `v6_rtt_count`, `v6_rtt_sum_ms` | | Tổng phía IPv6 của các cặp |
| `unpaired_v4_results`, `unpaired_v6_results` | bigint | Số kết quả không tìm được cặp |

### `gold.coverage_hourly`

Lưới **đủ 24 giờ × 4 measurement** cho mỗi cohort được ingest trong ngày, kể cả giờ không có dữ liệu. Dùng để biết dữ
liệu của giờ đó đã đủ chưa và vì sao thiếu.

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| cột nhóm thời gian | | Như trên |
| `interval_s` | integer | Chu kỳ đo của measurement (từ `dim_measurement` hiện hành, mặc định 240) |
| `expected_probes` | bigint | Số probe của cohort cho address family đó (0 nếu probe set không có probe IPv6) |
| `expected_results` | bigint | `expected_probes × 3600 / interval_s`, là mẫu số của độ đầy đủ ước tính |
| `results_total_raw` | bigint | Mọi kết quả của cohort trong giờ, trước khi lọc |
| `results_eligible` | bigint | Kết quả đủ điều kiện (bằng `results_total` của `quality_hourly_target`) |
| `results_excluded_not_public` | bigint | Bị loại vì probe không public |
| `results_excluded_clock` | bigint | Bị loại vì đồng hồ probe lệch |
| `probes_observed` | bigint | Số probe có kết quả đủ điều kiện |
| `results_source_corrected` | bigint | Số kết quả nguồn đã sửa |
| `max_event_time` | timestamptz | Thời điểm đo mới nhất trong giờ |
| `silver_loaded_at` | timestamptz | Lần nạp Silver mới nhất |
| `manifests` | bigint | Số biên nhận đã nạp cho giờ đó |
| `overlap_unchanged_skipped` | bigint | Số bản ghi trùng bị bỏ qua khi tải chồng lấn |
| `silver_status` | varchar | Trạng thái Silver của ngày × measurement (`published`, `blocked`, `not_built`…) |
| `quarantine_records` | bigint | Số bản ghi bị quarantine trong giờ |
| `data_state` | varchar | `measured` (có dữ liệu), `processed_no_results` (đã xử lý, API không trả gì), `not_processed` (chưa ingest giờ này) |

---

## 4. ops

### `ops.partition_status`

Sổ **công bố**: tầng sau chỉ đọc phần đã công bố của tầng trước, đúng snapshot ghi ở đây.

| Cột | Kiểu | Null | Ý nghĩa |
|---|---|---|---|
| `layer` | varchar | không | `silver` hoặc `gold` |
| `table_name` | varchar | không | Bảng, ví dụ `nessie.silver.fact_ping_result` |
| `ictrl_dt`, `measurement_id` | date, integer | không | Partition |
| `status` | varchar | không | Kết quả **lần chạy gần nhất**: `published`, `published_with_warnings`, `blocked`, `blocked_upstream` |
| `row_count` | bigint | có | Số dòng của **bản công bố gần nhất**, đúng partition (bảng, ngày, measurement); mart ghép cặp IPv4/IPv6 tính mỗi cặp cho cả hai measurement |
| `snapshot_id` | bigint | có | Snapshot Iceberg của **bản công bố gần nhất**; lần chạy bị chặn không xoá giá trị này. `NULL` = chưa từng công bố |
| `pipeline_run_id`, `dq_run_id` | varchar | không | Lần chạy và lần kiểm tra chất lượng gần nhất |
| `reason` | varchar | có | Lý do chặn, hoặc ghi chú (vd Gold dựng từ bản Silver cũ) |
| `updated_at` | timestamptz | không | Lúc cập nhật |

### `ops.dq_result`

Mọi kết quả kiểm tra chất lượng, cả đạt lẫn không đạt.

| Cột | Kiểu | Null | Ý nghĩa |
|---|---|---|---|
| `dq_run_id`, `pipeline_run_id` | varchar | không | Lần kiểm tra, lần chạy pipeline |
| `layer`, `table_name` | varchar | không | Bảng được kiểm tra |
| `partition_scope` | varchar | không | Phạm vi, ví dụ `ictrl_dt=2026-11-19` |
| `check_name` | varchar | không | Tên dbt test, vd `not_null_fact_ping_result_candidate_result_id`, `reconcile_bronze_keys_eq_fact_rows`; hoặc kiểm tra của runner như `post_write:row_count_matches` |
| `engine` | varchar | không | `dbt-<phiên bản>` hoặc `runner` (dữ liệu trước 2026-11-21: `gx-<phiên bản>`, `spark`) |
| `severity` | varchar | không | `critical` (lỗi thì chặn công bố) hoặc `warn` (vẫn công bố, kèm cảnh báo) |
| `success` | boolean | không | Đạt hay không |
| `observed_value` | varchar | có | Giá trị quan sát |
| `unexpected_count`, `element_count` | bigint | có | Số phần tử vi phạm, tổng số phần tử |
| `details` | varchar | có | Chi tiết (JSON) |
| `target_snapshot_id` | bigint | có | Snapshot đã ghi; `NULL` nếu bị chặn |
| `checked_at` | timestamptz | không | Lúc kiểm tra |
