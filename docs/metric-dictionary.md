# Metric dictionary và thiết kế Gold

Phiên bản: v1 (giai đoạn 4, 2026-11-19). Viết **trước** khi xây marts.
Code tương ứng:
- `transformation/lakehouse/gold.py` (biến đổi)
- `transformation/jobs/gold_build.py` (job)
- `sql/gold/001_tables.sql` (DDL)
- `superset/` (dashboard)

## 1. Tập bản ghi đầu vào của Gold

Một result của Silver chỉ được tính vào Gold khi thỏa **tất cả** điều kiện sau:

| Điều kiện | Lý do | Bản ghi bị loại được đếm ở |
|---|---|---|
| Partition Silver đã có bản công bố (`snapshot_id` khác NULL); Gold đọc đúng snapshot đó | Không dùng dữ liệu bị chặn vì lỗi chất lượng. Nếu lần chạy Silver gần nhất bị chặn, Gold dùng bản công bố trước đó | `coverage_hourly.silver_status` |
| `public_status = 'public'` theo phiên bản probe as-of | Chính sách chỉ phân tích probe công khai | `results_excluded_not_public` |
| `clock_suspect = false` | `event_ts` do đồng hồ probe ghi; nếu lệch thì xếp sai giờ | `results_excluded_clock` |
| `probe_id` thuộc probe set của cohort (xem dưới) | Tái lập được kết quả theo tập mẫu | Không áp dụng |

**Cohort (`probe_set_version`):**
- Mỗi dòng Gold thuộc **một** probe set. Một result được tính vào cohort S nếu `probe_id ∈ S.probes_af` **và** probe set S
  đã thực sự được ingest cho (measurement, giờ) đó, tức có manifest đã nạp vào Bronze. Probe nằm trong hai set sẽ có mặt ở cả
  hai cohort, nhưng chỉ ở những giờ mà set đó được ingest. Probe set không được ingest trong ngày không có dòng nào
  trong Gold, kể cả `coverage_hourly`, nên không sinh cảnh báo độ đầy đủ giả.
- `coverage_hourly` luôn đủ 24 giờ × 4 measurement cho mỗi cohort có mặt; probe set không có probe IPv6 vẫn có dòng
  cho 2001/2004 với `expected_probes = 0`. Khi `expected_results = 0`, độ đầy đủ không xác định: metric
  `ripe_gold_latest_hour_completeness_ratio` không được đẩy cho nhóm đó (thay vì 0), nên không sinh cảnh báo thiếu dữ liệu giả.
- Vì vậy dashboard **bắt buộc chọn đúng một** `probe_set_version`. Filter dạng single-select, mặc định `PROBE_SET_VERSION` (`dev-200-v2`).

**Thời gian:**
- `event_hour_utc` = giờ UTC của event.
- `event_hour_local` = cùng thời điểm nhưng biểu diễn theo giờ treo tường Asia/Ho_Chi_Minh, kiểu `timestamp` không múi giờ. Đây là cột thời gian chính của dashboard.
- `ictrl_dt` = ngày theo giờ Việt Nam.

## 2. Định nghĩa metric

Ký hiệu:
- `R` = tập result đủ điều kiện.
- `R_meas` = các result trong `R` có `packets_sent > 0`, tức là probe đã gửi được packet.
- "Cộng được" = có thể cộng dồn qua giờ hoặc qua nhóm để ra giá trị đúng ở grain lớn hơn.

| Metric | Định nghĩa | Tử số | Mẫu số | Đơn vị | Grain đầu vào | Null / mẫu số = 0 | Cộng được? |
|---|---|---|---|---|---|---|---|
| `results_total` | Số result đủ điều kiện | count(R) | — | result | result | 0 | Có |
| `results_measured` | Result đã gửi được packet | count(R_meas) | — | result | result | 0 | Có |
| `results_send_error` | Result có `sent = 0` (lỗi phía probe) | count | — | result | result | 0 | Có |
| `results_with_response` | Result có ≥ 1 reply hợp lệ | count(`rcvd > 0`) | — | result | result | 0 | Có |
| `results_all_lost` | Result đã gửi nhưng mất toàn bộ packet | count(`sent > 0` và `rcvd = 0`) | — | result | result | 0 | Có |
| `results_partial_loss` | `0 < rcvd < sent` | count | — | result | result | 0 | Có |
| **`response_rate_observed`** | Tỷ lệ result có phản hồi **trên các result đã đo được** | Σ `results_with_response` | Σ `results_measured` | % | result | `NULL` khi không có result nào đo được | Tính lại từ tử và mẫu |
| `packets_sent`, `packets_received`, `packets_lost` | Σ `sent`, Σ `rcvd`, Σ(`sent − rcvd`) trên R_meas. `rcvd` không tính duplicate (đã kiểm chứng 44.340/44.340) | | | packet | result | 0 | Có |
| **`packet_loss`** | Tỷ lệ mất gói | Σ `packets_lost` | Σ `packets_sent` | % | result | `NULL` khi Σ sent = 0 | Tính lại từ tử và mẫu; **không** lấy trung bình các tỷ lệ |
| `rtt_count` | Số reply có RTT hợp lệ (không dup, rtt ≥ 0) | Σ `valid_rtt_count` | — | reply | result | 0 | Có |
| **`rtt_mean_ms`** | RTT trung bình **theo packet** | Σ `rtt_sum_ms` | Σ `rtt_count` | ms | reply | `NULL` khi `rtt_count` = 0 | Tính lại từ tử và mẫu |
| `rtt_min_ms`, `rtt_max_ms` | Nhỏ nhất / lớn nhất trong các reply hợp lệ | | | ms | reply | `NULL` | Gộp bằng min/max |
| `rtt_stddev_ms` | √(Σx²/n − (Σx/n)²) trên reply hợp lệ | Σ `rtt_sumsq`, Σ `rtt_sum`, `n` | | ms | reply | `NULL` khi n < 2 | Tính lại từ ba tổng |
| **`rtt_p50/p95/p99_ms`** | Percentile **theo packet** của RTT reply hợp lệ | — | — | ms | reply, qua histogram | `NULL` khi không có reply | **Có**, cộng `reply_count` theo bucket rồi `approx_percentile(bucket_rep_ms, reply_count, p)` |
| `probes_observed` | Số probe phân biệt có ≥ 1 result | count(distinct probe_id) | | probe | result | 0 | **Không**. Ở grain lớn hơn phải đếm lại từ `quality_hourly_probe` |
| `countries_observed`, `asns_observed` | Tương tự `probes_observed` | | | | | 0 | **Không** |
| **`completeness_est`** | Ước tính độ đầy đủ | Σ `results_total_raw` | `expected_probes × 3600 / interval_s` | % | giờ × measurement | `NULL` khi expected = 0 | Tính lại từ tử và mẫu |
| `probe_coverage` | Tỷ lệ probe trong cohort có dữ liệu | `probes_observed` | `expected_probes` | % | giờ × measurement | `NULL` | Không cộng được qua giờ |
| `data_state` | `measured`, `processed_no_results` (có manifest nhưng 0 result) hoặc `not_processed` (chưa có manifest) | | | | giờ × measurement | | |
| `freshness_lag_s` | `gold_built_at − max(event_time)` | | | giây | measurement | | |

**Ghi chú về các metric trên:**
- **Percentile:**
  - Bucket logarit: `b = floor(ln(max(rtt, 0.1) / 0.1) / ln(1.02))`. Giá trị đại diện của bucket là `0.1 × 1.02^(b + 0.5)` ms.
  - Bucket 0 gồm cả RTT = 0 (dữ liệu thật có `min = 0.0`).
  - Sai số lượng tử ≤ ≈ 1%, cộng thêm sai số của `approx_percentile` trong Trino. Sai số thực đo được ghi ở §5.
  - Nhãn trên dashboard: "p95 RTT (theo packet, xấp xỉ từ histogram)".
- **Không có metric "RTT trung bình theo result" mặc định.** Nếu sau này cần phân phối của `rtt_avg_ms` theo result, phải đặt tên rõ là "percentile của result-average RTT".
- **Mức độ chắc chắn của completeness:** RIPE không công bố số result kỳ vọng cho built-in measurement (`participant_count = null`). Mẫu số chỉ là ước tính: số probe của cohort cho af đó (đủ điều kiện tại thời điểm tạo probe set) × số lần đo mỗi giờ theo `interval`. Probe ngừng hoạt động sau đó vẫn bị tính vào mẫu số, nên completeness bị **đánh giá thấp**, không bao giờ cao hơn thực tế.
- **Jitter:** chưa công bố. `rtt_stddev_ms` là độ lệch chuẩn RTT, **không** phải jitter theo RFC 3550.

## 3. Ghép cặp IPv4/IPv6

- **Cặp** = một result v4 và một result v6 của **cùng probe**, **cùng target** (1001↔2001 cho K-root, 1004↔2004 cho F-root), là **láng giềng gần nhất của nhau** (mutual nearest neighbour), với |Δt| ≤ 120 giây.
- **Căn cứ cho ngưỡng 120 giây** (đo trên dữ liệu thật):
  - Với mỗi result v4, khoảng cách tới result v6 gần nhất có p50 64 s, p90 110 s, p99 143 s.
  - 98,5% nằm trong ≤ 120 s (nửa chu kỳ 240 s); 99,94% nằm trong ≤ 240 s.
- **Thiếu một phía:** result không có cặp được đếm ở `unpaired_v4_results` / `unpaired_v6_results`, không bị âm thầm bỏ qua.
- **Metric của cặp:** chỉ tính trên các result đã ghép cặp, cho từng phía.
  - `pairs`, `v4_/v6_packets_sent/lost`, `v4_/v6_rtt_sum_ms/rtt_count`.
  - `delta_rtt_mean_ms` = mean v6 − mean v4, tính trên cùng tập cặp.
- **Grain:** giờ × cohort × target × probe. Cộng được lên quốc gia, ASN (theo ASN v4) hoặc toàn cục.

## 4. Gold marts (namespace `nessie.gold`, partition `ictrl_dt`)

| Mart | Grain | Mục đích |
|---|---|---|
| `quality_hourly_target` | giờ × cohort × measurement | KPI, time series theo target/af |
| `quality_hourly_country` | + `country_code` (`ZZ` = chưa biết) | Bản đồ, heatmap, bảng suy giảm |
| `quality_hourly_asn` | + `asn` (theo af), `asn_country_count` | Bảng ASN suy giảm |
| `quality_hourly_probe` | + `probe_id`, `country_code`, `asn` | Drill-down; đếm distinct probe/quốc gia/ASN ở mọi phạm vi |
| `rtt_histogram_hourly` | giờ × cohort × measurement × country × asn × bucket | Percentile gộp được ở mọi cấp |
| `ipv4_ipv6_paired_hourly` | giờ × cohort × target × probe | So sánh IPv4/IPv6 theo cặp |
| `coverage_hourly` | giờ × cohort × measurement (đủ cả lưới giờ) | Độ phủ, độ đầy đủ, trạng thái xử lý, độ tươi, quarantine, loại trừ |

- Các mart chất lượng có cùng bộ cột cộng được: `results_*`, `packets_*`, `rtt_sum_ms`, `rtt_sumsq_ms2`, `rtt_count`, `rtt_min_ms`, `rtt_max_ms`.
- Mỗi mart chỉ lưu tử số, mẫu số và các thống kê cộng được. **Mọi tỷ lệ đều được tính lại ở Superset/Trino** sau khi group.

**Tránh lặp logic:**
- Mart quốc gia, ASN và target đều tính từ **cùng một** tập result đủ điều kiện.
- Có kiểm tra tự động bảo đảm: tổng mart quốc gia = tổng mart ASN = mart target = tổng mart probe, và tổng histogram = `rtt_count`.

## 5. Kiểm chứng

- Unit test (`tests/transformation/test_gold.py`):
  - Công thức loss/mean/tỷ lệ phản hồi, khi mẫu số = 0, và rollup giờ → ngày bằng tính trực tiếp.
  - Percentile từ histogram so với giá trị chính xác.
  - Ghép cặp: mutual nearest, ngưỡng 120 s, phía thiếu.
- Chạy thật: xem `docs/status.md`. Gồm đối chiếu KPI Gold với số tính thẳng từ Silver, và sai số percentile đo trên dữ liệu thật.
