# Dashboard "Global Network Quality": đặc tả

- Superset 6.1.0, slug `global-network-quality`.
- Dashboard được dựng bằng code: `serving/superset/provision.py` (idempotent), metric lấy từ `config/metrics.json`, dataset ảo từ `serving/superset/datasets/*.sql`.
- Bản xuất để import: `serving/superset/export/global_network_quality.zip`.

## Dataset

| Dataset | Nguồn | Dùng cho |
|---|---|---|
| `quality_hourly_probe` | `gold.quality_hourly_probe` | KPI, time series, bảng quốc gia, drill-down. Có đủ cột cho mọi filter; đếm distinct probe/quốc gia/ASN đúng ở mọi phạm vi |
| `rtt_histogram_hourly` | `gold.rtt_histogram_hourly` | p50/p95/p99 bằng `approx_percentile(bucket_rep_ms, reply_count, p)` |
| `quality_hourly_country` | `gold.quality_hourly_country` | Bản đồ, heatmap |
| `quality_hourly_asn` | `gold.quality_hourly_asn` | Bảng ASN |
| `ipv4_ipv6_paired_hourly` | `gold.ipv4_ipv6_paired_hourly` | So sánh IPv4/IPv6 |
| `coverage_hourly` | `gold.coverage_hourly` | Độ phủ, tình trạng xử lý |
| `v_pipeline_status` (ảo) | `datasets/v_pipeline_status.sql` | Hàng trạng thái; cache 60 giây |
| `v_degraded_groups` (ảo) | `datasets/v_degraded_groups.sql` | Quốc gia/ASN suy giảm |

- Cột thời gian chính: `event_hour_local`, là giờ Asia/Ho_Chi_Minh kiểu `timestamp` không múi giờ.
- Biểu thức metric: `config/metrics.json`. Định nghĩa: `docs/metric-dictionary.md`.

## Chart

Mọi chart đều nhận filter cohort.

| # | Chart | Mục đích | Dataset / metric | Trục, đơn vị | Filter không áp dụng | Khi không có dữ liệu |
|---|---|---|---|---|---|---|
| 1 | Trạng thái dữ liệu theo measurement | Dữ liệu mới tới đâu, đã công bố chưa, có quá SLO 120 phút không | `v_pipeline_status` (raw) | Phút, trạng thái | thời gian, quốc gia, ASN, probe | Bảng rỗng = chưa build Gold |
| 2–4 | Số probe / số quốc gia / độ đầy đủ ước tính | Cỡ mẫu và độ phủ | `probes_observed`, `countries_observed` (probe); `completeness_est_pct` (coverage, chỉ giờ đã xử lý) | Số đếm, % | (4) quốc gia, ASN, probe: coverage là cấp cohort | Hiện "No data" |
| 5–9 | Packet loss, RTT TB, p95, tỷ lệ phản hồi, số result | KPI | `packet_loss_pct`, `rtt_mean_ms`, `response_rate_observed_pct`, `results` (probe); `rtt_p95_ms` (histogram) | %, ms | | Tỷ lệ có mẫu số 0 → trống (`NULL`), không phải 0 |
| 10 | Bản đồ packet loss theo quốc gia | Phân bố địa lý | country: màu = `packet_loss_pct`, bong bóng = `results` | % | ASN, probe | Quốc gia không có result: không tô màu |
| 11 | Chất lượng theo quốc gia | Tooltip đầy đủ cho bản đồ: số probe, cỡ mẫu, loss, RTT, tỷ lệ phản hồi, số result mất toàn bộ | probe, group by `country_code` | | | `ZZ` = chưa biết quốc gia |
| 12–15 | Time series loss / RTT TB / p95 / tỷ lệ phản hồi theo target × af | Diễn biến theo thời gian | probe / histogram, `x = event_hour_local`, group by `target, af` | %, ms | | Giờ không có dữ liệu: điểm bị ngắt, không nội suy |
| 16 | Heatmap loss quốc gia × giờ | Phát hiện bất thường theo vùng và thời gian | country | % | ASN, probe | **Ô trống = không có result** (khác với 100% = mất toàn bộ packet) |
| 17 | Quốc gia/ASN suy giảm | Nhóm xấu đi so với baseline | `v_degraded_groups`; ẩn `insufficient_sample` | Điểm %, ms | thời gian (có cửa sổ riêng), quốc gia, ASN, probe | Không có dòng = không có nhóm đủ mẫu |
| 18 | Chất lượng theo ASN | Xếp hạng ASN kèm cỡ mẫu | asn, `isp_name`, `isp_type` | | quốc gia, probe | `-1` = chưa biết ASN |
| 19 | Nhà mạng Việt Nam | So sánh nhà mạng VN kèm số probe | probe, `country_code = VN`, group by `isp_name, isp_type` | %, ms | nhà mạng, ASN, probe | Gộp các ASN của cùng nhà mạng; ISP khác Hosting/DC |
| 20 | RTT trung bình theo nhà mạng Việt Nam | RTT mỗi nhà mạng VN | bar, x = `isp_name`, màu = `isp_type` | ms | nhà mạng, ASN, probe | Xem kèm số probe ở chart 19 |
| 21–22 | IPv4 vs IPv6 trên cặp | So sánh công bằng: cùng probe, cùng target, \|Δt\| ≤ 120 s | paired: `pairs`, loss, RTT, delta, số result không có cặp | ms, % | af, measurement, ASN (bảng dùng `asn_v4`/`asn_v6`) | Không có cặp → `pairs = 0`, tỷ lệ `NULL` |
| 23–24 | Độ đầy đủ theo measurement; tình trạng xử lý theo giờ | Phân biệt `measured`, `processed_no_results` và `not_processed`; loại trừ; quarantine | coverage | % | quốc gia, ASN, probe | Giờ chưa xử lý vẫn hiện, với trạng thái `not_processed` |
| 25 | Drill-down theo probe | Tìm probe kéo chỉ số xấu đi | probe, group by `probe_id, country_code, asn, isp_name, measurement_id` | | | |
| — | Ghi chú "Đọc dashboard đúng cách" | Giới hạn diễn giải | Markdown | | | |

**Phạm vi filter** được `provision.py` tính tự động: một filter chỉ áp vào chart có dataset chứa cột tương ứng. Vì vậy không có chart nào "âm thầm" bỏ qua filter mà người xem tưởng đang được áp dụng.

## Native filter

| Filter | Kiểu | Mặc định | Ghi chú |
|---|---|---|---|
| Probe set (cohort) | Chọn 1, bắt buộc | `PROBE_SET_VERSION` trong `.env` (mặc định `dev-200-v2`), tức probe set pipeline đang thu; đổi biến này thì chạy lại `provision.py` | Bắt buộc chọn đúng một, vì probe thuộc hai set sẽ có mặt ở cả hai cohort |
| Khoảng thời gian (giờ VN) | Time range | No filter | Theo `event_hour_local` |
| Time grain | Time grain | (của chart: 1 giờ) | Tỷ lệ luôn tính lại từ tử và mẫu số nên đổi grain vẫn đúng |
| Target, Address family, Measurement | Chọn nhiều | | Vì measurement xác định (target, af), lọc measurement và target/af mâu thuẫn nhau sẽ cho kết quả rỗng, không cho số sai |
| Quốc gia, Nhà mạng, ASN, Probe | Chọn nhiều | | Nhà mạng = `isp_name` (tên theo ASN, seed `asn_info`) |

## Kiểm thử đã chạy (2026-11-19, dữ liệu 260.442 result ngày 19/11 giờ VN)

- `provision.py`: 22/22 chart dùng API mới chạy truy vấn thành công trên Trino. Bản đồ (plugin legacy) được kiểm tra bằng trình duyệt.
- Trình duyệt: dashboard render đủ 23 chart và 1 markdown, không chart nào lỗi. Console chỉ có lỗi đăng ký service worker của Superset (không ảnh hưởng chart).
- `verify_filters.py`: 10/10 kiểm tra.
  - Lọc measurement / af / target+af: mart probe = mart target.
  - Lọc quốc gia VN / US / ZZ: mart probe = mart country.
  - Grain ngày = tổng toàn phạm vi. Trung bình tỷ lệ theo giờ (cách sai) lệch: IPv6 5,61% so với 8,82% đúng.
  - Khoảng giờ VN 09:00–10:00 khớp coverage cùng giờ: 214.628 result.
  - p95 theo VN + af=4 tính trên đúng `rtt_count` reply.
- **Cache:** kết quả chart cache 5 phút (dataset trạng thái: 60 giây). Sau khi Gold công bố, dashboard có thể chậm tối đa 5 phút; bấm "Force refresh" để bỏ qua cache.

## Giới hạn đã biết

- Tooltip của bản đồ (`world_map`) chỉ hiện metric chính và metric phụ. Số probe và độ phủ của quốc gia nằm ở bảng "Chất lượng theo quốc gia" đặt cạnh bản đồ.
- Filter thời gian tương đối (ví dụ "Last day") tính theo đồng hồ server. Cột `event_hour_local` là giờ VN, nên khi dùng mốc tương đối sẽ lệch 7 giờ. Nên dùng khoảng thời gian tuyệt đối.
- Số probe trong bảng ASN và bảng suy giảm là max theo giờ hoặc số probe chung, không phải distinct trên toàn khoảng thời gian. Muốn đếm distinct, dùng bảng quốc gia hoặc drill-down.
