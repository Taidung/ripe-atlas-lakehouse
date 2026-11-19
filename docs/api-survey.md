# Khảo sát RIPE Atlas API (giai đoạn 0)

Ngày khảo sát: 2026-11-18. Tất cả request ẩn danh (không API key), từ máy dev Windows 11.
Ký hiệu: **[Doc]** = tài liệu chính thức nói vậy; **[Đo]** = kiểm chứng bằng request thật; **[Suy luận]** = chưa có bằng chứng trực tiếp.

## 1. Metadata 4 measurement — `GET /api/v2/measurements/{id}/` [Đo]

| measurement_id | af | target | target_ip | interval (s) | packets | start_time (UTC) | status |
|---|---|---|---|---|---|---|---|
| 1001 | 4 | k.root-servers.net | 193.0.14.129 | 240 | 3 | 2010-10-29 | Ongoing |
| 1004 | 4 | f.root-servers.net | 192.5.5.241 | 240 | 3 | 2010-10-29 | Ongoing |
| 2001 | 6 | k.root-servers.net | 2001:7fd::1 | 240 | 3 | 2010-11-01 | Ongoing |
| 2004 | 6 | f.root-servers.net | 2001:500:2f::f | 240 | 3 | 2011-09-22 | Ongoing |

Khớp với trang built-in measurements [Doc]. IPv6 chỉ chạy trên probe có khả năng IPv6 [Doc]. Vì vậy mẫu số kỳ vọng của IPv6 phải lọc theo khả năng của probe.
`probes_requested`/`participant_count` = null, nên **không có mẫu số kỳ vọng chính thức**. Completeness chỉ là **ước tính**, dựa trên baseline probe-hoạt-động × (3600/240).

## 2. Results — `GET /api/v2/measurements/{id}/results/`

| Câu hỏi | Kết quả | Nguồn |
|---|---|---|
| Biên `start` | **Inclusive**: `start=t&stop=t` trả về đúng result có `timestamp=t` | [Đo] |
| Biên `stop` | **Inclusive** (cùng thí nghiệm trên) | [Đo]; doc không nêu rõ |
| Pagination | **Không có**. `page_size=10` bị bỏ qua: 61 giây trả 3.707 bản ghi | [Đo] + [Doc] "None of the generic query parameters (fields, page, etc.) work on the results endpoint" |
| `format` | `json` (một mảng) và `txt` (mỗi dòng một JSON, phù hợp stream) | [Doc] + [Đo] |
| `probe_ids` | Danh sách id phân tách bằng dấu phẩy, có thể dùng khoảng | [Doc] + [Đo] |
| `public_only` | `true`/`1` đều được chấp nhận. Trên 1001, 5 phút: `true`=17.907, `false`=17.908 (lệch 1 do dữ liệu đến thêm giữa 2 request) | [Đo] — với request ẩn danh gần như không có khác biệt |
| Rate limit | Không có header `X-RateLimit-*`/`Retry-After` trong các response quan sát. Doc chỉ nêu HTTP 429 và khuyên back-off, không có con số | [Đo] + [Doc] |
| Late data | Mỗi result có `stored_timestamp`. Trong mẫu, độ trễ `stored_timestamp - timestamp` khoảng 30–130 giây | [Đo] (chưa đo phân phối đầy đủ) |

**Quy ước triển khai:** cửa sổ logic `[S, E)` gọi API với `start=S&stop=E-1`, rồi lọc lại `S <= timestamp < E` phía client. Hai biên inclusive nên không có result nào rơi vào 2 cửa sổ liền kề. Code hiện có gọi `start=S-1&stop=E` rồi lọc, cũng đúng nhưng lấy thừa.

### Khối lượng (mẫu 5 phút 13:00–13:05 UTC 2026-11-18, toàn bộ public probes) [Đo]

| msm | results/5 phút | probe khác nhau | bytes JSON/result | all-loss (`rcvd=0`) |
|---|---|---|---|---|
| 1001 | 17.907 | 14.257 | 501 | 478 (2,7%) |
| 1004 | 17.890 | 14.263 | 497 | 421 (2,4%) |
| 2001 | 9.906 | 7.903 | 529 | 950 (9,6%) |
| 2004 | 9.941 | 7.906 | 532 | 937 (9,4%) |

Ngoại suy (giả định tải đều trong giờ; chưa kiểm chứng trên 24h):

- ≈ 668k results/giờ ≈ **16 triệu/ngày**; reply rows ≈ 3×, tức ≈ 48 triệu/ngày.
- JSON thô ≈ 344 MB/giờ ≈ 8,3 GB/ngày. gzip đo được ≈ 7×, nên staging ≈ **1,2 GB/ngày**.
- Ước tính toàn hệ (staging + Bronze + Silver + Gold) ≈ 3 GB/ngày. Con số này **chưa đo**, sẽ đo lại ở giai đoạn 2.
- Một request 5 phút cho 1001 trả 8 MB JSON. Request 1 giờ toàn bộ probe ≈ 96 MB, nên phải stream (`format=txt`) hoặc chia nhỏ cửa sổ hay nhóm probe.

Business key `(msm_id, prb_id, timestamp, af)`: **0 trùng** trong cả 4 mẫu [Đo]. Đây mới là bằng chứng trên 5 phút; sẽ kiểm tra lại trên 24h trước khi chốt.

## 3. Ngữ nghĩa trường ping (đối chiếu doc format 5000 và dữ liệu thật)

| Trường | Ngữ nghĩa | Bằng chứng |
|---|---|---|
| `sent` | Số packet gửi. **Có thể = 0** khi probe không gửi được, ví dụ `"error": "sendto failed: Operation not permitted"` (9–28 bản ghi/5 phút) | [Đo] |
| `rcvd` | Số reply hợp lệ, **không gồm duplicate**. Mẫu `duplicate_reply`: `rcvd=0` dù có 1 phần tử `{"dup":1,"rtt":...}` | [Đo] |
| `dup` | Số reply trùng [Doc]; quan sát có giá trị bất thường như 143, 177 | [Đo] |
| `min/avg/max` | RTT ms; **= -1 khi không có reply hợp lệ** (100% trùng với `rcvd=0` trong mẫu) | [Đo]; doc không nêu sentinel |
| `result[]` | Mỗi phần tử là một trong: `{"rtt"}`, `{"rtt","ttl"}`, `{"rtt","src_addr"}`, `{"dup":1,"rtt"[,"ttl"]}`, `{"x":"*"}`, `{"error":...}` | [Đo] + [Doc] (doc ghi `src_Addr`, dữ liệu thật là `src_addr`) |
| `x: "*"` | Timeout của packet | [Doc] |
| Số phần tử | Có thể **lớn hơn `sent`** khi có duplicate (ví dụ 4 phần tử với `sent=3`) | [Đo] |
| `ttl` (cấp result) | Tùy chọn, vắng khi không có reply | [Đo] |
| `mver` | Vắng ở firmware cũ (`fw=4790`) | [Đo] |
| `lts` | Giây kể từ lần đồng bộ đồng hồ; `-1` = không biết | [Doc] |

**Hệ quả cho mô hình:**

- Packet loss tính trên `sent > 0`; `sent = 0` là **lỗi kỹ thuật phía probe**, không phải mất gói.
- "Timeout" chỉ gán cho phần tử `x="*"`.
- Không dùng `avg` khi `rcvd = 0`.
- Duplicate reply bị loại khỏi thống kê RTT nhưng được đếm riêng.

## 4. Probes — `GET /api/v2/probes/`

- Phân trang theo số trang: `count/next/previous/results`, `page_size` tối đa 500, theo `next` cho tới khi null [Doc + Đo].
- `is_public=true` → `count=51.813` (gồm cả probe đã bỏ). `is_public=false` → 10.312 [Đo].
- Bộ lọc không hợp lệ bị **bỏ qua im lặng** [Doc], nên test phải kiểm tra rằng bộ lọc có tác dụng.
- Có `asn_v4`, `asn_v6`, `country_code`, `geometry` (lon, lat), `status`, `status_since`, `first_connected`, `last_connected`, `tags` (gồm `system-ipv6-works`, `system-ipv4-works`, …) [Đo].
- **Không có lịch sử `is_public`**: API chỉ trả trạng thái hiện tại. Trạng thái public trong quá khứ chỉ biết được từ các snapshot ta tự lưu (`observed_at`). Với backfill trước ngày snapshot đầu tiên, public status là **"giả định theo snapshot hiện tại"**.

## 5. Fixtures thật

Các bản ghi thật đại diện từng trường hợp được lưu ở `tests/fixtures/ping_results_real_sample.jsonl`:
normal, partial loss, all-loss timeout, send error `sent=0`, duplicate reply, duplicate-only, reply có `src_addr`, firmware cũ không có `mver`, IPv6 normal, IPv6 all-loss.
