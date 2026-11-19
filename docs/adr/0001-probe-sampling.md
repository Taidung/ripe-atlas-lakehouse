# ADR-0001: Lấy mẫu probe phân tầng cho chế độ `analysis`

- Trạng thái: **Đã chấp nhận hướng đi (2026-11-19)**; tham số k và trần quốc gia **chờ chủ đồ án duyệt**.
- Bối cảnh: toàn bộ public probes ≈ 16 triệu result/ngày, toàn hệ ước tính ≈ 3 GB/ngày (`docs/api-survey.md`). Máy dev giới hạn RAM và đĩa.

## Quyết định

| Chế độ | Phạm vi probe |
|---|---|
| `dev` | 200 probe cố định của repo (`probe_set_version = dev-200-v1`) |
| `analysis` | Mẫu phân tầng theo `(country_code, ASN)`, ASN lấy theo af |

Thuật toán cho chế độ `analysis`:

1. **Tập nguồn:** public probe có result trong 4 measurement ở cửa sổ tham chiếu (24 giờ), đối chiếu với metadata `is_public=true`.
2. **Tầng:** `(country_code, asn_v4)` cho 1001/1004; `(country_code, asn_v6)` cho 2001/2004. ASN hoặc country null tạo tầng `UNKNOWN`.
3. **Chọn tối đa k probe mỗi tầng:**
   - Ưu tiên probe xuất hiện ở cả v4 và v6, để phục vụ paired cohort.
   - Thứ tự còn lại theo `sha256(seed || probe_id)`, nên deterministic.
4. **Trần theo quốc gia:** tối đa `C` probe mỗi quốc gia, cắt theo cùng thứ tự hash.
5. **Lưu tập mẫu:** `probe_set_version`, seed, k, C, ngày tham chiếu, danh sách probe và trọng số `w = |tầng| / |chọn|` được lưu vào staging (`_probe_sets/<version>.json`) và vào bảng `silver.dim_probe_set`.
6. **Không tự thay thế** probe ngừng hoạt động. Làm mới tập mẫu bằng cách tạo phiên bản mới.

## Số liệu đo (2026-11-19)

Nguồn: danh sách probe `is_public=true&status=1` (14.415 probe), giao với các probe có result trong mẫu 5 phút.

| | IPv4 | IPv6 |
|---|---|---|
| Tập nguồn | 13.149 probe | 7.415 probe |
| Số quốc gia | 186 | 149 |
| Số tầng (country, ASN) | 4.801 | 2.863 |
| Kích thước tầng (median / p90 / max) | 1 / 4 / 308 | 1 / 4 / 216 |
| Số tầng chỉ có 1 probe | 3.153 (66%) | 1.868 (65%) |
| ASN hoặc country null | 50 | **510** |

Số probe được chọn theo tham số:

| k \ trần C | không trần | 150 | 100 |
|---|---|---|---|
| 1 (v4 / v6) | 4.801 / 2.863 | 3.897 / 2.485 | 3.538 / 2.269 |
| 2 | 6.449 / 3.858 | 4.903 / 3.127 | **4.290 / 2.869** |
| 3 | 7.306 / 4.344 | 5.355 / 3.410 | 4.578 / 3.124 |

Probe xuất hiện ở cả v4 và v6: 7.271.
Khoảng 1.100 probe có result nhưng **không** nằm trong danh sách `status=1` lúc truy vấn (đã đổi trạng thái hoặc mất kết nối). Đây là trường hợp "probe chưa có metadata", cần bổ sung qua `GET /probes/?id__in=`.

**Đề xuất: k = 2, C = 100.** Lấy ≈ 4.290 probe v4 (33%) và ≈ 2.869 probe v6 (39%). Ước tính ≈ 1 GB/ngày toàn hệ; con số này sẽ đo lại.

## Hệ quả

- Vì 2/3 số tầng chỉ có 1 probe, phân tầng chủ yếu **cắt bớt** các nước và ASN đông probe (US, DE, FR, NL), giữ gần như nguyên phần còn lại.
- **Chỉ số toàn cầu** phải ghi rõ là *trên mẫu* (không trọng số) hay *ước tính tổng thể* (có trọng số `w`).
- **Chỉ số theo quốc gia và ASN** bị giới hạn bởi số probe trong mẫu. Dashboard phải hiện sample size và ngưỡng đủ mẫu.
- **510 probe IPv6 thiếu `asn_v6`** rơi vào tầng `UNKNOWN`. Cần kiểm tra lý do trước khi chốt, ví dụ probe không có IPv6 công khai trên giao diện.
