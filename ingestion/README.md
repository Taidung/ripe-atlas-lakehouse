# ingestion/: RIPE Atlas API → staging (SeaweedFS)

Lấy ping result và metadata (probe, measurement) từ RIPE Atlas API, rồi ghi vào bucket `staging` ở dạng `jsonl.gz`, kèm một manifest cho mỗi lượt chạy.
Package Python chạy bằng `python -m ingestion <lệnh>`, trên host (uv) hoặc trong container `ingestion` (profile `jobs`).

## Lệnh

| Lệnh | Việc làm |
|---|---|
| `metadata` | Chụp snapshot metadata của measurement và probe |
| `results` / `backfill --start --end` | Lấy ping result cho khoảng `[start, end)`, chia theo từng giờ |
| `incremental` | Lấy giờ vừa kết thúc, cộng thêm `OVERLAP_HOURS` giờ chồng lấn |
| `cleanup [--apply]` | Liệt kê (mặc định) hoặc xoá thật object staging mồ côi (không manifest nào trỏ tới) |
| `build-probe-set` | Sinh tập probe phân tầng (ADR-0001), ghi ra `config/probe_sets/` |

```bash
docker compose --profile jobs run --rm ingestion incremental
```

## Module (đọc theo thứ tự luồng dữ liệu)

| File | Vai trò |
|---|---|
| `cli.py` | Phân tích tham số dòng lệnh, gọi job tương ứng (`__main__.py` chỉ chuyển tiếp sang đây) |
| `settings.py` | Hằng số nghiệp vụ (4 measurement) và tham số vận hành đọc từ biến môi trường |
| `time_windows.py` | Cửa sổ `[start, end)` theo giờ, quy tắc biên của API results |
| `probe_sets.py` | Đọc tập probe có phiên bản (`probe_set_version`) |
| `probe_sampling.py` | Thuật toán lấy mẫu phân tầng k=2, cap=100 |
| `api_client.py` | HTTP client: retry có giới hạn, backoff kèm jitter, tôn trọng `Retry-After`; `iter_probe_pages` đi hết các trang `probes/` |
| `result_records.py` | Body API → record staging; phân loại delta (mới / trùng / đổi nội dung) |
| `staging_store.py` | Ghi object qua dlt; ghi manifest (commit marker) và hash index |
| `jobs.py` | Ghép tất cả lại: một job cho mỗi `(measurement_id, cửa sổ 1 giờ)`; job snapshot metadata probe/measurement |
| `staging_cleanup.py` | Dọn object mồ côi do lượt chạy bị ngắt giữa chừng |
| `metrics.py`, `logs.py` | Metric đẩy lên Pushgateway; log JSON từng dòng ra stderr |

## Hợp đồng đầu ra

- Dữ liệu: `s3://staging/ripe_atlas/<entity>/window_date=…/window_hour=…/msm_id=…/*.jsonl.gz`
- Manifest: `s3://staging/ripe_atlas/_manifests/<entity>/…/<run_id>.json`. Tầng Bronze **chỉ** đọc object thuộc manifest có `status=complete`.
- Đặc tả đầy đủ: `docs/data-contracts.md`

Test: `tests/ingestion/` (offline), `tests/contract/` (gọi API thật).
