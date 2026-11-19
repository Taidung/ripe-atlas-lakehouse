# tests/: kiểm thử theo thành phần

| Thư mục | Kiểm thử gì | Chạy ở đâu |
|---|---|---|
| `ingestion/` | Client API, cửa sổ thời gian, chuyển record, job ghi staging (filesystem cục bộ) | Host: `uv run pytest` (mặc định) |
| `transformation/` | Hàm Bronze/Silver/Gold trên dữ liệu RIPE Atlas thật trong `fixtures/` | Container Spark |
| `quality/` | Kiểm tra SCD2, đối soát, GX phát hiện lỗi critical | Container Spark |
| `contract/` | Hợp đồng với RIPE Atlas API thật (marker `live`) | Host, cần mạng |
| `integration/` | Smoke test Spark→Nessie→Trino; DQ chặn công bố trên nhánh Nessie | Bash, cần stack đang chạy |

Dùng chung:
- `spark_support.py`: fixture `spark`, `real_cases`, `unique_results` và các hàm dựng DataFrame Bronze/Silver.
- `fixtures/ping_results_real_sample.jsonl`: result thật đã gán tên trường hợp (normal, partial loss, send error, …).

```bash
# Spark (giống CI)
docker compose exec -T -w /opt/app spark-iceberg python -m pytest -q -p no:cacheprovider tests/transformation tests/quality
# API thật
uv run pytest -m live -o addopts="" tests/contract
```

Kết quả gần nhất: `docs/test-report.md`.
