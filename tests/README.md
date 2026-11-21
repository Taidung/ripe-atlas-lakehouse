# tests/: kiểm thử theo thành phần

| Thư mục | Kiểm thử gì | Chạy ở đâu |
|---|---|---|
| `ingestion/` | Client API, cửa sổ thời gian, chuyển record, job ghi staging (filesystem cục bộ) | Host: `uv run pytest` (mặc định) |
| `transformation/` | Runner biến đổi: CLI mọi lệnh, glob manifest theo giờ VN, dòng đối tượng chờ, nhật ký manifest, ánh xạ dbt test → mức critical/warn | Host: `uv run pytest` (mặc định) |
| `contract/` | Hợp đồng với RIPE Atlas API thật (marker `live`) | Host, cần mạng |
| `integration/` | DQ chặn công bố trên nhánh Nessie `dqtest` (`dq_block_on_branch.sh`) | Bash, cần core + Trino và image transform |

Logic dữ liệu Bronze/Silver/Gold nằm trong dbt nên được kiểm tra trên Trino: dbt test chạy cùng mỗi lần build (chặn công
bố nếu lỗi critical), `python -m lakehouse crosscheck` so Gold với Silver bằng SQL viết riêng.

Dùng chung: `fixtures/ping_results_real_sample.jsonl`, result thật đã gán tên trường hợp (normal, partial loss, send error, …).

```bash
uv run pytest -q                                   # ingestion + transformation (offline)
uv run pytest -m live -o addopts="" tests/contract # API thật
bash tests/integration/dq_block_on_branch.sh 2026-11-20
```

Kết quả gần nhất: `docs/test-report.md`.
