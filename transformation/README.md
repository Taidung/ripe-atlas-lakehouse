# transformation/: staging → Bronze → Silver → Gold bằng dbt trên Trino

Không có engine riêng. Trino (catalog ghi `lakehouse`, user `transform`) xử lý dữ liệu. Runner Python `lakehouse`
chỉ điều phối: chọn manifest, gọi dbt với tham số của lần chạy, đọc kết quả dbt test, rồi quyết định công bố.
Image `ripe-atlas-transform` (build từ `Dockerfile` ở thư mục này, context là gốc repo) chứa runner và dự án dbt.
Code nằm sẵn trong image, nên **sau khi sửa thư mục này phải build lại**: `docker compose --profile jobs build transform`.

```bash
docker compose --profile jobs run --rm transform --help
docker compose --profile jobs run --rm transform silver --dates 2026-11-20
```

## Lệnh (`python -m lakehouse <lệnh>`), theo thứ tự chạy

| Lệnh | Đầu vào → đầu ra | Ghi chú |
|---|---|---|
| `bronze` | staging (manifest `complete`) → `bronze.*` | Trino đọc thẳng `.jsonl.gz` qua catalog `staging`; chỉ chèn `record_uid` chưa có; bản ghi hỏng vào `bronze.quarantine` |
| `silver` | `bronze.*` → `silver.*` (star schema, SCD2) | Bảng nháp → dbt test → mới ghi; lỗi critical thì `blocked` (exit 2) |
| `gold` | Silver **đã công bố** (`FOR VERSION AS OF`) → `gold.*` | 7 mart, chỉ lưu tử số/mẫu số cộng được; kiểm tra nhất quán giữa các mart |
| `crosscheck` | Silver ↔ Gold | Kiểm chứng độc lập bằng SQL riêng, không dùng model Gold |
| `maintenance` | `ALTER TABLE ... EXECUTE optimize` | Gộp file nhỏ cho các ngày đã đóng |

## Thư mục

| Đường dẫn | Vai trò |
|---|---|
| `lakehouse/` | Runner: `bronze.py`, `silver.py`, `gold.py`, `crosscheck.py`, `maintenance.py`; `dbt_runner.py` (gọi dbt), `dq.py` (dbt test → `ops.dq_result`), `publication.py` (`ops.partition_status`), `trino_db.py`, `metrics.py` (Pushgateway), `common.py` |
| `sql/ops.sql` | DDL bảng runner ghi: bảng ngoài `staging.*`, `work.bronze_pending_object`, `bronze.ingest_manifest`, `ops.*` |
| `dbt/models/bronze` | Bảng kiểm tra tạm (`work.bronze_checked_*`) + bảng Bronze + quarantine |
| `dbt/models/silver` | `*_candidate` (nháp, schema `work`) + bảng công bố (`dim_*`, `fact_ping_result`, `fact_ping_reply`) |
| `dbt/models/gold` | `gold_cohort`, `*_candidate` + 7 mart công bố |
| `dbt/macros` | Hằng số, SCD2, đọc snapshot công bố, generic test (`tests.sql`) |
| `dbt/tests` | Test đối soát Silver↔Bronze, nhất quán giữa các mart Gold |
| `dbt/seeds/probe_set_member.csv` | Thành viên probe set; sinh từ `config/probe_sets/*.json` bằng `dbt/scripts/build_probe_set_seed.py` |

Biến `LAKEHOUSE_CATALOG` (mặc định `lakehouse`) chọn catalog ghi; `lakehouse_dqtest` trỏ vào nhánh Nessie `dqtest`,
chỉ dùng cho `tests/integration/dq_block_on_branch.sh`.

Test: `tests/transformation/` (runner, offline), dbt test (chạy cùng mỗi lần build), `crosscheck`, test tích hợp trên nhánh Nessie.
