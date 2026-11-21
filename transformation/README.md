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
| `silver` | `bronze.*` → `silver.*` (star schema, SCD2) | Bảng nháp → dbt test → mới ghi (mỗi bảng một `MERGE`); lỗi critical thì `blocked` (exit 2) |
| `gold` | Silver **đã công bố** (`FOR VERSION AS OF`) → `gold.*` | 7 mart, chỉ lưu tử số/mẫu số cộng được; kiểm tra nhất quán giữa các mart |
| `crosscheck` | Silver ↔ Gold | Kiểm chứng độc lập bằng SQL riêng, không dùng model Gold |
| `maintenance` | `ALTER TABLE ... EXECUTE optimize` | Gộp file nhỏ cho các ngày đã đóng |

## Thư mục

| Đường dẫn | Vai trò |
|---|---|
| `lakehouse/` | Runner: `bronze.py`, `silver.py`, `gold.py`, `crosscheck.py`, `maintenance.py`; `dbt_runner.py` (gọi dbt), `dq.py` (dbt test → `ops.dq_result`), `publication.py` (`ops.partition_status`), `trino_db.py`, `metrics.py` (Pushgateway), `common.py` |
| `sql/ops.sql` | DDL bảng runner ghi: bảng ngoài `staging.*`, mẫu `work.bronze_pending_object`, `bronze.ingest_manifest`, `ops.*` |
| `dbt/models/bronze` | Bảng kiểm tra tạm (`bronze_checked_*`, schema tạm) + bảng Bronze + quarantine |
| `dbt/models/silver` | `*_candidate` (nháp) + bảng công bố (`dim_*`, `fact_ping_result`, `fact_ping_reply`) |
| `dbt/models/gold` | `gold_cohort`, `*_candidate` + 7 mart công bố |
| `dbt/macros` | Hằng số, SCD2, đọc snapshot công bố, generic test (`tests.sql`), chiến lược ghi `replace_day` (`publish.sql`) |
| `dbt/tests` | Test đối soát Silver↔Bronze, nhất quán giữa các mart Gold |
| `dbt/seeds/probe_set_member.csv` | Thành viên probe set; sinh từ `config/probe_sets/*.json` bằng `dbt/scripts/build_probe_set_seed.py` |

**Ghi một ngày = một commit:** bảng công bố dùng chiến lược `replace_day` (`dbt/macros/publish.sql`): một lệnh `MERGE`
thay toàn bộ ngày (chèn/cập nhật theo khóa, xóa dòng cũ không còn), nên người đọc chỉ thấy bản cũ hoặc bản mới.
Giữa các bảng (fact và reply, 7 mart) vẫn là các commit riêng: tầng sau đọc snapshot đã công bố nên không bị ảnh hưởng,
nhưng Superset đọc bản mới nhất có thể thấy một ngày đang được ghi dở giữa các mart trong vài giây.

**Bảng tạm riêng mỗi lượt chạy:** bảng nháp, bảng kiểm tra, danh sách object đang chờ và cả bảng `<bảng>__dbt_tmp` mà
dbt tạo khi ghi incremental (macro `trino__make_temp_relation`) đều nằm trong schema `work_<run_id>`
(`lakehouse/trino_db.py::work_schema`), xóa khi xong; lượt chạy tay chạy cùng lúc với Airflow không đè bảng tạm của nhau.
MERGE còn chỉ lấy dòng của đúng ngày đang ghi. Schema sót lại do job bị giết được `maintenance` dọn sau 1 ngày.

**Hủy đúng query khi dừng task:** runner và dbt gắn client tag Trino `TRINO_CLIENT_TAG` (Airflow đặt theo dag/task/run/try);
`TransformOperator.on_kill` chỉ hủy query mang tag của task đó.

Biến `LAKEHOUSE_CATALOG` (mặc định `lakehouse`) chọn catalog ghi; `lakehouse_dqtest` trỏ vào nhánh Nessie `dqtest`,
chỉ dùng cho `tests/integration/dq_block_on_branch.sh`.

Test: `tests/transformation/` (runner, offline), dbt test (chạy cùng mỗi lần build), `crosscheck`, test tích hợp trên nhánh Nessie.
