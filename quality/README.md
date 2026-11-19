# quality/: kiểm tra chất lượng dữ liệu (package `dq`)

Job Silver và Gold gọi package này trước khi công bố dữ liệu.
- Lỗi mức **critical**: chặn công bố, partition chuyển sang `blocked`.
- Lỗi mức **warn**: vẫn công bố, với trạng thái `published_with_warnings`.

Mọi kết quả được ghi vào `nessie.ops.dq_result`.

| Module | Vai trò |
|---|---|
| `dq/results.py` | Kiểu kết quả dùng chung: `CRITICAL`/`WARN`, `make_check`, `has_critical_failure`, `failed_names` |
| `dq/gx_suites.py` | Bộ expectation Great Expectations theo bảng: `silver_fact_result`, `silver_fact_reply`, `silver_dimension`, `gold_quality_target` |
| `dq/gx_runner.py` | `run_suite(df, …)`: chạy một suite qua GX Checkpoint, lưu kết quả và Data Docs vào `artifacts/gx/` |
| `dq/checks.py` | Kiểm tra viết bằng Spark: SCD2 (chồng lấn, liên tục), đối soát Bronze↔Silver, nhất quán giữa các mart Gold |
| `dq/recorder.py` | `DqRecorder`: ghi kết quả vào `ops.dq_result` |

Cách dùng trong job (rút gọn từ `transformation/jobs/silver_build.py`):

```python
from dq import gx_suites
from dq.gx_runner import run_suite
from dq.results import has_critical_failure

checks = run_suite(facts, "silver_fact_ping_result", gx_suites.silver_fact_result(), run_label)
if has_critical_failure(checks):
    ...  # không ghi, đánh dấu partition blocked
```

Test: `tests/quality/` (chạy trong container Spark), `tests/integration/dq_block_on_branch.sh` (chặn công bố thật trên nhánh Nessie).
