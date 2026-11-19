# transformation/legacy — job thế hệ đầu (giữ để tham khảo)

Hai job Spark đầu tiên của dự án, trước khi có thiết kế Bronze/Silver/Gold hiện tại:

| File | Việc làm | Bảng đích |
|---|---|---|
| `staging_to_bronze.py` | Đọc staging, ghi Bronze kiểu cũ | `bronze.ping_results` |
| `bronze_to_silver.py` | Dựng star schema SCD1 | `silver.legacy_*` |
| `test_*.py` | Unit test của hai job trên (`unittest`) | — |

**Không dùng cho pipeline chính.** Pipeline hiện tại nằm ở `transformation/jobs/`
(`bronze_ingest.py` → `silver_build.py` → `gold_build.py`).
Các bảng legacy vẫn còn trong catalog; chỉ xoá khi chủ đồ án đồng ý (xem `docs/status.md`).
