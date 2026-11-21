# ADR-0003: Tầng biến đổi bằng dbt trên Trino; bỏ Spark, Great Expectations, OpenMetadata

- Trạng thái: **Đã chấp nhận (2026-11-21)**, trên nhánh `feat/dbt-trino`. Chủ đồ án chọn: bỏ OpenMetadata (dùng dbt docs),
  Bronze = runner Python chọn manifest + dbt trên Trino, làm trên nhánh mới để `main` (bản Spark) vẫn demo được.

## Bối cảnh

- Spark chỉ dùng ở chế độ local trong một container chạy thường trực (image ≈ 6,5 GB, driver 3 GB), cùng lúc Trino đã
  chạy để phục vụ truy vấn: hai engine cho một lượng dữ liệu nhỏ (dev: vài trăm nghìn result/ngày).
- Logic biến đổi và kiểm tra chất lượng nằm rải trong PySpark + GX + kiểm tra Spark tự viết; lineage OpenMetadata phải
  khai báo tay vì chưa kiểm chứng được OpenLineage với Nessie.
- Đo trước đó (`docs/test-report.md` §3.1): nút thắt RAM là Windows; bỏ Spark không đủ để bật full stack, nên lý do chính
  **không** phải tiết kiệm RAM mà là bớt một engine và gom logic về SQL có test, có lineage.

## Quyết định

- **Một engine:** Trino 484 vừa biến đổi (catalog `lakehouse` ghi Iceberg qua Nessie; catalog `staging` là Hive connector
  đọc thẳng `.jsonl.gz` của dlt) vừa phục vụ truy vấn (catalog `nessie` chỉ đọc). Phân quyền theo user bằng file
  (`serving/trino/rules.json`): chỉ `transform` được ghi.
- **dbt-core 1.12.7 + dbt-trino 1.12.0** cho toàn bộ SQL: Bronze (bảng kiểm tra tạm → bảng Bronze/quarantine), Silver (SCD2,
  as-of join, fact/reply), Gold (7 mart). Mỗi bảng công bố có bản nháp `*_candidate` trong schema `work`.
- **Runner Python mỏng** (`transformation/lakehouse`, image `ripe-atlas-transform` 762 MB): những việc SQL không làm được hoặc
  làm vụng: đọc manifest trên S3, chọn manifest chưa nạp, đăng ký partition staging, gọi dbt theo từng ngày, đọc kết quả
  test để quyết định `published` / `published_with_warnings` / `blocked`, ghi `ops.*`, đẩy metric Pushgateway.
- **Chất lượng = dbt test:** `severity: error` = critical (chặn công bố), `severity: warn` = cảnh báo; ngưỡng tỷ lệ kiểu
  `mostly` của GX thay bằng generic test `ratio_at_least`. Hai bước tách rõ: `dbt build` (nháp + test) → runner quyết định →
  `dbt run` (chỉ model công bố), không dựa vào cơ chế skip của dbt cho test nhiều cha.
- **Giữ nguyên:** giao thức công bố theo snapshot (`ops.partition_status`, Gold đọc `FOR VERSION AS OF`), `record_uid` và
  `result_id` (tính giống hệt nên dữ liệu Bronze cũ dùng tiếp), tên bảng/cột, metric Pushgateway, DAG Airflow (đổi operator).
- **Catalog nghiệp vụ = dbt docs** thay OpenMetadata.

## Hệ quả

- Kiểm chứng trên dữ liệu thật (`docs/test-report.md` §6): Bronze/Silver giống hệt bản Spark; Gold khớp tới sai số cộng
  số thực; crosscheck PASS; test chặn công bố trên nhánh Nessie PASS.
- Trino thành service core, phải chạy khi pipeline chạy; heap chạm giới hạn 3 GiB khi tính ngày lớn (không lỗi). Ngày ở
  chế độ analysis (strat, ≈ 1 GB/ngày) **chưa đo**.
- `probe_sk`/`measurement_sk` đổi giá trị (xxhash64 của Trino); mọi ngày Silver/Gold đã được tính lại.
- Mất: Data Docs của GX, giao diện OpenMetadata (owner, glossary, lineage tới dashboard Superset).
- Test logic dữ liệu cần Trino: unit test offline chỉ còn cho runner; logic SQL được bảo vệ bằng dbt test chạy mỗi lần
  build, `crosscheck` và `dq_block_on_branch.sh`. Chưa có dbt unit test với fixture.
- Ghi nhiều commit cho một lần công bố (xóa ngày rồi chèn): chấp nhận được vì tầng sau đọc snapshot đã công bố, không đọc
  bản mới nhất.
