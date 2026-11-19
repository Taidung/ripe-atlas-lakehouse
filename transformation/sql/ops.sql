-- Bảng vận hành: kết quả kiểm tra chất lượng và trạng thái công bố theo partition.

CREATE NAMESPACE IF NOT EXISTS nessie.ops;

-- Một dòng = một kiểm tra trong một lần chạy, trên một bảng và một phạm vi partition.
CREATE TABLE IF NOT EXISTS nessie.ops.dq_result (
  dq_run_id STRING NOT NULL,
  pipeline_run_id STRING NOT NULL,
  layer STRING NOT NULL,
  table_name STRING NOT NULL,
  partition_scope STRING NOT NULL COMMENT 'ví dụ ictrl_dt=2026-11-19 (danh sách, phân tách bằng dấu phẩy)',
  check_name STRING NOT NULL,
  engine STRING NOT NULL COMMENT 'gx-<version> | spark',
  severity STRING NOT NULL COMMENT 'critical | warn',
  success BOOLEAN NOT NULL,
  observed_value STRING,
  unexpected_count BIGINT,
  element_count BIGINT,
  details STRING,
  target_snapshot_id BIGINT COMMENT 'snapshot đã ghi (null nếu bị chặn)',
  checked_at TIMESTAMP NOT NULL
) USING iceberg
PARTITIONED BY (days(checked_at))
TBLPROPERTIES ('format-version'='2');

-- Một dòng = trạng thái hiện tại của một partition (bảng, ngày, measurement).
CREATE TABLE IF NOT EXISTS nessie.ops.partition_status (
  layer STRING NOT NULL,
  table_name STRING NOT NULL,
  ictrl_dt DATE NOT NULL,
  measurement_id INT NOT NULL,
  status STRING NOT NULL COMMENT 'published | published_with_warnings | blocked',
  row_count BIGINT,
  pipeline_run_id STRING NOT NULL,
  dq_run_id STRING NOT NULL,
  snapshot_id BIGINT,
  reason STRING,
  updated_at TIMESTAMP NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version'='2');
