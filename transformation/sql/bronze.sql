-- DDL Bronze. Idempotent: job bronze_ingest chạy file này trước mỗi lần ghi.
-- Grain, khóa và lý do partition: docs/data-contracts.md §Bronze.

CREATE NAMESPACE IF NOT EXISTS nessie.bronze;

-- Một dòng = một nội dung result nguồn duy nhất (measurement, probe, timestamp, sha của payload).
-- Cùng business key nhưng payload khác -> nhiều dòng; Silver chọn bản theo quy tắc xác định.
CREATE TABLE IF NOT EXISTS nessie.bronze.ping_result_raw (
  record_uid        STRING    NOT NULL COMMENT 'sha256(measurement_id|probe_id|event_ts|raw_sha256)',
  measurement_id    INT       NOT NULL,
  probe_id          BIGINT    NOT NULL,
  af                INT       NOT NULL,
  event_ts          BIGINT    NOT NULL COMMENT 'timestamp nguồn, Unix giây UTC',
  event_time        TIMESTAMP NOT NULL COMMENT 'event_ts dạng timestamp (UTC)',
  stored_ts         BIGINT             COMMENT 'stored_timestamp của RIPE Atlas, Unix giây',
  ictrl_dt          DATE      NOT NULL COMMENT 'ngày Asia/Ho_Chi_Minh của cửa sổ logic',
  window_start      TIMESTAMP NOT NULL,
  raw_json          STRING    NOT NULL COMMENT 'nguyên văn một dòng response format=txt',
  raw_sha256        STRING    NOT NULL COMMENT 'sha256 bytes UTF-8 của raw_json',
  change_type       STRING    NOT NULL COMMENT 'new | changed (so với các lần stage trước)',
  fw                INT,
  mver              STRING,
  source_object     STRING    NOT NULL,
  manifest_path     STRING    NOT NULL,
  ingest_run_id     STRING    NOT NULL,
  dlt_load_id       STRING    NOT NULL,
  fetched_at        TIMESTAMP NOT NULL,
  probe_set_version STRING    NOT NULL,
  bronze_run_id     STRING    NOT NULL,
  bronze_loaded_at  TIMESTAMP NOT NULL
) USING iceberg
PARTITIONED BY (ictrl_dt, measurement_id)
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd',
  'write.target-file-size-bytes' = '134217728',
  'write.distribution-mode' = 'hash',
  'write.merge.mode' = 'copy-on-write'
);

ALTER TABLE nessie.bronze.ping_result_raw WRITE ORDERED BY probe_id, event_ts;

-- Một dòng = một snapshot metadata của một probe trong một lần thu thập.
CREATE TABLE IF NOT EXISTS nessie.bronze.probe_metadata_raw (
  record_uid        STRING    NOT NULL COMMENT 'sha256(probe_id|ingest_run_id)',
  probe_id          BIGINT    NOT NULL,
  fetch_status      STRING    NOT NULL COMMENT 'ok | not_returned',
  is_public         BOOLEAN            COMMENT 'is_public tại observed_at; null nếu not_returned',
  raw_json          STRING             COMMENT 'JSON canonical (sort_keys) của phần tử trong trang probes',
  raw_sha256        STRING,
  raw_encoding      STRING    NOT NULL,
  observed_at       TIMESTAMP NOT NULL,
  ictrl_dt          DATE      NOT NULL,
  source_object     STRING    NOT NULL,
  manifest_path     STRING    NOT NULL,
  ingest_run_id     STRING    NOT NULL,
  dlt_load_id       STRING    NOT NULL,
  probe_set_version STRING    NOT NULL,
  bronze_run_id     STRING    NOT NULL,
  bronze_loaded_at  TIMESTAMP NOT NULL
) USING iceberg
PARTITIONED BY (ictrl_dt)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- Một dòng = một snapshot metadata của một measurement trong một lần thu thập.
CREATE TABLE IF NOT EXISTS nessie.bronze.measurement_metadata_raw (
  record_uid        STRING    NOT NULL COMMENT 'sha256(measurement_id|ingest_run_id)',
  measurement_id    INT       NOT NULL,
  fetch_status      STRING    NOT NULL,
  is_public         BOOLEAN,
  raw_json          STRING             COMMENT 'nguyên văn body GET /measurements/{id}/',
  raw_sha256        STRING,
  raw_encoding      STRING    NOT NULL,
  observed_at       TIMESTAMP NOT NULL,
  ictrl_dt          DATE      NOT NULL,
  source_object     STRING    NOT NULL,
  manifest_path     STRING    NOT NULL,
  ingest_run_id     STRING    NOT NULL,
  dlt_load_id       STRING    NOT NULL,
  probe_set_version STRING    NOT NULL,
  bronze_run_id     STRING    NOT NULL,
  bronze_loaded_at  TIMESTAMP NOT NULL
) USING iceberg
PARTITIONED BY (ictrl_dt)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- Một dòng = một bản ghi bị cách ly vì một lý do, từ một manifest.
CREATE TABLE IF NOT EXISTS nessie.bronze.quarantine (
  quarantine_uid    STRING    NOT NULL COMMENT 'sha256(entity|manifest_path|raw_sha256|reason|line)',
  entity            STRING    NOT NULL,
  reason            STRING    NOT NULL,
  measurement_id    INT,
  entity_id         BIGINT,
  event_ts          BIGINT,
  raw_json          STRING,
  raw_sha256        STRING,
  source_object     STRING,
  manifest_path     STRING    NOT NULL,
  ingest_run_id     STRING,
  ictrl_dt          DATE      NOT NULL,
  detected_at       TIMESTAMP NOT NULL,
  bronze_run_id     STRING    NOT NULL,
  reprocess_status  STRING    NOT NULL COMMENT 'pending | reprocessed | ignored'
) USING iceberg
PARTITIONED BY (entity, ictrl_dt)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- Một dòng = một lần Bronze xử lý một manifest staging (audit append-only + checkpoint).
-- Manifest "đã nạp" nếu có ít nhất một dòng bronze_status = 'loaded'.
CREATE TABLE IF NOT EXISTS nessie.bronze.ingest_manifest (
  manifest_path       STRING    NOT NULL,
  entity              STRING    NOT NULL,
  ingest_run_id       STRING    NOT NULL,
  run_type            STRING,
  load_id             STRING,
  measurement_id      INT,
  window_start        TIMESTAMP,
  window_end          TIMESTAMP,
  ictrl_dt            DATE      NOT NULL,
  probe_set_version   STRING,
  probes_requested    INT,
  request_count       INT,
  fetched             BIGINT,
  staged              BIGINT,
  unchanged           BIGINT,
  changed             BIGINT,
  object_count        INT,
  object_bytes        BIGINT,
  rows_read           BIGINT,
  rows_valid          BIGINT,
  rows_inserted       BIGINT    COMMENT 'dòng mới thực sự thêm vào bảng Bronze',
  rows_already_present BIGINT   COMMENT 'cùng record_uid đã có (xử lý lại / tải chồng lấn)',
  rows_quarantined    BIGINT,
  ingest_started_at   TIMESTAMP,
  ingest_finished_at  TIMESTAMP,
  bronze_status       STRING    NOT NULL COMMENT 'loaded',
  bronze_run_id       STRING    NOT NULL,
  bronze_loaded_at    TIMESTAMP NOT NULL,
  manifest_json       STRING    NOT NULL
) USING iceberg
PARTITIONED BY (entity, ictrl_dt)
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');
