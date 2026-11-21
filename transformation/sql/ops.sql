-- DDL (cú pháp Trino) của các bảng do runner ghi; idempotent, runner chạy file này trước mỗi lần chạy.
-- Bảng do dbt tạo (Bronze/Silver/Gold) KHÔNG nằm ở đây: xem transformation/dbt/models.

-- ---------------------------------------------------------------- staging (Hive, bảng ngoài, chỉ đọc)
CREATE SCHEMA IF NOT EXISTS staging.ripe_atlas;

CREATE TABLE IF NOT EXISTS staging.ripe_atlas.ping_result_raw (
  measurement_id bigint, probe_id bigint, event_ts bigint, parse_status varchar, raw_json varchar, raw_sha256 varchar,
  line_no bigint, request_index bigint, change_type varchar, run_id varchar, fetched_at varchar,
  "_dlt_load_id" varchar, "_dlt_id" varchar,
  window_date varchar, window_hour varchar, msm_id varchar
) WITH (
  format = 'JSON',
  external_location = 's3://staging/ripe_atlas/ping_result_raw',
  partitioned_by = ARRAY['window_date', 'window_hour', 'msm_id']
);

CREATE TABLE IF NOT EXISTS staging.ripe_atlas.probe_metadata_raw (
  entity_id bigint, fetch_status varchar, raw_json varchar, raw_sha256 varchar, raw_encoding varchar, is_public boolean,
  observed_at varchar, request_index bigint, run_id varchar, "_dlt_load_id" varchar, "_dlt_id" varchar,
  ictrl_dt varchar
) WITH (
  format = 'JSON',
  external_location = 's3://staging/ripe_atlas/probe_metadata_raw',
  partitioned_by = ARRAY['ictrl_dt']
);

CREATE TABLE IF NOT EXISTS staging.ripe_atlas.measurement_metadata_raw (
  entity_id bigint, fetch_status varchar, raw_json varchar, raw_sha256 varchar, raw_encoding varchar, is_public boolean,
  observed_at varchar, request_index bigint, run_id varchar, "_dlt_load_id" varchar, "_dlt_id" varchar,
  ictrl_dt varchar
) WITH (
  format = 'JSON',
  external_location = 's3://staging/ripe_atlas/measurement_metadata_raw',
  partitioned_by = ARRAY['ictrl_dt']
);

-- ---------------------------------------------------------------- work (bảng tạm của runner)
CREATE SCHEMA IF NOT EXISTS work;

-- Một dòng = một object staging thuộc manifest chưa nạp của lần chạy Bronze hiện tại (thay toàn bộ mỗi lần chạy).
CREATE TABLE IF NOT EXISTS work.bronze_pending_object (
  entity varchar, manifest_path varchar, source_object varchar,
  window_date varchar, window_hour varchar, msm_id varchar, part_ictrl_dt varchar,
  expected_measurement_id bigint, expected_af bigint, window_start_ts bigint, window_end_ts bigint,
  ictrl_dt date, probe_set_version varchar, expected_load_id varchar, manifest_staged_rows bigint
);

-- ---------------------------------------------------------------- bronze: nhật ký nạp manifest
CREATE SCHEMA IF NOT EXISTS bronze;

-- Một dòng = một lần Bronze xử lý một manifest staging (audit append-only + checkpoint).
-- Manifest "đã nạp" nếu có ít nhất một dòng bronze_status = 'loaded'.
CREATE TABLE IF NOT EXISTS bronze.ingest_manifest (
  manifest_path varchar NOT NULL, entity varchar NOT NULL, ingest_run_id varchar NOT NULL, run_type varchar,
  load_id varchar, measurement_id integer, window_start timestamp(6) with time zone, window_end timestamp(6) with time zone,
  ictrl_dt date NOT NULL, probe_set_version varchar, probes_requested integer, request_count integer,
  fetched bigint, staged bigint, unchanged bigint, changed bigint, object_count integer, object_bytes bigint,
  rows_read bigint, rows_valid bigint, rows_inserted bigint, rows_already_present bigint, rows_quarantined bigint,
  ingest_started_at timestamp(6) with time zone, ingest_finished_at timestamp(6) with time zone,
  bronze_status varchar NOT NULL, bronze_run_id varchar NOT NULL, bronze_loaded_at timestamp(6) with time zone NOT NULL,
  manifest_json varchar NOT NULL
) WITH (partitioning = ARRAY['entity', 'ictrl_dt'], format_version = 2);

-- ---------------------------------------------------------------- ops: chất lượng và công bố
CREATE SCHEMA IF NOT EXISTS ops;

-- Một dòng = một kiểm tra trong một lần chạy, trên một bảng và một phạm vi partition.
CREATE TABLE IF NOT EXISTS ops.dq_result (
  dq_run_id varchar NOT NULL, pipeline_run_id varchar NOT NULL, layer varchar NOT NULL, table_name varchar NOT NULL,
  partition_scope varchar NOT NULL, check_name varchar NOT NULL, engine varchar NOT NULL, severity varchar NOT NULL,
  success boolean NOT NULL, observed_value varchar, unexpected_count bigint, element_count bigint, details varchar,
  target_snapshot_id bigint, checked_at timestamp(6) with time zone NOT NULL
) WITH (partitioning = ARRAY['day(checked_at)'], format_version = 2);

-- Một dòng = trạng thái hiện tại của một partition (bảng, ngày, measurement).
CREATE TABLE IF NOT EXISTS ops.partition_status (
  layer varchar NOT NULL, table_name varchar NOT NULL, ictrl_dt date NOT NULL, measurement_id integer NOT NULL,
  status varchar NOT NULL, row_count bigint, pipeline_run_id varchar NOT NULL, dq_run_id varchar NOT NULL,
  snapshot_id bigint, reason varchar, updated_at timestamp(6) with time zone NOT NULL
) WITH (format_version = 2);
