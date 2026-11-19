-- DDL Silver. Idempotent; job silver_build chạy file này trước mỗi lần ghi.
-- Đặc tả cột: docs/silver-model.md.

CREATE NAMESPACE IF NOT EXISTS nessie.silver;

CREATE TABLE IF NOT EXISTS nessie.silver.fact_ping_result (
  result_id STRING NOT NULL, measurement_id INT NOT NULL, af INT NOT NULL, target STRING NOT NULL,
  probe_id BIGINT NOT NULL, event_ts BIGINT NOT NULL, event_time TIMESTAMP NOT NULL, event_hour_utc TIMESTAMP NOT NULL,
  ictrl_dt DATE NOT NULL, date_key INT NOT NULL, time_key INT NOT NULL,
  packets_sent INT, packets_received INT, packets_lost INT, duplicate_count_src INT,
  result_status STRING NOT NULL, has_response BOOLEAN NOT NULL,
  src_rtt_min DOUBLE, src_rtt_avg DOUBLE, src_rtt_max DOUBLE,
  rtt_min_ms DOUBLE, rtt_avg_ms DOUBLE, rtt_max_ms DOUBLE,
  reply_element_count INT NOT NULL, valid_rtt_count INT NOT NULL, rtt_sum_ms DOUBLE, rtt_sumsq_ms2 DOUBLE,
  duplicate_reply_count INT NOT NULL, timeout_count INT NOT NULL, error_count INT NOT NULL, unknown_element_count INT NOT NULL,
  counts_consistent BOOLEAN NOT NULL,
  ttl INT, packet_size INT, proto STRING, dst_addr STRING, fw INT, mver STRING, lts BIGINT,
  stored_ts BIGINT, ingest_delay_s BIGINT, clock_suspect BOOLEAN NOT NULL,
  source_version_count INT NOT NULL, is_source_corrected BOOLEAN NOT NULL,
  record_uid STRING NOT NULL, raw_sha256 STRING NOT NULL, ingest_run_id STRING NOT NULL, probe_set_version STRING NOT NULL,
  silver_run_id STRING NOT NULL, silver_loaded_at TIMESTAMP NOT NULL,
  probe_sk BIGINT NOT NULL, country_code STRING, asn BIGINT, public_status STRING NOT NULL, metadata_basis STRING NOT NULL,
  measurement_sk BIGINT NOT NULL
) USING iceberg
PARTITIONED BY (ictrl_dt, measurement_id)
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd',
               'write.target-file-size-bytes'='134217728', 'write.distribution-mode'='hash');

ALTER TABLE nessie.silver.fact_ping_result WRITE ORDERED BY probe_id, event_ts;

CREATE TABLE IF NOT EXISTS nessie.silver.fact_ping_reply (
  reply_id STRING NOT NULL, result_id STRING NOT NULL, reply_seq INT NOT NULL, reply_type STRING NOT NULL,
  rtt_ms DOUBLE, is_valid_rtt BOOLEAN NOT NULL, reply_ttl INT, error_message STRING,
  measurement_id INT NOT NULL, af INT NOT NULL, probe_id BIGINT NOT NULL, probe_sk BIGINT NOT NULL,
  event_time TIMESTAMP NOT NULL, event_hour_utc TIMESTAMP NOT NULL, ictrl_dt DATE NOT NULL, date_key INT NOT NULL,
  country_code STRING, asn BIGINT, public_status STRING NOT NULL,
  silver_run_id STRING NOT NULL, silver_loaded_at TIMESTAMP NOT NULL
) USING iceberg
PARTITIONED BY (ictrl_dt, measurement_id)
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd',
               'write.target-file-size-bytes'='134217728', 'write.distribution-mode'='hash');

ALTER TABLE nessie.silver.fact_ping_reply WRITE ORDERED BY result_id, reply_seq;
