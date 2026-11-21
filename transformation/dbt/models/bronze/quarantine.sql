{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['bronze_ping_result_raw', 'bronze_probe_metadata_raw', 'bronze_measurement_metadata_raw'],
    properties={'partitioning': "ARRAY['entity', 'ictrl_dt']", 'format_version': '2'},
) }}
{# Một dòng = một bản ghi bị cách ly vì một lý do, từ một manifest. Đọc bảng kiểm tra của entity đang chạy (var entity). #}
-- depends_on: {{ ref('bronze_checked_ping_result_raw') }}
-- depends_on: {{ ref('bronze_checked_probe_metadata_raw') }}
-- depends_on: {{ ref('bronze_checked_measurement_metadata_raw') }}

WITH q AS (
    SELECT c.*, row_number() OVER (PARTITION BY quarantine_uid ORDER BY source_object) AS q_rank
    FROM {{ ref('bronze_checked_' ~ var('entity')) }} c
    WHERE reason IS NOT NULL
)
SELECT
    quarantine_uid,
    entity,
    reason,
    q_measurement_id     AS measurement_id,
    q_entity_id          AS entity_id,
    q_event_ts           AS event_ts,
    raw_json,
    raw_sha256,
    source_object,
    manifest_path,
    ingest_run_id,
    ictrl_dt,
    bronze_loaded_at     AS detected_at,
    bronze_run_id,
    'pending'            AS reprocess_status
FROM q
WHERE q_rank = 1
{% if is_incremental() %}
  AND NOT EXISTS (
      SELECT 1 FROM {{ this }} t
      WHERE t.quarantine_uid = q.quarantine_uid AND t.entity = q.entity AND t.ictrl_dt = q.ictrl_dt
  )
{% endif %}
