{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['bronze_ping_result_raw'],
    properties={
        'partitioning': "ARRAY['ictrl_dt', 'measurement_id']",
        'sorted_by': "ARRAY['probe_id', 'event_ts']",
        'format_version': '2',
    },
) }}
{#- Một dòng = một nội dung result nguồn duy nhất (measurement, probe, timestamp, sha của payload).
    Chỉ chèn record_uid chưa có (tương đương MERGE ... WHEN NOT MATCHED INSERT): xử lý lại cùng manifest không sinh dòng mới. -#}
SELECT
    record_uid, measurement_id, probe_id, af, event_ts, event_time, stored_ts, ictrl_dt, window_start,
    raw_json, raw_sha256, change_type, fw, mver, source_object, manifest_path, ingest_run_id, dlt_load_id,
    fetched_at, probe_set_version, bronze_run_id, bronze_loaded_at
FROM {{ ref('bronze_checked_ping_result_raw') }} c
WHERE reason IS NULL AND dedup_rank = 1
{% if is_incremental() %}
  AND NOT EXISTS (
      SELECT 1 FROM {{ this }} t
      WHERE t.record_uid = c.record_uid AND t.ictrl_dt = c.ictrl_dt AND t.measurement_id = c.measurement_id
  )
{% endif %}
