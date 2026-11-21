{{ config(materialized='table', schema='work', tags=['gold_build']) }}
{#- (probe_set_version, measurement_id, event_hour_utc) có manifest results đã nạp vào Bronze trong ngày (var day).
    Một probe thuộc nhiều probe set chỉ được tính cho cohort THỰC SỰ được ingest giờ đó; nếu không, cohort chưa từng
    ingest vẫn "có dữ liệu" nhờ các probe trùng và bị báo độ đầy đủ rất thấp. Lọc theo partition (entity, ictrl_dt). -#}
SELECT
    probe_set_version,
    measurement_id,
    window_start                       AS event_hour_utc,
    count(DISTINCT manifest_path)      AS manifests,
    max(unchanged)                     AS overlap_unchanged_skipped
FROM {{ source('bronze_log', 'ingest_manifest') }}
WHERE entity = 'ping_result_raw' AND ictrl_dt = {{ day_literal() }} AND bronze_status = 'loaded'
GROUP BY 1, 2, 3
