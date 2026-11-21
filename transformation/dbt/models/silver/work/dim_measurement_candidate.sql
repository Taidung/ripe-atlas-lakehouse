{{ config(materialized='table', schema='work', tags=['silver_dims_build']) }}
{#- dim_measurement (SCD2) tính lại toàn bộ từ snapshot Bronze; được kiểm tra rồi mới chép sang silver.dim_measurement. -#}
{% set snapshots %}
    SELECT
        measurement_id,
        observed_at,
        try_cast(json_extract_scalar(raw_json, '$.af') AS integer)       AS af,
        json_extract_scalar(raw_json, '$.target')                        AS target,
        json_extract_scalar(raw_json, '$.target_ip')                     AS target_ip,
        try_cast(json_extract_scalar(raw_json, '$.interval') AS integer) AS interval_s,
        try_cast(json_extract_scalar(raw_json, '$.packets') AS integer)  AS packets,
        try_cast(json_extract_scalar(raw_json, '$.size') AS integer)     AS packet_size,
        try_cast(json_extract_scalar(raw_json, '$.is_public') AS boolean) AS is_public,
        json_extract_scalar(raw_json, '$.status.name')                   AS status_name,
        json_extract_scalar(raw_json, '$.type')                          AS msm_type
    FROM {{ ref('measurement_metadata_raw') }}
    WHERE fetch_status = 'ok'
{% endset %}
{{ scd2(snapshots, 'measurement_id', 'integer',
        ['af', 'target', 'target_ip', 'interval_s', 'packets', 'packet_size', 'is_public', 'status_name', 'msm_type'],
        'measurement', 'measurement_sk') }}
