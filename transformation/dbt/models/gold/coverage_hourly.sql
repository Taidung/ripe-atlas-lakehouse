{{ config(
    materialized='incremental',
    incremental_strategy='replace_day',
    unique_key=['probe_set_version', 'measurement_id', 'event_hour_utc'],
    tags=['gold_publish'],
    properties={'partitioning': "ARRAY['ictrl_dt']", 'format_version': '2'},
) }}
-- Bản công bố: thay cả ngày (var day) bằng bảng nháp đã qua kiểm tra, một MERGE (macros/publish.sql).
SELECT * FROM {{ ref('coverage_hourly_candidate') }}
