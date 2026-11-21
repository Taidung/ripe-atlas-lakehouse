{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['gold_publish'],
    pre_hook="{{ delete_day_if_exists() }}",
    properties={'partitioning': "ARRAY['ictrl_dt']", 'format_version': '2'},
) }}
-- Bản công bố: ghi lại cả ngày (var day) từ bảng nháp đã qua kiểm tra.
SELECT * FROM {{ ref('rtt_histogram_hourly_candidate') }}
