{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['silver_facts_publish'],
    pre_hook="{{ delete_day_if_exists() }}",
    properties={
        'partitioning': "ARRAY['ictrl_dt', 'measurement_id']",
        'sorted_by': "ARRAY['result_id', 'reply_seq']",
        'format_version': '2',
    },
) }}
{#- Một dòng = một gói (một phần tử của mảng result). Ghi lại cả ngày như fact_ping_result. -#}
SELECT * FROM {{ ref('fact_ping_reply_candidate') }}
