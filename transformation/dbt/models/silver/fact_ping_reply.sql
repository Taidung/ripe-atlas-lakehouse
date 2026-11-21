{{ config(
    materialized='incremental',
    incremental_strategy='replace_day',
    unique_key=['reply_id'],
    tags=['silver_facts_publish'],
    properties={
        'partitioning': "ARRAY['ictrl_dt', 'measurement_id']",
        'sorted_by': "ARRAY['result_id', 'reply_seq']",
        'format_version': '2',
    },
) }}
{#- Một dòng = một gói (một phần tử của mảng result). Ghi lại cả ngày như fact_ping_result (một MERGE). -#}
SELECT * FROM {{ ref('fact_ping_reply_candidate') }}
