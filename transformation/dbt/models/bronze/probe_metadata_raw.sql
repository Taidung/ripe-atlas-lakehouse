{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['bronze_probe_metadata_raw'],
    properties={'partitioning': "ARRAY['ictrl_dt']", 'format_version': '2'},
) }}
{#- Một dòng = một snapshot metadata của một probe trong một lần thu thập (record_uid = sha(probe_id|ingest_run_id)). -#}
{{ bronze_metadata_insert('probe_metadata_raw', 'probe_id') }}
