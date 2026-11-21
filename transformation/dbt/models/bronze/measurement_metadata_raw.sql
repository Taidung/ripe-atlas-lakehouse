{{ config(
    materialized='incremental',
    incremental_strategy='append',
    tags=['bronze_measurement_metadata_raw'],
    properties={'partitioning': "ARRAY['ictrl_dt']", 'format_version': '2'},
) }}
{#- Một dòng = một snapshot metadata của một measurement trong một lần thu thập. -#}
{{ bronze_metadata_insert('measurement_metadata_raw', 'measurement_id') }}
