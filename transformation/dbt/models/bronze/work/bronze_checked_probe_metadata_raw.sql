{{ config(materialized='table', schema='work', tags=['bronze_probe_metadata_raw']) }}
{{ bronze_metadata_checked('probe_metadata_raw', 'probe_id') }}
