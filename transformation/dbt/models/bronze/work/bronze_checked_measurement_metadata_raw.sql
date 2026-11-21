{{ config(materialized='table', schema='work', tags=['bronze_measurement_metadata_raw']) }}
{{ bronze_metadata_checked('measurement_metadata_raw', 'measurement_id') }}
