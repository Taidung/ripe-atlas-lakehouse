{{ config(materialized='table', tags=['silver_dims_publish']) }}
SELECT * FROM {{ ref('dim_measurement_candidate') }}
