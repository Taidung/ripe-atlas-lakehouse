{{ config(materialized='table', tags=['silver_dims_publish']) }}
{#- Bản công bố của dim_probe: chỉ chạy khi test của dim_probe_candidate không có lỗi critical (runner quyết định). -#}
SELECT * FROM {{ ref('dim_probe_candidate') }}
