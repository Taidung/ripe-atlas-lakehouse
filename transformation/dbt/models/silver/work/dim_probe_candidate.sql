{{ config(materialized='table', schema='work', tags=['silver_dims_build']) }}
{#- dim_probe (SCD2) tính lại toàn bộ từ snapshot Bronze; được kiểm tra rồi mới chép sang silver.dim_probe. -#}
{% set snapshots %}
    SELECT
        probe_id,
        observed_at,
        json_extract_scalar(raw_json, '$.country_code')                                AS country_code,
        try_cast(json_extract_scalar(raw_json, '$.asn_v4') AS bigint)                  AS asn_v4,
        try_cast(json_extract_scalar(raw_json, '$.asn_v6') AS bigint)                  AS asn_v6,
        json_extract_scalar(raw_json, '$.prefix_v4')                                   AS prefix_v4,
        json_extract_scalar(raw_json, '$.prefix_v6')                                   AS prefix_v6,
        try_cast(json_extract_scalar(raw_json, '$.is_public') AS boolean)              AS is_public,
        try_cast(json_extract_scalar(raw_json, '$.is_anchor') AS boolean)              AS is_anchor,
        round(try_cast(json_extract_scalar(raw_json, '$.geometry.coordinates[1]') AS double), 4) AS latitude,
        round(try_cast(json_extract_scalar(raw_json, '$.geometry.coordinates[0]') AS double), 4) AS longitude
    FROM {{ ref('probe_metadata_raw') }}
    WHERE fetch_status = 'ok'
{% endset %}
{{ scd2(snapshots, 'probe_id', 'bigint',
        ['country_code', 'asn_v4', 'asn_v6', 'prefix_v4', 'prefix_v6', 'is_public', 'is_anchor', 'latitude', 'longitude'],
        'probe', 'probe_sk') }}
