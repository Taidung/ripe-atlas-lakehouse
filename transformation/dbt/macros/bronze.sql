{# Macro của tầng Bronze. Chính sách lỗi (docs/data-contracts.md §Quarantine):
   - lỗi mức bản ghi -> quarantine, batch vẫn chạy;
   - lỗi mức batch (số dòng / load_id khác manifest, tỷ lệ quarantine vượt ngưỡng) -> test lỗi, dbt không ghi bảng Bronze. #}

{% macro pending_partition_filter(entity, partition_cols, pending_cols) -%}
    {#- Điều kiện partition (giá trị literal) của các object đang chờ: Trino chỉ mở đúng thư mục staging cần đọc,
        không liệt kê toàn bộ lịch sử. `pending_cols`: cột tương ứng trong bronze_pending_object.
        Lúc parse (execute=false) trả về điều kiện giả. -#}
    {%- if not execute -%}{{ return('1 = 1') }}{%- endif -%}
    {%- set q -%}
        SELECT DISTINCT {{ pending_cols | join(', ') }}
        FROM {{ source('runner', 'bronze_pending_object') }} WHERE entity = '{{ entity }}'
    {%- endset -%}
    {%- set rows = run_query(q).rows -%}
    {%- if rows | length == 0 -%}{{ return('1 = 0') }}{%- endif -%}
    {%- set tuples = [] -%}
    {%- for r in rows -%}
        {%- set vals = [] -%}
        {%- for v in r -%}
            {#- Đường dẫn object thiếu một khóa partition: NULL sẽ thành chuỗi 'None', lọc ra 0 dòng mà không báo lỗi. -#}
            {%- if v is none -%}
                {{ exceptions.raise_compiler_error("Bronze " ~ entity ~ ": object đang chờ thiếu giá trị partition " ~ pending_cols ~ " = " ~ r) }}
            {%- endif -%}
            {%- do vals.append("'" ~ v ~ "'") -%}
        {%- endfor -%}
        {%- do tuples.append('(' ~ vals | join(', ') ~ ')') -%}
    {%- endfor -%}
    ({{ partition_cols | join(', ') }}) IN ({{ tuples | join(', ') }})
{%- endmacro %}

{% macro pending_objects(entity) -%}
    SELECT * FROM {{ source('runner', 'bronze_pending_object') }} WHERE entity = '{{ entity }}'
{%- endmacro %}

{% macro bronze_metadata_insert(entity, id_column) -%}
SELECT
    record_uid, {{ id_column }}, fetch_status, is_public, raw_json, raw_sha256, raw_encoding, observed_at, ictrl_dt,
    source_object, manifest_path, ingest_run_id, dlt_load_id, probe_set_version, bronze_run_id, bronze_loaded_at
FROM {{ ref('bronze_checked_' ~ entity) }} c
WHERE reason IS NULL AND dedup_rank = 1
{% if is_incremental() %}
  AND NOT EXISTS (SELECT 1 FROM {{ this }} t WHERE t.record_uid = c.record_uid AND t.ictrl_dt = c.ictrl_dt)
{% endif %}
{%- endmacro %}

{% macro bronze_metadata_checked(entity, id_column) -%}
{#- Snapshot metadata probe / measurement đã stage -> lý do quarantine (NULL = hợp lệ) + cột bảng Bronze. -#}
WITH pending AS (
    {{ pending_objects(entity) }}
),
staged AS (
    SELECT s.*, s."$path" AS source_object
    FROM {{ source('staging_files', entity) }} s
    WHERE {{ pending_partition_filter(entity, ['ictrl_dt'], ['part_ictrl_dt']) }}
),
joined AS (
    SELECT st.*, p.manifest_path, p.ictrl_dt AS manifest_ictrl_dt, p.probe_set_version, p.expected_load_id
    FROM staged st
    JOIN pending p ON st.source_object = p.source_object
),
checked AS (
    SELECT j.*,
        CASE
            WHEN entity_id IS NULL THEN 'missing_key'
            WHEN fetch_status = 'not_returned' THEN NULL
            WHEN raw_json IS NULL OR {{ sha256_hex('raw_json') }} <> raw_sha256 THEN 'hash_mismatch'
            WHEN try_cast(try(json_parse(raw_json)) AS map(varchar, json)) IS NULL THEN 'malformed_json'
            WHEN try_cast(json_extract_scalar(raw_json, '$.id') AS bigint) <> entity_id THEN 'id_mismatch'
        END AS reason
    FROM joined j
)
SELECT
    {{ uid(['entity_id', 'run_id']) }}                                     AS record_uid,
    CAST(entity_id AS {{ 'integer' if id_column == 'measurement_id' else 'bigint' }}) AS {{ id_column }},
    fetch_status,
    is_public,
    raw_json,
    raw_sha256,
    raw_encoding,
    {{ to_ts6('from_iso8601_timestamp(observed_at)') }}                    AS observed_at,
    manifest_ictrl_dt                                                      AS ictrl_dt,
    source_object,
    manifest_path,
    run_id                                                                 AS ingest_run_id,
    "_dlt_load_id"                                                         AS dlt_load_id,
    expected_load_id,
    coalesce(probe_set_version, 'unknown')                                 AS probe_set_version,
    '{{ var("run_id") }}'                                                  AS bronze_run_id,
    {{ run_ts() }}                                                         AS bronze_loaded_at,
    reason,
    row_number() OVER (PARTITION BY entity_id, run_id, reason IS NULL  -- = record_uid
                       ORDER BY source_object, "_dlt_id")                  AS dedup_rank,
    -- cột chung cho bronze.quarantine
    {{ uid(["'" ~ entity ~ "'", 'manifest_path', 'raw_sha256', 'reason', '"_dlt_id"']) }} AS quarantine_uid,
    '{{ entity }}'                                                         AS entity,
    CAST(NULL AS integer)                                                  AS q_measurement_id,
    entity_id                                                              AS q_entity_id,
    CAST(NULL AS bigint)                                                   AS q_event_ts
FROM checked
{%- endmacro %}
