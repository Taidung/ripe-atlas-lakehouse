{{ config(materialized='table', schema='work', tags=['bronze_ping_result_raw']) }}
{#- Mọi dòng staged của các manifest đang chờ, kèm lý do quarantine (NULL = hợp lệ) và cột bảng Bronze.
    Bảng tạm: test kiểm tra lô chạy trên bảng này TRƯỚC khi ghi bronze.ping_result_raw / bronze.quarantine. -#}
WITH pending AS (
    {{ pending_objects('ping_result_raw') }}
),
staged AS (
    SELECT s.*, s."$path" AS source_object
    FROM {{ source('staging_files', 'ping_result_raw') }} s
    WHERE {{ pending_partition_filter('ping_result_raw', ['window_date', 'window_hour', 'msm_id'],
                                      ['window_date', 'window_hour', 'msm_id']) }}
),
joined AS (
    SELECT st.*, p.manifest_path, p.expected_measurement_id, p.expected_af, p.window_start_ts, p.window_end_ts,
           p.ictrl_dt, p.probe_set_version, p.expected_load_id
    FROM staged st
    JOIN pending p ON st.source_object = p.source_object
),
parsed AS (
    SELECT j.*,
        try_cast(try(json_parse(raw_json)) AS map(varchar, json))         AS doc,
        try_cast(json_extract_scalar(raw_json, '$.msm_id') AS bigint)     AS p_msm_id,
        try_cast(json_extract_scalar(raw_json, '$.prb_id') AS bigint)     AS p_prb_id,
        try_cast(json_extract_scalar(raw_json, '$.timestamp') AS bigint)  AS p_timestamp,
        try_cast(json_extract_scalar(raw_json, '$.af') AS bigint)         AS p_af,
        json_extract_scalar(raw_json, '$.type')                           AS p_type,
        try_cast(json_extract_scalar(raw_json, '$.stored_timestamp') AS bigint) AS p_stored_ts,
        try_cast(json_extract_scalar(raw_json, '$.fw') AS bigint)         AS p_fw,
        json_extract_scalar(raw_json, '$.mver')                           AS p_mver
    FROM joined j
),
checked AS (
    SELECT p.*,
        CASE
            WHEN parse_status = 'invalid_utf8' THEN 'invalid_utf8'
            WHEN {{ sha256_hex('raw_json') }} <> raw_sha256 THEN 'hash_mismatch'
            WHEN doc IS NULL THEN 'malformed_json'
            WHEN p_msm_id IS NULL OR p_prb_id IS NULL OR p_timestamp IS NULL OR p_af IS NULL THEN 'missing_key'
            WHEN p_msm_id <> expected_measurement_id THEN 'wrong_measurement'
            WHEN p_af <> expected_af THEN 'af_mismatch'
            WHEN coalesce(p_type, '') <> 'ping' THEN 'wrong_type'
            WHEN p_timestamp < window_start_ts OR p_timestamp >= window_end_ts THEN 'out_of_window'
        END AS reason
    FROM parsed p
)
SELECT
    {{ uid(['p_msm_id', 'p_prb_id', 'p_timestamp', 'raw_sha256']) }}       AS record_uid,
    CAST(p_msm_id AS integer)                                              AS measurement_id,
    p_prb_id                                                               AS probe_id,
    CAST(p_af AS integer)                                                  AS af,
    p_timestamp                                                            AS event_ts,
    {{ to_ts6("from_unixtime(p_timestamp, 'UTC')") }}                             AS event_time,
    p_stored_ts                                                            AS stored_ts,
    ictrl_dt,
    {{ to_ts6("from_unixtime(window_start_ts, 'UTC')") }}                         AS window_start,
    raw_json,
    raw_sha256,
    change_type,
    CAST(p_fw AS integer)                                                  AS fw,
    p_mver                                                                 AS mver,
    source_object,
    manifest_path,
    run_id                                                                 AS ingest_run_id,
    "_dlt_load_id"                                                         AS dlt_load_id,
    expected_load_id,
    {{ to_ts6('from_iso8601_timestamp(fetched_at)') }}                     AS fetched_at,
    coalesce(probe_set_version, 'unknown')                                 AS probe_set_version,
    '{{ var("run_id") }}'                                                  AS bronze_run_id,
    {{ run_ts() }}                                                         AS bronze_loaded_at,
    reason,
    -- Cùng nội dung được stage nhiều lần (hai run chồng nhau): chỉ bản stage sớm nhất được ghi (dedup_rank = 1).
    row_number() OVER (
        PARTITION BY {{ uid(['p_msm_id', 'p_prb_id', 'p_timestamp', 'raw_sha256']) }}, reason IS NULL
        ORDER BY from_iso8601_timestamp(fetched_at), run_id, source_object, line_no
    )                                                                      AS dedup_rank,
    -- cột chung cho bronze.quarantine
    {{ uid(["'ping_result_raw'", 'manifest_path', 'raw_sha256', 'reason', 'request_index', 'line_no']) }} AS quarantine_uid,
    'ping_result_raw'                                                      AS entity,
    CAST(expected_measurement_id AS integer)                               AS q_measurement_id,
    probe_id                                                               AS q_entity_id,
    event_ts                                                               AS q_event_ts
FROM checked
