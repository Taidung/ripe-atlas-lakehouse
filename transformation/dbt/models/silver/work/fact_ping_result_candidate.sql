{{ config(materialized='table', schema='work', tags=['silver_facts_build']) }}
{#- Fact result của MỘT ngày (var day, giờ VN), tính lại toàn bộ từ Bronze (cả 4 measurement).
    Được kiểm tra (critical/warn + đối soát với Bronze) rồi mới ghi vào silver.fact_ping_result. -#}
WITH bronze AS (
    SELECT * FROM {{ ref('ping_result_raw') }} WHERE ictrl_dt = {{ day_literal() }}
),
-- Một dòng mỗi (measurement, probe, event_ts): bản fetched_at mới nhất, rồi stored_ts, rồi raw_sha256.
versions AS (
    SELECT b.*,
        count(*) OVER (PARTITION BY measurement_id, probe_id, event_ts) AS source_version_count,
        row_number() OVER (
            PARTITION BY measurement_id, probe_id, event_ts
            ORDER BY fetched_at DESC, stored_ts DESC NULLS LAST, raw_sha256 DESC
        ) AS rn
    FROM bronze b
),
parsed AS (
    SELECT v.*,
        try_cast(json_extract_scalar(raw_json, '$.sent') AS integer)  AS sent,
        try_cast(json_extract_scalar(raw_json, '$.rcvd') AS integer)  AS rcvd,
        try_cast(json_extract_scalar(raw_json, '$.dup') AS integer)   AS dup_src,
        try_cast(json_extract_scalar(raw_json, '$.min') AS double)    AS src_min,
        try_cast(json_extract_scalar(raw_json, '$.avg') AS double)    AS src_avg,
        try_cast(json_extract_scalar(raw_json, '$.max') AS double)    AS src_max,
        try_cast(json_extract_scalar(raw_json, '$.ttl') AS integer)   AS r_ttl,
        try_cast(json_extract_scalar(raw_json, '$.size') AS integer)  AS r_size,
        json_extract_scalar(raw_json, '$.proto')                      AS r_proto,
        json_extract_scalar(raw_json, '$.dst_addr')                   AS r_dst_addr,
        try_cast(json_extract_scalar(raw_json, '$.lts') AS bigint)    AS r_lts,
        {{ parse_replies('raw_json') }}                               AS replies,
        event_time AT TIME ZONE {{ report_tz() }}                     AS local_time
    FROM versions v
    WHERE rn = 1
),
measured AS (
    SELECT p.*,
        filter(replies, e -> e.rtt IS NOT NULL AND coalesce(e.dup, 0) = 0 AND e.rtt >= 0) AS valid_replies
    FROM parsed p
),
facts AS (
    SELECT
        {{ sha256_hex("concat_ws('|', 'ping_result', CAST(measurement_id AS varchar), CAST(probe_id AS varchar), CAST(event_ts AS varchar))") }} AS result_id,
        measurement_id,
        CAST({{ msm_af('measurement_id') }} AS integer)                AS af,
        {{ msm_target('measurement_id') }}                             AS target,
        probe_id,
        event_ts,
        event_time,
        date_trunc('hour', event_time)                                 AS event_hour_utc,
        CAST(local_time AS date)                                       AS ictrl_dt,
        CAST(year(local_time) * 10000 + month(local_time) * 100 + day(local_time) AS integer) AS date_key,
        CAST(hour(local_time) * 100 + minute(local_time) AS integer)   AS time_key,
        sent                                                           AS packets_sent,
        rcvd                                                           AS packets_received,
        CASE WHEN sent > 0 AND rcvd <= sent THEN sent - rcvd END       AS packets_lost,
        dup_src                                                        AS duplicate_count_src,
        CASE
            WHEN sent IS NULL OR rcvd IS NULL OR rcvd > sent OR rcvd < 0 THEN 'inconsistent'
            WHEN sent = 0 THEN 'send_error'
            WHEN rcvd = 0 THEN 'all_lost'
            WHEN rcvd < sent THEN 'partial_loss'
            ELSE 'all_received'
        END                                                            AS result_status,
        coalesce(rcvd > 0, false)                                      AS has_response,
        src_min                                                        AS src_rtt_min,
        src_avg                                                        AS src_rtt_avg,
        src_max                                                        AS src_rtt_max,
        CASE WHEN src_min >= 0 THEN src_min END                        AS rtt_min_ms,
        CASE WHEN src_avg >= 0 THEN src_avg END                        AS rtt_avg_ms,
        CASE WHEN src_max >= 0 THEN src_max END                        AS rtt_max_ms,
        CAST(cardinality(replies) AS integer)                          AS reply_element_count,
        CAST(cardinality(valid_replies) AS integer)                    AS valid_rtt_count,
        CASE WHEN cardinality(valid_replies) > 0
             THEN reduce(valid_replies, CAST(0 AS double), (s, e) -> s + e.rtt, s -> s) END AS rtt_sum_ms,
        CASE WHEN cardinality(valid_replies) > 0
             THEN reduce(valid_replies, CAST(0 AS double), (s, e) -> s + e.rtt * e.rtt, s -> s) END AS rtt_sumsq_ms2,
        CAST(cardinality(filter(replies, e -> e.rtt IS NOT NULL AND coalesce(e.dup, 0) > 0)) AS integer) AS duplicate_reply_count,
        CAST(cardinality(filter(replies, e -> e.x IS NOT NULL)) AS integer)      AS timeout_count,
        CAST(cardinality(filter(replies, e -> e.error IS NOT NULL)) AS integer)  AS error_count,
        CAST(cardinality(filter(replies, e -> e.rtt IS NULL AND e.x IS NULL AND e.error IS NULL)) AS integer) AS unknown_element_count,
        cardinality(valid_replies) = coalesce(rcvd, -1)                AS counts_consistent,
        r_ttl                                                          AS ttl,
        r_size                                                         AS packet_size,
        r_proto                                                        AS proto,
        r_dst_addr                                                     AS dst_addr,
        fw,
        mver,
        r_lts                                                          AS lts,
        stored_ts,
        stored_ts - event_ts                                           AS ingest_delay_s,
        -- timestamp do đồng hồ probe ghi: nghi ngờ khi probe không biết đã sync (lts = -1) hoặc RIPE lưu trước cả
        -- thời điểm đo (stored_ts < event_ts). Đo 2026-11-19: 712/44.340 result, 3 probe.
        coalesce(r_lts < 0 OR stored_ts < event_ts, false)             AS clock_suspect,
        CAST(source_version_count AS integer)                          AS source_version_count,
        source_version_count > 1                                       AS is_source_corrected,
        record_uid,
        raw_sha256,
        ingest_run_id,
        probe_set_version,
        '{{ var("run_id") }}'                                          AS silver_run_id,
        {{ run_ts() }}                                                 AS silver_loaded_at,
        replies                                                        AS _replies
    FROM measured
),
-- As-of join: gắn phiên bản dimension hiệu lực tại event_time. Không mất fact (unknown -> -1), không nhân bản
-- (các khoảng hiệu lực không chồng lấn; kiểm tra bằng test scd2_valid).
probe AS (
    SELECT probe_id AS p_id, probe_sk, join_from, valid_from, valid_to, country_code, asn_v4, asn_v6, is_public
    FROM {{ ref('dim_probe') }} WHERE NOT is_unknown_member
),
msm AS (
    SELECT measurement_id AS m_id, measurement_sk, join_from AS m_join_from, valid_to AS m_valid_to
    FROM {{ ref('dim_measurement') }} WHERE NOT is_unknown_member
)
SELECT
    f.*,
    coalesce(p.probe_sk, -1)                                           AS probe_sk,
    p.country_code,
    CASE WHEN f.af = 4 THEN p.asn_v4 ELSE p.asn_v6 END                 AS asn,
    CASE WHEN p.is_public THEN 'public' WHEN NOT p.is_public THEN 'not_public' ELSE 'unknown' END AS public_status,
    CASE WHEN p.probe_sk IS NULL THEN 'no_metadata'
         WHEN f.event_time < p.valid_from THEN 'assumed_from_first_snapshot'
         ELSE 'asof_snapshot' END                                      AS metadata_basis,
    coalesce(m.measurement_sk, -1)                                     AS measurement_sk
FROM facts f
LEFT JOIN probe p ON f.probe_id = p.p_id AND f.event_time >= p.join_from AND f.event_time < p.valid_to
LEFT JOIN msm m ON f.measurement_id = m.m_id AND f.event_time >= m.m_join_from AND f.event_time < m.m_valid_to
