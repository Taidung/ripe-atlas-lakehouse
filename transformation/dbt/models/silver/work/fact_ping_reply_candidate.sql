{{ config(materialized='table', schema='work', tags=['silver_facts_build']) }}
{#- Một dòng = một phần tử trong mảng `result` của một ping (thường 3 gói), cùng ngày với fact_ping_result_candidate. -#}
WITH exploded AS (
    SELECT f.result_id, f.measurement_id, f.af, f.probe_id, f.probe_sk, f.event_time, f.event_hour_utc, f.ictrl_dt,
           f.date_key, f.country_code, f.asn, f.public_status, f.silver_run_id, f.silver_loaded_at,
           u.rtt, u.x, u.error, u.dup, u.ttl, u.ord
    FROM {{ ref('fact_ping_result_candidate') }} f
    CROSS JOIN UNNEST(f._replies) WITH ORDINALITY AS u(rtt, x, error, dup, ttl, ord)
),
typed AS (
    SELECT e.*,
        CASE
            WHEN error IS NOT NULL THEN 'error'
            WHEN x IS NOT NULL THEN 'timeout'
            WHEN rtt IS NOT NULL AND coalesce(dup, 0) > 0 THEN 'duplicate'
            WHEN rtt IS NOT NULL THEN 'reply'
            ELSE 'unknown'
        END AS reply_type
    FROM exploded e
)
SELECT
    {{ sha256_hex("concat_ws('|', result_id, CAST(ord - 1 AS varchar))") }} AS reply_id,
    result_id,
    CAST(ord - 1 AS integer)                                AS reply_seq,
    reply_type,
    rtt                                                     AS rtt_ms,
    coalesce(reply_type = 'reply' AND rtt >= 0, false)      AS is_valid_rtt,
    CAST(ttl AS integer)                                    AS reply_ttl,
    error                                                   AS error_message,
    measurement_id, af, probe_id, probe_sk, event_time, event_hour_utc, ictrl_dt, date_key,
    country_code, asn, public_status, silver_run_id, silver_loaded_at
FROM typed
