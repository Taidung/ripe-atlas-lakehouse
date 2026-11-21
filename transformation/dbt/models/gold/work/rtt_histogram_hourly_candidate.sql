{{ config(materialized='table', schema='work', tags=['gold_build']) }}
{#- Phân bố RTT hợp lệ theo bucket log (mỗi bucket rộng hơn bucket trước 2%), để tính p50/p95 sau khi gộp. -#}
WITH replies AS (
    SELECT result_id, rtt_ms FROM {{ published('fact_ping_reply', 'reply_snapshot') }}
    WHERE {{ readable_filter() }} AND is_valid_rtt
),
h AS (
    SELECT c.ictrl_dt, c.event_hour_utc, c.event_hour_local, c.probe_set_version, c.measurement_id, c.af, c.target,
           c.country_code, c.asn, {{ bucket_id('r.rtt_ms') }} AS bucket_id, count(*) AS reply_count
    FROM replies r
    JOIN {{ ref('gold_cohort') }} c ON c.result_id = r.result_id AND c.is_eligible
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
SELECT h.*,
    {{ bucket_base_ms() }} * power({{ bucket_ratio() }}, bucket_id + 0.5)                       AS bucket_rep_ms,
    CASE WHEN bucket_id = 0 THEN 0e0 ELSE {{ bucket_base_ms() }} * power({{ bucket_ratio() }}, bucket_id) END AS bucket_lower_ms,
    {{ bucket_base_ms() }} * power({{ bucket_ratio() }}, bucket_id + 1)                         AS bucket_upper_ms,
    {{ gold_meta() }}
FROM h
