{{ config(materialized='table', schema='work', tags=['gold_build']) }}
{#- Mọi result Silver ĐÃ CÔNG BỐ của ngày thuộc cohort (trước khi lọc public/clock), kèm giờ địa phương và mã unknown
    cho country/asn, tên nhà mạng theo ASN (seed asn_info; ASN chưa có tên hiện 'AS<n>'). is_eligible = probe public và
    đồng hồ không đáng ngờ. -#}
WITH facts AS (
    SELECT * FROM {{ published('fact_ping_result', 'fact_snapshot') }} WHERE {{ readable_filter() }}
),
members AS (
    SELECT m.* FROM {{ ref('probe_set_member') }} m
    WHERE m.probe_set_version IN (SELECT probe_set_version FROM {{ ref('gold_ingested_hours') }})
)
SELECT
    f.result_id, f.measurement_id, f.af, f.target, f.probe_id, f.probe_sk, f.event_ts, f.event_time, f.event_hour_utc,
    {{ local_hour('f.event_hour_utc') }}                         AS event_hour_local,
    f.ictrl_dt, f.packets_sent, f.packets_received, f.packets_lost, f.result_status, f.valid_rtt_count, f.rtt_sum_ms, f.rtt_sumsq_ms2,
    f.rtt_min_ms, f.rtt_max_ms, f.public_status, f.clock_suspect, f.is_source_corrected, f.silver_loaded_at,
    m.probe_set_version,
    coalesce(f.country_code, 'ZZ')                               AS country_code,
    coalesce(f.asn, -1)                                          AS asn,
    CASE WHEN f.asn IS NULL THEN 'Chưa biết'
         ELSE coalesce(i.isp_name, 'AS' || CAST(f.asn AS varchar)) END AS isp_name,
    i.isp_type,
    f.public_status = 'public' AND NOT f.clock_suspect          AS is_eligible
FROM facts f
JOIN members m ON m.af = f.af AND m.probe_id = f.probe_id
LEFT JOIN {{ ref('asn_info') }} i ON i.asn = f.asn
WHERE EXISTS (
    SELECT 1 FROM {{ ref('gold_ingested_hours') }} h
    WHERE h.probe_set_version = m.probe_set_version AND h.measurement_id = f.measurement_id AND h.event_hour_utc = f.event_hour_utc
)
