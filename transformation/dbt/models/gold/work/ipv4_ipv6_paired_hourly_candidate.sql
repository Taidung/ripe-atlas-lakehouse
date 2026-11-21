{{ config(materialized='table', schema='work', tags=['gold_build']) }}
{#- Ghép cặp IPv4/IPv6 của CÙNG probe tới CÙNG target, đo cách nhau <= 120 giây; mỗi result chỉ thuộc tối đa một cặp
    (láng giềng gần nhất hai chiều). Cặp được xếp vào giờ của result IPv4. Result không ghép được đếm riêng. -#}
WITH eligible AS (
    SELECT * FROM {{ ref('gold_cohort') }} WHERE is_eligible
),
v4 AS (SELECT * FROM eligible WHERE af = 4),
v6 AS (SELECT * FROM eligible WHERE af = 6),
cand AS (
    SELECT
        v4.probe_set_version, v4.target, v4.probe_id,
        v4.result_id AS v4_result_id, v6.result_id AS v6_result_id,
        v4.event_ts AS v4_event_ts, v6.event_ts AS v6_event_ts,
        abs(v6.event_ts - v4.event_ts) AS dt,
        v4.ictrl_dt, v4.event_hour_utc, v4.event_hour_local,
        v4.country_code AS v4_country_code, v4.asn AS v4_asn, v6.asn AS v6_asn,
        v4.packets_sent AS v4_packets_sent, v4.packets_lost AS v4_packets_lost,
        v4.valid_rtt_count AS v4_valid_rtt_count, v4.rtt_sum_ms AS v4_rtt_sum_ms,
        v6.packets_sent AS v6_packets_sent, v6.packets_lost AS v6_packets_lost,
        v6.valid_rtt_count AS v6_valid_rtt_count, v6.rtt_sum_ms AS v6_rtt_sum_ms
    FROM v4
    JOIN v6 ON v4.probe_set_version = v6.probe_set_version AND v4.target = v6.target AND v4.probe_id = v6.probe_id
    WHERE abs(v6.event_ts - v4.event_ts) <= 120
),
ranked AS (
    SELECT c.*,
        row_number() OVER (PARTITION BY v4_result_id ORDER BY dt, v6_event_ts) AS r4,
        row_number() OVER (PARTITION BY v6_result_id ORDER BY dt, v4_event_ts) AS r6
    FROM cand c
),
pairs AS (
    SELECT * FROM ranked WHERE r4 = 1 AND r6 = 1
),
agg_pairs AS (
    SELECT probe_set_version, target, probe_id, ictrl_dt, event_hour_utc, event_hour_local,
        count(*)                                                                        AS pairs,
        avg(dt)                                                                         AS pair_dt_mean_s,
        arbitrary(v4_country_code)                                                      AS country_code,
        arbitrary(v4_asn)                                                               AS asn_v4,
        arbitrary(v6_asn)                                                               AS asn_v6,
        CAST(coalesce(sum(v4_packets_sent), 0) AS bigint)                               AS v4_packets_sent,
        CAST(coalesce(sum(v4_packets_lost), 0) AS bigint)                               AS v4_packets_lost,
        CAST(coalesce(sum(v4_valid_rtt_count), 0) AS bigint)                            AS v4_rtt_count,
        CAST(coalesce(sum(v6_packets_sent), 0) AS bigint)                               AS v6_packets_sent,
        CAST(coalesce(sum(v6_packets_lost), 0) AS bigint)                               AS v6_packets_lost,
        CAST(coalesce(sum(v6_valid_rtt_count), 0) AS bigint)                            AS v6_rtt_count,
        sum(v4_rtt_sum_ms)                                                              AS v4_rtt_sum_ms,
        sum(v6_rtt_sum_ms)                                                              AS v6_rtt_sum_ms
    FROM pairs
    GROUP BY 1, 2, 3, 4, 5, 6
),
paired_ids AS (
    SELECT v4_result_id AS result_id FROM pairs UNION ALL SELECT v6_result_id FROM pairs
),
unpaired AS (
    SELECT probe_set_version, target, probe_id, ictrl_dt, event_hour_utc, event_hour_local,
        sum(CAST(af = 4 AS bigint))   AS unpaired_v4_results,
        sum(CAST(af = 6 AS bigint))   AS unpaired_v6_results,
        arbitrary(country_code)       AS u_country_code
    FROM eligible e
    WHERE NOT EXISTS (SELECT 1 FROM paired_ids p WHERE p.result_id = e.result_id)
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    coalesce(a.probe_set_version, u.probe_set_version)   AS probe_set_version,
    coalesce(a.target, u.target)                         AS target,
    coalesce(a.probe_id, u.probe_id)                     AS probe_id,
    coalesce(a.ictrl_dt, u.ictrl_dt)                     AS ictrl_dt,
    coalesce(a.event_hour_utc, u.event_hour_utc)         AS event_hour_utc,
    coalesce(a.event_hour_local, u.event_hour_local)     AS event_hour_local,
    coalesce(a.pairs, 0)                                 AS pairs,
    a.pair_dt_mean_s,
    coalesce(a.country_code, u.u_country_code)          AS country_code,
    a.asn_v4,
    a.asn_v6,
    coalesce(a.v4_packets_sent, 0)                       AS v4_packets_sent,
    coalesce(a.v4_packets_lost, 0)                       AS v4_packets_lost,
    coalesce(a.v4_rtt_count, 0)                          AS v4_rtt_count,
    coalesce(a.v6_packets_sent, 0)                       AS v6_packets_sent,
    coalesce(a.v6_packets_lost, 0)                       AS v6_packets_lost,
    coalesce(a.v6_rtt_count, 0)                          AS v6_rtt_count,
    a.v4_rtt_sum_ms,
    a.v6_rtt_sum_ms,
    coalesce(u.unpaired_v4_results, 0)                   AS unpaired_v4_results,
    coalesce(u.unpaired_v6_results, 0)                   AS unpaired_v6_results,
    CASE coalesce(a.target, u.target) WHEN 'k.root-servers.net' THEN 1001 WHEN 'f.root-servers.net' THEN 1004 END AS measurement_id_v4,
    CASE coalesce(a.target, u.target) WHEN 'k.root-servers.net' THEN 2001 WHEN 'f.root-servers.net' THEN 2004 END AS measurement_id_v6,
    {{ gold_meta() }}
FROM agg_pairs a
FULL OUTER JOIN unpaired u
  ON a.probe_set_version = u.probe_set_version AND a.target = u.target AND a.probe_id = u.probe_id
 AND a.ictrl_dt = u.ictrl_dt AND a.event_hour_utc = u.event_hour_utc AND a.event_hour_local = u.event_hour_local
