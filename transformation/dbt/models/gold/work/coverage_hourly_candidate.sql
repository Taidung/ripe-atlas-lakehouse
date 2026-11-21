{{ config(materialized='table', schema='work', tags=['gold_build']) }}
{#- Lưới đủ 24 giờ (giờ VN) × cohort × 4 measurement, kể cả giờ không có dữ liệu. Cohort = probe set được ingest
    trong ngày; probe set không có probe của một address family vẫn có đủ dòng với expected_probes = 0. -#}
WITH hours AS (
    SELECT
        CAST(h AS timestamp(6))                                                AS event_hour_local,
        CAST(with_timezone(h, {{ report_tz() }}) AT TIME ZONE 'UTC' AS timestamp(6) with time zone) AS event_hour_utc,
        {{ day_literal() }}                                                    AS ictrl_dt
    FROM UNNEST(sequence(CAST({{ day_literal() }} AS timestamp), CAST({{ day_literal() }} AS timestamp) + INTERVAL '23' HOUR,
                         INTERVAL '1' HOUR)) AS t(h)
),
intervals AS (
    SELECT measurement_id, interval_s FROM {{ ref('dim_measurement') }} WHERE is_current AND NOT is_unknown_member
),
msm AS (
    SELECT CAST(m AS integer) AS measurement_id, CAST({{ msm_af('m') }} AS integer) AS af, {{ msm_target('m') }} AS target,
           coalesce(i.interval_s, 240) AS interval_s
    FROM UNNEST(ARRAY[1001, 1004, 2001, 2004]) AS t(m)
    LEFT JOIN intervals i ON i.measurement_id = m
),
members AS (
    SELECT * FROM {{ ref('probe_set_member') }}
    WHERE probe_set_version IN (SELECT probe_set_version FROM {{ ref('gold_ingested_hours') }})
),
cohorts AS (SELECT DISTINCT probe_set_version FROM members),
expected AS (SELECT probe_set_version, af, count(*) AS expected_probes FROM members GROUP BY 1, 2),
grid AS (
    SELECT hours.*, msm.*, c.probe_set_version, CAST(coalesce(e.expected_probes, 0) AS bigint) AS expected_probes
    FROM hours CROSS JOIN msm CROSS JOIN cohorts c
    LEFT JOIN expected e ON e.probe_set_version = c.probe_set_version AND e.af = msm.af
),
observed AS (
    SELECT probe_set_version, measurement_id, event_hour_utc,
        count(*)                                                       AS results_total_raw,
        sum(CAST(is_eligible AS bigint))                               AS results_eligible,
        sum(CAST(public_status <> 'public' AS bigint))                 AS results_excluded_not_public,
        sum(CAST(public_status = 'public' AND clock_suspect AS bigint)) AS results_excluded_clock,
        count(DISTINCT CASE WHEN is_eligible THEN probe_id END)        AS probes_observed,
        sum(CAST(is_source_corrected AS bigint))                       AS results_source_corrected,
        max(event_time)                                                AS max_event_time,
        max(silver_loaded_at)                                          AS silver_loaded_at
    FROM {{ ref('gold_cohort') }}
    GROUP BY 1, 2, 3
),
silver_status AS (
    SELECT measurement_id, status AS silver_status
    FROM {{ source('ops', 'partition_status') }}
    WHERE table_name = 'nessie.silver.fact_ping_result' AND ictrl_dt = {{ day_literal() }}
),
quarantine AS (
    SELECT measurement_id, date_trunc('hour', from_unixtime(event_ts, 'UTC')) AS event_hour_utc, count(*) AS quarantine_records
    FROM {{ ref('quarantine') }}
    WHERE entity = 'ping_result_raw' AND ictrl_dt = {{ day_literal() }} AND event_ts IS NOT NULL
    GROUP BY 1, 2
),
joined AS (
    SELECT
        g.measurement_id, g.event_hour_utc, g.ictrl_dt, g.probe_set_version, g.af, g.event_hour_local, g.target,
        g.interval_s, g.expected_probes,
        coalesce(o.results_total_raw, 0)            AS results_total_raw,
        coalesce(o.results_eligible, 0)             AS results_eligible,
        coalesce(o.results_excluded_not_public, 0)  AS results_excluded_not_public,
        coalesce(o.results_excluded_clock, 0)       AS results_excluded_clock,
        coalesce(o.probes_observed, 0)              AS probes_observed,
        coalesce(o.results_source_corrected, 0)     AS results_source_corrected,
        o.max_event_time,
        o.silver_loaded_at,
        coalesce(p.manifests, 0)                    AS manifests,
        coalesce(p.overlap_unchanged_skipped, 0)    AS overlap_unchanged_skipped,
        coalesce(s.silver_status, 'not_built')      AS silver_status,
        coalesce(q.quarantine_records, 0)           AS quarantine_records
    FROM grid g
    LEFT JOIN observed o
      ON o.probe_set_version = g.probe_set_version AND o.measurement_id = g.measurement_id AND o.event_hour_utc = g.event_hour_utc
    LEFT JOIN {{ ref('gold_ingested_hours') }} p
      ON p.probe_set_version = g.probe_set_version AND p.measurement_id = g.measurement_id AND p.event_hour_utc = g.event_hour_utc
    LEFT JOIN silver_status s ON s.measurement_id = g.measurement_id
    LEFT JOIN quarantine q ON q.measurement_id = g.measurement_id AND q.event_hour_utc = g.event_hour_utc
)
SELECT j.*,
    CAST(floor(expected_probes * (3600e0 / interval_s)) AS bigint)   AS expected_results,
    CASE WHEN results_total_raw > 0 THEN 'measured'
         WHEN manifests > 0 THEN 'processed_no_results'
         ELSE 'not_processed' END                                  AS data_state,
    {{ gold_meta() }}
FROM joined j
