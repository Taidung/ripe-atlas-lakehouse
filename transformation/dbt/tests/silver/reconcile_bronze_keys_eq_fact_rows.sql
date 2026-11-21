{{ config(tags=['silver_facts_build']) }}
-- Đối soát: số khóa (measurement, probe, event_ts) phân biệt ở Bronze của ngày = số dòng fact.
WITH b AS (
    SELECT count(*) AS n FROM (
        SELECT DISTINCT measurement_id, probe_id, event_ts FROM {{ ref('ping_result_raw') }} WHERE ictrl_dt = {{ day_literal() }}
    )
),
f AS (SELECT count(*) AS n FROM {{ ref('fact_ping_result_candidate') }})
SELECT b.n AS bronze_distinct_keys, f.n AS fact_rows FROM b CROSS JOIN f WHERE b.n <> f.n
