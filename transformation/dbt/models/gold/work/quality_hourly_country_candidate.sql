{{ config(materialized='table', schema='work', tags=['gold_build']) }}
SELECT {{ hour_keys() }}, {{ msm_keys() }}, country_code,
    {{ measure_aggs() }},
    count(DISTINCT probe_id)                                         AS probes_observed,
    count(DISTINCT CASE WHEN asn <> -1 THEN asn END)                 AS asns_observed,
    {{ gold_meta() }}
FROM {{ ref('gold_cohort') }} WHERE is_eligible
GROUP BY {{ hour_keys() }}, {{ msm_keys() }}, country_code
