{{ config(materialized='table', schema='work', tags=['gold_build']) }}
SELECT {{ hour_keys() }}, {{ msm_keys() }}, asn,
    {{ measure_aggs() }},
    count(DISTINCT probe_id)                                         AS probes_observed,
    count(DISTINCT country_code)                                     AS asn_country_count,
    min(country_code)                                                AS asn_primary_country,
    arbitrary(isp_name)                                              AS isp_name,
    arbitrary(isp_type)                                              AS isp_type,
    {{ gold_meta() }}
FROM {{ ref('gold_cohort') }} WHERE is_eligible
GROUP BY {{ hour_keys() }}, {{ msm_keys() }}, asn
