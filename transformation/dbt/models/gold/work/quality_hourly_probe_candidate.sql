{{ config(materialized='table', schema='work', tags=['gold_build']) }}
SELECT {{ hour_keys() }}, {{ msm_keys() }}, probe_id,
    {{ measure_aggs() }},
    max(probe_sk)                                                    AS probe_sk,
    arbitrary(country_code)                                          AS country_code,
    arbitrary(asn)                                                   AS asn,
    {{ gold_meta() }}
FROM {{ ref('gold_cohort') }} WHERE is_eligible
GROUP BY {{ hour_keys() }}, {{ msm_keys() }}, probe_id
