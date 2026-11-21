{{ config(tags=['gold_build']) }}
-- Các mart cùng tính từ một tập result nên tổng phải khớp nhau; tránh lặp logic sai lệch giữa các mart.
{% set keys = ['ictrl_dt', 'event_hour_utc', 'event_hour_local', 'probe_set_version', 'measurement_id'] %}
{% set measures = ['results_total', 'results_measured', 'results_with_response', 'packets_sent', 'packets_lost', 'rtt_count'] %}
WITH t AS ({{ gold_totals('quality_hourly_target_candidate', keys, measures) }}),
{% for mart in ['country', 'asn', 'probe'] %}
{{ mart }}_mismatch AS (
    SELECT 'target_eq_sum_{{ mart }}' AS check_name, count(*) AS n
    FROM t FULL OUTER JOIN ({{ gold_totals('quality_hourly_' ~ mart ~ '_candidate', keys, measures) }}) x
      ON {% for k in keys %}t.{{ k }} = x.{{ k }}{{ ' AND ' if not loop.last }}{% endfor %}
    WHERE {% for m in measures %}t.{{ m }} IS DISTINCT FROM x.{{ m }}{{ ' OR ' if not loop.last }}{% endfor %}
),
{% endfor %}
hist AS (
    SELECT 'histogram_total_eq_rtt_count' AS check_name, count(*) AS n
    FROM (SELECT {{ keys | join(', ') }}, rtt_count FROM {{ ref('quality_hourly_target_candidate') }}) t2
    FULL OUTER JOIN (
        SELECT {{ keys | join(', ') }}, sum(reply_count) AS h FROM {{ ref('rtt_histogram_hourly_candidate') }} GROUP BY {{ keys | join(', ') }}
    ) h ON {% for k in keys %}t2.{{ k }} = h.{{ k }}{{ ' AND ' if not loop.last }}{% endfor %}
    WHERE coalesce(t2.rtt_count, 0) <> coalesce(h.h, 0)
),
relations AS (
    SELECT 'measure_relations' AS check_name, count(*) AS n FROM {{ ref('quality_hourly_target_candidate') }}
    WHERE packets_lost <> packets_sent - packets_received OR results_with_response > results_measured
       OR results_measured > results_total OR packets_lost < 0
),
coverage_vs_target AS (
    SELECT 'coverage_eligible_eq_target_results' AS check_name, count(*) AS n
    FROM {{ ref('coverage_hourly_candidate') }} c
    FULL OUTER JOIN {{ ref('quality_hourly_target_candidate') }} t3
      ON c.probe_set_version = t3.probe_set_version AND c.measurement_id = t3.measurement_id AND c.event_hour_utc = t3.event_hour_utc
    WHERE coalesce(c.results_eligible, 0) <> coalesce(t3.results_total, 0)
),
grid AS (
    SELECT 'coverage_full_hour_grid' AS check_name, abs(count(*) - count(DISTINCT probe_set_version) * 24 * 4) AS n
    FROM {{ ref('coverage_hourly_candidate') }}
),
pair_relations AS (
    SELECT 'pair_relations' AS check_name, count(*) AS n FROM {{ ref('ipv4_ipv6_paired_hourly_candidate') }}
    WHERE pairs < 0 OR v4_packets_lost > v4_packets_sent OR v6_packets_lost > v6_packets_sent
)
SELECT * FROM (
    SELECT * FROM country_mismatch UNION ALL SELECT * FROM asn_mismatch UNION ALL SELECT * FROM probe_mismatch
    UNION ALL SELECT * FROM hist UNION ALL SELECT * FROM relations UNION ALL SELECT * FROM coverage_vs_target
    UNION ALL SELECT * FROM grid UNION ALL SELECT * FROM pair_relations
) WHERE n > 0
