{# Macro tầng Gold. Mọi mart chỉ lưu tử số/mẫu số và thống kê cộng được; tỷ lệ tính lại sau khi group.
   Định nghĩa metric: docs/metric-dictionary.md. #}

{% macro bucket_base_ms() %}0.1e0{% endmacro %}
{% macro bucket_ratio() %}1.02e0{% endmacro %}

{% macro bucket_id(rtt) -%}
    CAST(floor(ln(greatest({{ rtt }}, {{ bucket_base_ms() }}) / {{ bucket_base_ms() }}) / ln({{ bucket_ratio() }})) AS integer)
{%- endmacro %}

{% macro local_hour(col) -%}
    {#- Giờ treo tường Asia/Ho_Chi_Minh, kiểu timestamp không múi giờ (cho Superset/Trino). -#}
    CAST({{ col }} AT TIME ZONE {{ report_tz() }} AS timestamp(6))
{%- endmacro %}

{% macro hour_keys() %}ictrl_dt, event_hour_utc, event_hour_local, probe_set_version{% endmacro %}
{% macro msm_keys() %}measurement_id, af, target{% endmacro %}

{% macro measure_aggs() -%}
    count(*)                                                                             AS results_total,
    sum(CAST(packets_sent > 0 AS bigint))                                                AS results_measured,
    sum(CAST(result_status = 'send_error' AS bigint))                                    AS results_send_error,
    sum(CAST(packets_sent > 0 AND packets_received > 0 AS bigint))                       AS results_with_response,
    sum(CAST(result_status = 'all_lost' AS bigint))                                      AS results_all_lost,
    sum(CAST(result_status = 'partial_loss' AS bigint))                                  AS results_partial_loss,
    CAST(coalesce(sum(CASE WHEN packets_sent > 0 THEN packets_sent END), 0) AS bigint)   AS packets_sent,
    CAST(coalesce(sum(CASE WHEN packets_sent > 0 THEN packets_received END), 0) AS bigint) AS packets_received,
    CAST(coalesce(sum(CASE WHEN packets_sent > 0 THEN packets_sent - packets_received END), 0) AS bigint) AS packets_lost,
    CAST(coalesce(sum(valid_rtt_count), 0) AS bigint)                                    AS rtt_count,
    sum(rtt_sum_ms)                                                                      AS rtt_sum_ms,
    sum(rtt_sumsq_ms2)                                                                   AS rtt_sumsq_ms2,
    min(rtt_min_ms)                                                                      AS rtt_min_ms,
    max(rtt_max_ms)                                                                      AS rtt_max_ms
{%- endmacro %}

{% macro gold_meta() -%}
    '{{ var("run_id") }}' AS gold_run_id,
    {{ run_ts() }}        AS gold_built_at
{%- endmacro %}

{% macro published(model, snapshot_var) -%}
    {#- Bảng Silver ĐÚNG tại snapshot đã công bố (ops.partition_status), không đọc bản mới nhất chưa qua kiểm tra. -#}
    {{ ref(model) }} FOR VERSION AS OF {{ var(snapshot_var) }}
{%- endmacro %}

{% macro readable_filter() -%}
    ictrl_dt = {{ day_literal() }} AND measurement_id IN ({{ var('measurement_ids') | join(', ') }})
{%- endmacro %}


{% macro gold_totals(model, keys, measures) -%}
    SELECT {{ keys | join(', ') }}, {% for m in measures %}sum({{ m }}) AS {{ m }}{{ ', ' if not loop.last }}{% endfor %}
    FROM {{ ref(model) }} GROUP BY {{ keys | join(', ') }}
{%- endmacro %}
