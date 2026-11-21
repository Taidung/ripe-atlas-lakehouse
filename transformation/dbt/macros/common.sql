{# Macro dùng chung. Hằng số nghiệp vụ giống transformation/lakehouse/common.py. #}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Dùng đúng tên schema khai báo (bronze, silver, gold, ops), không ghép tiền tố target. Bảng tạm (`work`) vào
        schema riêng của lượt chạy, runner truyền qua var work_schema (lakehouse/trino_db.py::work_schema). -#}
    {%- if custom_schema_name is none or custom_schema_name == 'work' -%}{{ var('work_schema', 'work') }}
    {%- else -%}{{ custom_schema_name }}{%- endif -%}
{%- endmacro %}

{% macro report_tz() %}'Asia/Ho_Chi_Minh'{% endmacro %}

{% macro sha256_hex(expr) -%}
    lower(to_hex(sha256(to_utf8({{ expr }}))))
{%- endmacro %}

{% macro uid(cols) -%}
    {#- sha256(c1|c2|...), NULL -> '' : giống _uid() của bản Spark nên record_uid của dữ liệu cũ vẫn khớp. -#}
    {%- set parts = [] -%}
    {%- for c in cols -%}{%- do parts.append("coalesce(CAST(" ~ c ~ " AS varchar), '')") -%}{%- endfor -%}
    {{ sha256_hex("concat_ws('|', " ~ parts | join(', ') ~ ")") }}
{%- endmacro %}

{% macro measurements() %}{{ return({1001: (4, 'k.root-servers.net'), 1004: (4, 'f.root-servers.net'),
                                     2001: (6, 'k.root-servers.net'), 2004: (6, 'f.root-servers.net')}) }}{% endmacro %}

{% macro msm_af(col) -%}
    CASE {{ col }} {% for m, v in measurements().items() %}WHEN {{ m }} THEN {{ v[0] }} {% endfor %}END
{%- endmacro %}

{% macro msm_target(col) -%}
    CASE {{ col }} {% for m, v in measurements().items() %}WHEN {{ m }} THEN '{{ v[1] }}' {% endfor %}END
{%- endmacro %}

{% macro run_ts() -%}
    CAST(from_iso8601_timestamp('{{ var("run_ts") }}') AS timestamp(6) with time zone)
{%- endmacro %}

{% macro epoch_ts() %}TIMESTAMP '1970-01-01 00:00:00.000000 UTC'{% endmacro %}
{% macro open_end_ts() %}TIMESTAMP '9999-12-31 00:00:00.000000 UTC'{% endmacro %}

{% macro to_ts6(expr) -%}
    CAST({{ expr }} AS timestamp(6) with time zone)
{%- endmacro %}

{% macro day_literal() %}DATE '{{ var("day") }}'{% endmacro %}

