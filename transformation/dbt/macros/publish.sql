{# Chiến lược incremental `replace_day`: thay toàn bộ dữ liệu của ngày var('day') trong bảng công bố bằng bản nháp,
   trong MỘT lệnh MERGE = một commit Iceberg. Người đọc (Superset, Trino) chỉ thấy bản cũ hoặc bản mới, không bao giờ
   thấy ngày trống; lỗi giữa chừng không để lại gì.
   - dòng mới trùng khóa (unique_key) -> UPDATE; dòng mới chưa có -> INSERT;
   - dòng cũ của ngày không còn trong bản mới -> đưa vào nguồn với cờ xóa -> DELETE.
   Khóa trùng trong bản nháp làm MERGE lỗi (một dòng đích khớp nhiều dòng nguồn): không ghi gì. Dòng cờ xóa không bao giờ
   được INSERT lại (kể cả khi khóa có NULL nên không khớp). #}

{% macro get_incremental_replace_day_sql(arg_dict) %}
    {%- set target, source = arg_dict['target_relation'], arg_dict['temp_relation'] -%}
    {%- set keys = arg_dict['unique_key'] if arg_dict['unique_key'] is not string else [arg_dict['unique_key']] -%}
    {%- set cols = arg_dict['dest_columns'] | map(attribute='quoted') | list -%}
    {%- set key_match = [] -%}
    {%- for k in keys %}{% do key_match.append('n.' ~ k ~ ' = o.' ~ k) %}{% endfor -%}
MERGE INTO {{ target }} AS t
USING (
    -- chặn thêm: chỉ dữ liệu của đúng ngày đang ghi, kể cả khi bảng nguồn lẫn ngày khác
    SELECT {{ cols | join(', ') }}, false AS dbt_delete_flag FROM {{ source }} WHERE ictrl_dt = {{ day_literal() }}
    UNION ALL
    SELECT {% for c in cols %}o.{{ c }}, {% endfor %}true FROM {{ target }} AS o
    WHERE o.ictrl_dt = {{ day_literal() }}
      AND NOT EXISTS (SELECT 1 FROM {{ source }} AS n WHERE n.ictrl_dt = {{ day_literal() }} AND {{ key_match | join(' AND ') }})
) AS s
ON t.ictrl_dt = {{ day_literal() }}{% for k in keys %} AND t.{{ k }} = s.{{ k }}{% endfor %}
WHEN MATCHED AND s.dbt_delete_flag THEN DELETE
WHEN MATCHED THEN UPDATE SET {% for c in cols %}{{ c }} = s.{{ c }}{{ ', ' if not loop.last }}{% endfor %}
WHEN NOT MATCHED AND NOT s.dbt_delete_flag THEN INSERT ({{ cols | join(', ') }}) VALUES ({% for c in cols %}s.{{ c }}{{ ', ' if not loop.last }}{% endfor %})
{% endmacro %}

{% macro trino__make_temp_relation(base_relation, suffix='__dbt_tmp') %}
    {#- Bảng tạm của materialization incremental (`<bảng>__dbt_tmp`) vào schema tạm của lượt chạy, không nằm chung trong
        bronze/silver/gold: hai lượt chạy song song không ghi đè bảng tạm của nhau rồi MERGE nhầm dữ liệu. -#}
    {{ return(base_relation.incorporate(path={"schema": var('work_schema', 'work'), "identifier": base_relation.identifier ~ suffix})) }}
{% endmacro %}
