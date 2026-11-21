{# Generic test của đồ án. Mỗi test trả về các dòng vi phạm; 0 dòng = đạt. #}

{# ---------------------------------------------------------------- Bronze: kiểm tra mức lô #}
{% test staged_rows_match_manifest(model, entity) %}
{#- Số dòng đọc được của mỗi manifest = số dòng manifest ghi đã stage (object thiếu -> 0 dòng -> lỗi). -#}
WITH expected AS (
    SELECT manifest_path, max(manifest_staged_rows) AS expected_rows
    FROM {{ source('runner', 'bronze_pending_object') }} WHERE entity = '{{ entity }}'
    GROUP BY 1
),
got AS (SELECT manifest_path, count(*) AS got_rows FROM {{ model }} GROUP BY 1)
SELECT e.manifest_path, e.expected_rows, coalesce(g.got_rows, 0) AS got_rows
FROM expected e LEFT JOIN got g ON e.manifest_path = g.manifest_path
WHERE e.expected_rows <> coalesce(g.got_rows, 0)
{% endtest %}

{% test load_id_matches_manifest(model) %}
SELECT source_object, dlt_load_id, expected_load_id, count(*) AS n
FROM {{ model }}
WHERE dlt_load_id IS DISTINCT FROM expected_load_id
GROUP BY 1, 2, 3
{% endtest %}

{% test quarantine_ratio_at_most(model, max_ratio) %}
SELECT quarantined, total, CAST(quarantined AS double) / total AS ratio
FROM (SELECT count_if(reason IS NOT NULL) AS quarantined, count(*) AS total FROM {{ model }})
WHERE total > 0 AND CAST(quarantined AS double) / total > {{ max_ratio }}
{% endtest %}

{# ---------------------------------------------------------------- tỷ lệ (thay `mostly` của GX) #}
{% test ratio_at_least(model, condition, min_ratio, where_clause='1 = 1') %}
{#- Tỷ lệ dòng thỏa `condition` (trong các dòng thỏa where_clause) phải >= min_ratio. -#}
SELECT ok, total, CAST(ok AS double) / total AS ratio
FROM (
    SELECT count_if(coalesce({{ condition }}, false)) AS ok, count(*) AS total
    FROM {{ model }} WHERE {{ where_clause }}
)
WHERE total > 0 AND CAST(ok AS double) / total < {{ min_ratio }}
{% endtest %}

{# ---------------------------------------------------------------- miền giá trị #}
{% test expression_is_true(model, expression, where_clause='1 = 1') %}
SELECT * FROM {{ model }} WHERE {{ where_clause }} AND NOT coalesce({{ expression }}, true)
{% endtest %}

{% test unique_combination(model, columns) %}
SELECT {{ columns | join(', ') }}, count(*) AS n
FROM {{ model }}
GROUP BY {{ columns | join(', ') }}
HAVING count(*) > 1
{% endtest %}

{# ---------------------------------------------------------------- SCD2 #}
{% test scd2_valid(model, key) %}
{#- Không chồng lấn hiệu lực, đúng một bản current mỗi key, phiên bản liền mạch, đúng một unknown member. -#}
WITH real AS (SELECT * FROM {{ model }} WHERE NOT is_unknown_member),
overlap_rows AS (
    SELECT 'overlap' AS problem, a.{{ key }} AS k
    FROM real a JOIN real b
      ON a.{{ key }} = b.{{ key }} AND a.version_no < b.version_no
     AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to
),
not_one_current AS (
    SELECT 'not_one_current' AS problem, {{ key }} AS k FROM real GROUP BY {{ key }} HAVING count_if(is_current) <> 1
),
gap_rows AS (
    SELECT 'gap' AS problem, x.{{ key }} AS k
    FROM real x JOIN real y ON x.{{ key }} = y.{{ key }} AND y.version_no = x.version_no + 1
    WHERE x.valid_to <> y.valid_from
),
unknown_rows AS (
    SELECT 'unknown_member_count' AS problem, CAST(count(*) AS bigint) AS k
    FROM {{ model }} WHERE is_unknown_member HAVING count(*) <> 1
)
SELECT problem, CAST(k AS bigint) AS k FROM overlap_rows
UNION ALL SELECT problem, CAST(k AS bigint) FROM not_one_current
UNION ALL SELECT problem, CAST(k AS bigint) FROM gap_rows
UNION ALL SELECT problem, k FROM unknown_rows
{% endtest %}
