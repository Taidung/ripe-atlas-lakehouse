{# Macro của tầng Silver. Đặc tả cột: docs/silver-model.md. #}

{% macro null_safe_concat(cols) -%}
    {#- Ghép thuộc tính để băm; NULL được mã hóa riêng nên NULL và chuỗi rỗng khác nhau. -#}
    {%- set parts = [] -%}
    {%- for c in cols -%}{%- do parts.append("coalesce(CAST(" ~ c ~ " AS varchar), '<null>')") -%}{%- endfor -%}
    concat_ws('|', {{ parts | join(', ') }})
{%- endmacro %}

{% macro scd2(snapshots, key, key_type, tracked, entity, sk) -%}
{#- Gộp snapshot (key, observed_at, thuộc tính) thành phiên bản SCD2. Snapshot trùng thuộc tính không sinh version mới;
    thuộc tính quay lại giá trị cũ thì sinh version mới. Version đầu có join_from = 1970 để fact trước snapshot đầu
    vẫn gắn được (metadata_basis = assumed_from_first_snapshot). -#}
WITH snap AS (
    SELECT s.*, {{ sha256_hex(null_safe_concat(tracked)) }} AS attr_hash FROM ({{ snapshots }}) s
),
ordered AS (
    SELECT snap.*,
        lag(attr_hash) OVER (PARTITION BY {{ key }} ORDER BY observed_at, attr_hash)   AS prev_hash,
        lag(observed_at) OVER (PARTITION BY {{ key }} ORDER BY observed_at, attr_hash) AS prev_observed_at
    FROM snap
),
flagged AS (
    SELECT o.*, (prev_hash IS NULL OR prev_hash <> attr_hash) AS is_change FROM ordered o
),
numbered AS (
    SELECT f.*,
        sum(CASE WHEN is_change THEN 1 ELSE 0 END) OVER (
            PARTITION BY {{ key }} ORDER BY observed_at, attr_hash ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS version_no
    FROM flagged f
),
versions AS (
    SELECT
        {{ key }},
        CAST(version_no AS bigint)                                       AS version_no,
        min(observed_at)                                                 AS valid_from,
        max(observed_at)                                                 AS last_observed_at,
        count(*)                                                         AS observation_count,
        arbitrary(attr_hash)                                             AS attr_hash,
        min(CASE WHEN is_change THEN prev_observed_at END)               AS prev_version_last_observed_at
        {%- for c in tracked %},
        arbitrary({{ c }})                                               AS {{ c }}
        {%- endfor %}
    FROM numbered
    GROUP BY {{ key }}, version_no
),
real AS (
    SELECT v.*,
        coalesce(lead(valid_from) OVER (PARTITION BY {{ key }} ORDER BY version_no), {{ open_end_ts() }}) AS valid_to
    FROM versions v
)
SELECT
    CAST({{ key }} AS {{ key_type }})                                    AS {{ key }},
    version_no, valid_from, last_observed_at, observation_count, attr_hash, prev_version_last_observed_at,
    {{ tracked | join(', ') }},
    valid_to,
    valid_to = {{ open_end_ts() }}                                       AS is_current,
    CASE WHEN version_no = 1 THEN {{ epoch_ts() }} ELSE valid_from END   AS join_from,
    'observed_at'                                                        AS valid_from_basis,
    from_big_endian_64(xxhash64(to_utf8(concat_ws('|', '{{ entity }}', CAST({{ key }} AS varchar), to_iso8601(valid_from))))) AS {{ sk }},
    false                                                                AS is_unknown_member
FROM real
UNION ALL
-- Unknown member: fact không có metadata probe/measurement gắn vào sk = -1, không bị mất dòng.
SELECT
    CAST(-1 AS {{ key_type }}), CAST(0 AS bigint), {{ epoch_ts() }}, CAST(NULL AS timestamp(6) with time zone), CAST(0 AS bigint),
    'unknown', CAST(NULL AS timestamp(6) with time zone),
    {%- for c in tracked %} NULL,{% endfor %}
    {{ open_end_ts() }}, true, {{ epoch_ts() }}, 'unknown_member', CAST(-1 AS bigint), true
{%- endmacro %}

{% macro delete_day_if_exists() -%}
    {#- pre_hook của bảng ghi theo ngày: xóa partition ngày đang tính (kể cả khi kết quả mới rỗng). -#}
    {%- set rel = adapter.get_relation(this.database, this.schema, this.identifier) -%}
    {%- if rel is not none -%}
        DELETE FROM {{ this }} WHERE ictrl_dt = {{ day_literal() }}
    {%- else -%}
        SELECT 1
    {%- endif -%}
{%- endmacro %}


{% macro parse_replies(json_col) -%}
    {#- Mảng `result` của một ping -> array(row). Phần tử: {rtt}, {x: "*"} (timeout), {error}, kèm dup/ttl nếu có. -#}
    transform(
        coalesce(try_cast(json_extract({{ json_col }}, '$.result') AS array(json)), CAST(ARRAY[] AS array(json))),
        e -> CAST(ROW(
            try_cast(json_extract_scalar(e, '$.rtt') AS double),
            coalesce(json_extract_scalar(e, '$.x'), json_format(json_extract(e, '$.x'))),
            coalesce(json_extract_scalar(e, '$.error'), json_format(json_extract(e, '$.error'))),
            try_cast(json_extract_scalar(e, '$.dup') AS bigint),
            try_cast(json_extract_scalar(e, '$.ttl') AS bigint)
        ) AS ROW(rtt double, x varchar, error varchar, dup bigint, ttl bigint))
    )
{%- endmacro %}
