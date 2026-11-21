{{ config(tags=['silver_facts_build']) }}
-- Đối soát: số reply = tổng số phần tử result; số reply hợp lệ = tổng valid_rtt_count; không có reply mồ côi.
WITH f AS (
    SELECT coalesce(sum(reply_element_count), 0) AS elements, coalesce(sum(valid_rtt_count), 0) AS valid
    FROM {{ ref('fact_ping_result_candidate') }}
),
r AS (
    SELECT count(*) AS rows_, count_if(is_valid_rtt) AS valid FROM {{ ref('fact_ping_reply_candidate') }}
),
orphans AS (
    SELECT count(*) AS n FROM {{ ref('fact_ping_reply_candidate') }} r
    WHERE NOT EXISTS (SELECT 1 FROM {{ ref('fact_ping_result_candidate') }} f WHERE f.result_id = r.result_id)
)
SELECT 'reply_rows_eq_elements' AS check_name, r.rows_ AS got, f.elements AS expected FROM f CROSS JOIN r WHERE r.rows_ <> f.elements
UNION ALL
SELECT 'valid_replies_eq_valid_rtt_count', r.valid, f.valid FROM f CROSS JOIN r WHERE r.valid <> f.valid
UNION ALL
SELECT 'no_orphan_replies', o.n, 0 FROM orphans o WHERE o.n > 0
