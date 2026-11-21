{{ config(materialized='table', tags=['silver_dims_publish']) }}
{#- Một dòng mỗi phút trong ngày (giờ VN); time_key = HHMM. -#}
WITH m AS (
    SELECT minute_of_day FROM UNNEST(sequence(0, 1439)) AS t(minute_of_day)
)
SELECT
    CAST((minute_of_day / 60) * 100 + minute_of_day % 60 AS integer) AS time_key,
    CAST(minute_of_day / 60 AS integer)                              AS hour,
    CAST(minute_of_day % 60 AS integer)                              AS minute,
    format('%02d:00', minute_of_day / 60)                            AS hour_label,
    CASE WHEN minute_of_day / 60 < 6 THEN 'night'
         WHEN minute_of_day / 60 < 12 THEN 'morning'
         WHEN minute_of_day / 60 < 18 THEN 'afternoon'
         ELSE 'evening' END                                          AS part_of_day
FROM m
