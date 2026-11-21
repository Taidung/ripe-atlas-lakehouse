{{ config(materialized='table', tags=['silver_dims_publish']) }}
{#- Lịch 2010-2035. day_of_week theo ISO (1 = thứ Hai), iso_week theo ISO-8601. -#}
WITH d AS (
    SELECT full_date
    FROM UNNEST(sequence(DATE '2010-01-01', DATE '2035-12-31', INTERVAL '1' DAY)) AS t(full_date)
)
SELECT
    CAST(year(full_date) * 10000 + month(full_date) * 100 + day(full_date) AS integer) AS date_key,
    full_date,
    CAST(year(full_date) AS integer)                     AS year,
    CAST(quarter(full_date) AS integer)                  AS quarter,
    CAST(month(full_date) AS integer)                    AS month,
    format_datetime(CAST(full_date AS timestamp), 'MMMM') AS month_name,
    CAST(day(full_date) AS integer)                      AS day,
    CAST(day_of_week(full_date) AS integer)              AS day_of_week,
    format_datetime(CAST(full_date AS timestamp), 'EEEE') AS day_name,
    CAST(week(full_date) AS integer)                     AS iso_week,
    day_of_week(full_date) IN (6, 7)                     AS is_weekend
FROM d
