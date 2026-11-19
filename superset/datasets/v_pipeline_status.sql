-- Trạng thái dữ liệu cho hàng đầu dashboard: dữ liệu mới đến đâu, đã công bố chưa, có trễ quá SLO không.
-- SLO độ tươi đề xuất: Gold trong vòng 120 phút tính từ event_time (docs/design.md, giả định A3).
WITH c AS (
  SELECT probe_set_version, measurement_id, target, af,
         max(CASE WHEN data_state = 'measured' THEN event_hour_local END) AS latest_measured_hour_local,
         max(max_event_time) AS latest_event_time_utc,
         max(gold_built_at) AS gold_built_at,
         count_if(data_state = 'not_processed' AND event_hour_utc < current_timestamp - INTERVAL '2' HOUR) AS hours_not_processed_past_slo
  FROM nessie.gold.coverage_hourly
  GROUP BY 1, 2, 3, 4
),
silver AS (
  SELECT measurement_id, max_by(status, ictrl_dt) AS silver_status, max(ictrl_dt) AS silver_latest_day
  FROM nessie.ops.partition_status WHERE table_name = 'nessie.silver.fact_ping_result' GROUP BY 1
),
gold AS (
  SELECT measurement_id, max_by(status, ictrl_dt) AS gold_status, max(ictrl_dt) AS gold_latest_day
  FROM nessie.ops.partition_status WHERE table_name = 'nessie.gold.quality_hourly_target' GROUP BY 1
)
SELECT c.probe_set_version, c.measurement_id, c.target, c.af,
       c.latest_measured_hour_local,
       c.latest_event_time_utc,
       date_diff('minute', c.latest_event_time_utc, current_timestamp) AS freshness_minutes,
       CASE WHEN date_diff('minute', c.latest_event_time_utc, current_timestamp) <= 120 THEN 'fresh' ELSE 'stale (> 120 min)' END AS freshness_state,
       c.hours_not_processed_past_slo,
       silver.silver_status, gold.gold_status, c.gold_built_at
FROM c
LEFT JOIN silver ON silver.measurement_id = c.measurement_id
LEFT JOIN gold ON gold.measurement_id = c.measurement_id
