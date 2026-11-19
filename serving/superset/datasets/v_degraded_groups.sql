-- Quốc gia / ASN có chất lượng đường đo suy giảm: so cửa sổ gần nhất với baseline ngay trước đó,
-- CHỈ TRÊN CÁC PROBE CÓ DỮ LIỆU Ở CẢ HAI CỬA SỔ (so sánh cùng probe).
-- Lý do: so tổng nhóm giữa hai cửa sổ có thành phần probe khác nhau (probe mới/mất) cho kết quả sai.
-- Đo thực tế ngày 2026-11-19: so tổng nhóm gắn cờ 104 quốc gia "degraded" chỉ vì baseline có ~140 probe,
-- recent có ~4.300 probe.
--
-- NGƯỠNG LÀ CẤU HÌNH ĐỀ XUẤT (chưa hiệu chỉnh trên lịch sử dài): sửa tại CTE params.
--   recent   = 3 giờ gần nhất có dữ liệu của cohort; baseline = 24 giờ ngay trước recent
--   degraded khi: loss tăng >= 2 điểm %  HOẶC  RTT trung bình theo packet tăng >= 20 %
--   đủ mẫu khi: >= 2 probe chung VÀ mỗi cửa sổ >= 50 result của các probe chung
WITH params AS (
  SELECT 3 AS recent_hours, 24 AS baseline_hours, 2.0 AS loss_pp, 1.2 AS rtt_ratio, 50 AS min_results, 2 AS min_common_probes
),
bounds AS (SELECT probe_set_version, max(event_hour_local) AS mx FROM nessie.gold.quality_hourly_probe GROUP BY 1),
w AS (
  SELECT q.probe_set_version, q.measurement_id, q.target, q.af, q.probe_id, q.country_code, q.asn,
         q.results_total, q.packets_sent, q.packets_lost, q.rtt_sum_ms, q.rtt_count,
         CASE WHEN q.event_hour_local > b.mx - p.recent_hours * INTERVAL '1' HOUR THEN 'recent'
              WHEN q.event_hour_local > b.mx - (p.recent_hours + p.baseline_hours) * INTERVAL '1' HOUR THEN 'baseline' END AS win
  FROM nessie.gold.quality_hourly_probe q
  JOIN bounds b ON b.probe_set_version = q.probe_set_version
  CROSS JOIN params p
),
per_probe AS (
  SELECT probe_set_version, measurement_id, target, af, probe_id,
         max(country_code) AS country_code, max(asn) AS asn,
         sum(CASE WHEN win = 'recent' THEN results_total END) AS r_results,
         sum(CASE WHEN win = 'recent' THEN packets_sent END) AS r_sent,
         sum(CASE WHEN win = 'recent' THEN packets_lost END) AS r_lost,
         sum(CASE WHEN win = 'recent' THEN rtt_sum_ms END) AS r_rtt_sum,
         sum(CASE WHEN win = 'recent' THEN rtt_count END) AS r_rtt_n,
         sum(CASE WHEN win = 'baseline' THEN results_total END) AS b_results,
         sum(CASE WHEN win = 'baseline' THEN packets_sent END) AS b_sent,
         sum(CASE WHEN win = 'baseline' THEN packets_lost END) AS b_lost,
         sum(CASE WHEN win = 'baseline' THEN rtt_sum_ms END) AS b_rtt_sum,
         sum(CASE WHEN win = 'baseline' THEN rtt_count END) AS b_rtt_n
  FROM w WHERE win IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5
  HAVING sum(CASE WHEN win = 'recent' THEN results_total END) > 0
     AND sum(CASE WHEN win = 'baseline' THEN results_total END) > 0
),
grouped AS (
  SELECT 'country' AS level, country_code AS group_key, probe_set_version, measurement_id, target, af,
         count(*) AS common_probes, sum(r_results) AS r_results, sum(b_results) AS b_results,
         sum(r_sent) AS r_sent, sum(r_lost) AS r_lost, sum(r_rtt_sum) AS r_rtt_sum, sum(r_rtt_n) AS r_rtt_n,
         sum(b_sent) AS b_sent, sum(b_lost) AS b_lost, sum(b_rtt_sum) AS b_rtt_sum, sum(b_rtt_n) AS b_rtt_n
  FROM per_probe GROUP BY 1, 2, 3, 4, 5, 6
  UNION ALL
  SELECT 'asn', CAST(asn AS varchar), probe_set_version, measurement_id, target, af,
         count(*), sum(r_results), sum(b_results), sum(r_sent), sum(r_lost), sum(r_rtt_sum), sum(r_rtt_n),
         sum(b_sent), sum(b_lost), sum(b_rtt_sum), sum(b_rtt_n)
  FROM per_probe GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT g.level, g.group_key, g.probe_set_version, g.measurement_id, g.target, g.af,
       g.common_probes, g.r_results AS recent_results, g.b_results AS baseline_results,
       100 * CAST(g.r_lost AS double) / nullif(g.r_sent, 0) AS recent_loss_pct,
       100 * CAST(g.b_lost AS double) / nullif(g.b_sent, 0) AS baseline_loss_pct,
       g.r_rtt_sum / nullif(g.r_rtt_n, 0) AS recent_rtt_mean_ms,
       g.b_rtt_sum / nullif(g.b_rtt_n, 0) AS baseline_rtt_mean_ms,
       CASE
         WHEN g.common_probes < p.min_common_probes OR g.r_results < p.min_results OR g.b_results < p.min_results
           THEN 'insufficient_sample'
         WHEN 100 * CAST(g.r_lost AS double) / nullif(g.r_sent, 0) - 100 * CAST(g.b_lost AS double) / nullif(g.b_sent, 0) >= p.loss_pp
           OR g.r_rtt_sum / nullif(g.r_rtt_n, 0) >= p.rtt_ratio * (g.b_rtt_sum / nullif(g.b_rtt_n, 0))
           THEN 'degraded'
         ELSE 'normal'
       END AS status
FROM grouped g CROSS JOIN params p
