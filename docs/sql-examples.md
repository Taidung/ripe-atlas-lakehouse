# Truy vấn mẫu qua Trino (SQL client)

**Kết nối:** host `localhost`, port `8081`, catalog `nessie`, user tùy ý (môi trường đồ án chưa bật xác thực).
- Trino dùng khóa S3 chỉ đọc, không ghi được dữ liệu.
- Giới hạn mỗi truy vấn: 5 phút và 1 GB bộ nhớ (`trino/config.properties`).

Với mọi truy vấn Gold, **luôn lọc đúng một `probe_set_version`** và **tính tỷ lệ từ tổng**, không lấy trung bình các tỷ lệ.

## Gold

KPI theo target và address family trong một ngày (giờ VN):

```sql
SELECT target, af,
       sum(results_total)                                                   AS results,
       100 * CAST(sum(packets_lost) AS double) / nullif(sum(packets_sent), 0) AS packet_loss_pct,
       sum(rtt_sum_ms) / nullif(sum(rtt_count), 0)                            AS rtt_mean_ms,
       100 * CAST(sum(results_with_response) AS double) / nullif(sum(results_measured), 0) AS response_rate_observed_pct
FROM nessie.gold.quality_hourly_target
WHERE probe_set_version = 'strat-k2-c100-v1' AND ictrl_dt = DATE '2026-11-19'
GROUP BY 1, 2 ORDER BY 1, 2;
```

p95 RTT theo packet theo quốc gia, tính từ histogram (gộp được ở mọi grain):

```sql
SELECT country_code, sum(reply_count) AS replies,
       approx_percentile(bucket_rep_ms, reply_count, 0.95) AS rtt_p95_ms
FROM nessie.gold.rtt_histogram_hourly
WHERE probe_set_version = 'strat-k2-c100-v1' AND af = 4
GROUP BY 1 HAVING sum(reply_count) >= 500 ORDER BY rtt_p95_ms DESC LIMIT 20;
```

So sánh IPv4/IPv6 trên cặp cùng probe:

```sql
SELECT target, sum(pairs) AS pairs,
       100 * CAST(sum(v4_packets_lost) AS double) / nullif(sum(v4_packets_sent), 0) AS v4_loss_pct,
       100 * CAST(sum(v6_packets_lost) AS double) / nullif(sum(v6_packets_sent), 0) AS v6_loss_pct,
       sum(v4_rtt_sum_ms) / nullif(sum(v4_rtt_count), 0) AS v4_rtt_ms,
       sum(v6_rtt_sum_ms) / nullif(sum(v6_rtt_count), 0) AS v6_rtt_ms,
       sum(unpaired_v4_results) AS unpaired_v4, sum(unpaired_v6_results) AS unpaired_v6
FROM nessie.gold.ipv4_ipv6_paired_hourly
WHERE probe_set_version = 'strat-k2-c100-v1'
GROUP BY 1;
```

Độ phủ và trạng thái xử lý theo giờ:

```sql
SELECT event_hour_local, measurement_id, data_state, expected_probes, probes_observed,
       results_total_raw, expected_results, silver_status
FROM nessie.gold.coverage_hourly
WHERE probe_set_version = 'strat-k2-c100-v1' AND ictrl_dt = DATE '2026-11-19'
ORDER BY 1, 2;
```

## Drill-down xuống Silver

Các result mất toàn bộ packet của một probe, kèm từng reply:

```sql
SELECT f.event_time, f.measurement_id, f.result_status, f.packets_sent, f.packets_received,
       r.reply_seq, r.reply_type, r.rtt_ms, r.error_message
FROM nessie.silver.fact_ping_result f
JOIN nessie.silver.fact_ping_reply r ON r.result_id = f.result_id AND r.ictrl_dt = f.ictrl_dt
WHERE f.ictrl_dt = DATE '2026-11-19' AND f.probe_id = 3108 AND f.result_status = 'all_lost'
ORDER BY f.event_time, r.reply_seq;
```

Lịch sử metadata (SCD2) của một probe:

```sql
SELECT probe_id, version_no, country_code, asn_v4, asn_v6, is_public, valid_from, valid_to, is_current,
       observation_count, prev_version_last_observed_at
FROM nessie.silver.dim_probe WHERE probe_id = 3108 ORDER BY version_no;
```

Truy ngược một result Silver về payload gốc và object staging:

```sql
SELECT f.result_id, b.raw_json, b.source_object, b.manifest_path, b.ingest_run_id, b.fetched_at
FROM nessie.silver.fact_ping_result f
JOIN nessie.bronze.ping_result_raw b ON b.record_uid = f.record_uid AND b.ictrl_dt = f.ictrl_dt
WHERE f.ictrl_dt = DATE '2026-11-19' AND f.probe_id = 3108 LIMIT 5;
```

Kiểm tra chất lượng thất bại gần đây:

```sql
SELECT checked_at, layer, table_name, partition_scope, severity, check_name, observed_value
FROM nessie.ops.dq_result WHERE NOT success ORDER BY checked_at DESC LIMIT 20;
```
