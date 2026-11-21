{{ config(
    materialized='incremental',
    incremental_strategy='replace_day',
    unique_key=['result_id'],
    tags=['silver_facts_publish'],
    properties={
        'partitioning': "ARRAY['ictrl_dt', 'measurement_id']",
        'sorted_by': "ARRAY['probe_id', 'event_ts']",
        'format_version': '2',
    },
) }}
{#- Một dòng = một lần ping (thường 3 gói) của một probe tới một target. Ghi lại cả ngày (var day) từ bản đã qua kiểm tra
    bằng một MERGE (macros/publish.sql). Runner chỉ chạy model này khi không có lỗi critical. -#}
SELECT
    result_id, measurement_id, af, target, probe_id, event_ts, event_time, event_hour_utc, ictrl_dt, date_key, time_key,
    packets_sent, packets_received, packets_lost, duplicate_count_src, result_status, has_response,
    src_rtt_min, src_rtt_avg, src_rtt_max, rtt_min_ms, rtt_avg_ms, rtt_max_ms,
    reply_element_count, valid_rtt_count, rtt_sum_ms, rtt_sumsq_ms2,
    duplicate_reply_count, timeout_count, error_count, unknown_element_count, counts_consistent,
    ttl, packet_size, proto, dst_addr, fw, mver, lts, stored_ts, ingest_delay_s, clock_suspect,
    source_version_count, is_source_corrected, record_uid, raw_sha256, ingest_run_id, probe_set_version,
    silver_run_id, silver_loaded_at, probe_sk, country_code, asn, public_status, metadata_basis, measurement_sk
FROM {{ ref('fact_ping_result_candidate') }}
