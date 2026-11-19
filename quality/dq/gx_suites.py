"""Bộ expectation Great Expectations (GX Core 1.25) cho từng bảng. Mỗi phần tử: (expectation, severity).

Ngưỡng `mostly` của các kiểm tra warn là cấu hình đề xuất, chưa hiệu chỉnh trên lịch sử dài.
"""

from __future__ import annotations

from dq.results import CRITICAL, WARN

MEASUREMENT_IDS = [1001, 1004, 2001, 2004]
MEASUREMENT_AF = [(1001, 4), (1004, 4), (2001, 6), (2004, 6)]


def silver_fact_result(expected_rows: tuple[int, int] | None = None):
    """silver.fact_ping_result: khóa, miền giá trị, quan hệ packet/RTT; tỷ lệ unknown/public/độ trễ là warn."""
    import great_expectations.expectations as gxe

    required = ("result_id", "measurement_id", "af", "probe_id", "event_ts", "event_time", "ictrl_dt",
                "date_key", "time_key", "probe_sk", "measurement_sk", "result_status", "record_uid")
    exps = [(gxe.ExpectColumnValuesToNotBeNull(column=c), CRITICAL) for c in required]
    exps += [
        # Khóa
        (gxe.ExpectColumnValuesToBeUnique(column="result_id"), CRITICAL),
        (gxe.ExpectCompoundColumnsToBeUnique(column_list=["measurement_id", "probe_id", "event_ts"]), CRITICAL),
        # Miền giá trị
        (gxe.ExpectColumnValuesToBeInSet(column="measurement_id", value_set=MEASUREMENT_IDS), CRITICAL),
        (gxe.ExpectColumnValuesToBeInSet(column="af", value_set=[4, 6]), CRITICAL),
        (gxe.ExpectColumnPairValuesToBeInSet(column_A="measurement_id", column_B="af", value_pairs_set=MEASUREMENT_AF), CRITICAL),
        (gxe.ExpectColumnValuesToBeInSet(column="result_status",
                                         value_set=["all_received", "partial_loss", "all_lost", "send_error"]), CRITICAL),
        # Quan hệ packet / RTT
        (gxe.ExpectColumnValuesToBeBetween(column="packets_sent", min_value=0, max_value=64), CRITICAL),
        (gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="packets_sent", column_B="packets_received",
                                                     or_equal=True, ignore_row_if="either_value_is_missing"), CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="rtt_min_ms", min_value=0), CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="rtt_avg_ms", min_value=0), CRITICAL),
        (gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="rtt_max_ms", column_B="rtt_min_ms",
                                                     or_equal=True, ignore_row_if="either_value_is_missing"), CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="valid_rtt_count", min_value=0, max_value=64), CRITICAL),
        # Cảnh báo (vẫn công bố)
        (gxe.ExpectColumnValuesToBeInSet(column="counts_consistent", value_set=[True], mostly=0.999), WARN),
        (gxe.ExpectColumnValuesToNotBeInSet(column="probe_sk", value_set=[-1], mostly=0.99), WARN),
        (gxe.ExpectColumnValuesToBeInSet(column="public_status", value_set=["public"], mostly=0.99), WARN),
        # Độ trễ âm = đồng hồ probe lệch (clock_suspect); > 3 giờ = vượt overlap.
        (gxe.ExpectColumnValuesToBeBetween(column="ingest_delay_s", min_value=0, max_value=3 * 3600, mostly=0.99), WARN),
        (gxe.ExpectColumnValuesToBeInSet(column="clock_suspect", value_set=[False], mostly=0.95), WARN),
    ]
    if expected_rows:
        exps.append((gxe.ExpectTableRowCountToBeBetween(min_value=expected_rows[0], max_value=expected_rows[1]), WARN))
    return exps


def silver_fact_reply():
    """silver.fact_ping_reply: khóa (result_id, reply_seq), loại phần tử, miền RTT."""
    import great_expectations.expectations as gxe

    known = ["reply", "duplicate", "timeout", "error"]
    return [
        (gxe.ExpectColumnValuesToNotBeNull(column="reply_id"), CRITICAL),
        (gxe.ExpectColumnValuesToNotBeNull(column="reply_seq"), CRITICAL),
        (gxe.ExpectColumnValuesToBeUnique(column="reply_id"), CRITICAL),
        (gxe.ExpectCompoundColumnsToBeUnique(column_list=["result_id", "reply_seq"]), CRITICAL),
        (gxe.ExpectColumnValuesToBeInSet(column="reply_type", value_set=known + ["unknown"]), CRITICAL),
        # Phần tử dạng lạ (unknown) không phá hợp đồng nhưng có thể là dấu hiệu nguồn đổi format.
        (gxe.ExpectColumnValuesToBeInSet(column="reply_type", value_set=known), WARN),
        # Chưa thấy RTT âm hay > 60 s trong dữ liệu thật; nếu có thì cảnh báo (is_valid_rtt đã loại RTT âm).
        (gxe.ExpectColumnValuesToBeBetween(column="rtt_ms", min_value=0, max_value=60000), WARN),
    ]


def silver_dimension(surrogate_key: str, natural_key: str):
    """silver.dim_probe / dim_measurement (SCD2): surrogate key duy nhất, (key, version) duy nhất, valid_to > valid_from."""
    import great_expectations.expectations as gxe

    return [
        (gxe.ExpectColumnValuesToBeUnique(column=surrogate_key), CRITICAL),
        (gxe.ExpectColumnValuesToNotBeNull(column=surrogate_key), CRITICAL),
        (gxe.ExpectCompoundColumnsToBeUnique(column_list=[natural_key, "version_no"]), CRITICAL),
        (gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="valid_to", column_B="valid_from"), CRITICAL),
    ]


def gold_quality_target():
    """gold.quality_hourly_target: khóa giờ × cohort × measurement và quan hệ giữa các tổng."""
    import great_expectations.expectations as gxe

    exps = [(gxe.ExpectColumnValuesToNotBeNull(column=c), CRITICAL)
            for c in ("ictrl_dt", "event_hour_utc", "event_hour_local", "probe_set_version", "measurement_id")]
    exps += [
        (gxe.ExpectCompoundColumnsToBeUnique(column_list=["probe_set_version", "measurement_id", "event_hour_utc"]), CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="packets_lost", min_value=0), CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="rtt_sum_ms", min_value=0), CRITICAL),
        (gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="packets_sent", column_B="packets_lost", or_equal=True), CRITICAL),
    ]
    return exps
