"""Kiểm tra chất lượng Silver: GX Core (expectation suite + checkpoint) và kiểm tra Spark bổ sung.

Mỗi kiểm tra trả về dict thống nhất:
  {check_name, engine, severity, success, observed_value, unexpected_count, element_count, details}

Mức độ:
- critical: vi phạm hợp đồng dữ liệu (khóa trùng, null ở cột bắt buộc, rcvd > sent, SCD2 chồng lấn,
  lệch đối soát Bronze/Silver). Có lỗi critical thì không ghi partition.
- warn: bất thường cần xem nhưng vẫn công bố được (tỷ lệ unknown dimension, volume lệch kỳ vọng).

Kết quả đo mạng xấu (mất gói, all_lost) KHÔNG phải lỗi chất lượng dữ liệu.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

GX_ROOT = Path("/opt/app/artifacts/gx")
CRITICAL, WARN = "critical", "warn"


def _check(name, engine, severity, success, observed=None, unexpected=None, elements=None, details=None) -> dict[str, Any]:
    return {
        "check_name": name, "engine": engine, "severity": severity, "success": bool(success),
        "observed_value": None if observed is None else json.dumps(observed, default=str),
        "unexpected_count": None if unexpected is None else int(unexpected),
        "element_count": None if elements is None else int(elements),
        "details": None if details is None else json.dumps(details, default=str)[:4000],
    }


# ---------------------------------------------------------------------------- GX suites
def fact_result_expectations(expected_rows: tuple[int, int] | None = None):
    import great_expectations.expectations as gxe

    exps = [
        (gxe.ExpectColumnValuesToNotBeNull(column=c), CRITICAL)
        for c in ("result_id", "measurement_id", "af", "probe_id", "event_ts", "event_time", "ictrl_dt",
                  "date_key", "time_key", "probe_sk", "measurement_sk", "result_status", "record_uid")
    ]
    exps += [
        (gxe.ExpectColumnValuesToBeUnique(column="result_id"), CRITICAL),
        (gxe.ExpectCompoundColumnsToBeUnique(column_list=["measurement_id", "probe_id", "event_ts"]), CRITICAL),
        (gxe.ExpectColumnValuesToBeInSet(column="measurement_id", value_set=[1001, 1004, 2001, 2004]), CRITICAL),
        (gxe.ExpectColumnValuesToBeInSet(column="af", value_set=[4, 6]), CRITICAL),
        (gxe.ExpectColumnPairValuesToBeInSet(column_A="measurement_id", column_B="af",
                                             value_pairs_set=[(1001, 4), (1004, 4), (2001, 6), (2004, 6)]), CRITICAL),
        (gxe.ExpectColumnValuesToBeInSet(column="result_status",
                                         value_set=["all_received", "partial_loss", "all_lost", "send_error"]), CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="packets_sent", min_value=0, max_value=64), CRITICAL),
        (gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="packets_sent", column_B="packets_received",
                                                     or_equal=True, ignore_row_if="either_value_is_missing"), CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="rtt_min_ms", min_value=0), CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="rtt_avg_ms", min_value=0), CRITICAL),
        (gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="rtt_max_ms", column_B="rtt_min_ms",
                                                     or_equal=True, ignore_row_if="either_value_is_missing"), CRITICAL),
        (gxe.ExpectColumnValuesToBeBetween(column="valid_rtt_count", min_value=0, max_value=64), CRITICAL),
        (gxe.ExpectColumnValuesToBeInSet(column="counts_consistent", value_set=[True], mostly=0.999), WARN),
        (gxe.ExpectColumnValuesToNotBeInSet(column="probe_sk", value_set=[-1], mostly=0.99), WARN),
        (gxe.ExpectColumnValuesToBeInSet(column="public_status", value_set=["public"], mostly=0.99), WARN),
        # Độ trễ âm = đồng hồ probe lệch (clock_suspect); > 3 giờ = vượt overlap. Cả hai là cảnh báo.
        (gxe.ExpectColumnValuesToBeBetween(column="ingest_delay_s", min_value=0, max_value=3 * 3600, mostly=0.99), WARN),
        (gxe.ExpectColumnValuesToBeInSet(column="clock_suspect", value_set=[False], mostly=0.95), WARN),
    ]
    if expected_rows:
        exps.append((gxe.ExpectTableRowCountToBeBetween(min_value=expected_rows[0], max_value=expected_rows[1]), WARN))
    return exps


def fact_reply_expectations():
    import great_expectations.expectations as gxe

    known = ["reply", "duplicate", "timeout", "error"]
    return [
        (gxe.ExpectColumnValuesToNotBeNull(column="reply_id"), CRITICAL),
        (gxe.ExpectColumnValuesToNotBeNull(column="reply_seq"), CRITICAL),
        (gxe.ExpectColumnValuesToBeUnique(column="reply_id"), CRITICAL),
        (gxe.ExpectCompoundColumnsToBeUnique(column_list=["result_id", "reply_seq"]), CRITICAL),
        (gxe.ExpectColumnValuesToBeInSet(column="reply_type", value_set=known + ["unknown"]), CRITICAL),
        # Phần tử dạng lạ (unknown) không phá hợp đồng nhưng cần xem: có thể nguồn đổi format.
        (gxe.ExpectColumnValuesToBeInSet(column="reply_type", value_set=known), WARN),
        # Chưa thấy RTT âm hay > 60 s trong dữ liệu thật; nếu có thì cảnh báo (is_valid_rtt đã loại RTT âm).
        (gxe.ExpectColumnValuesToBeBetween(column="rtt_ms", min_value=0, max_value=60000), WARN),
    ]


def dim_expectations(sk: str, key: str):
    import great_expectations.expectations as gxe

    return [
        (gxe.ExpectColumnValuesToBeUnique(column=sk), CRITICAL),
        (gxe.ExpectColumnValuesToNotBeNull(column=sk), CRITICAL),
        (gxe.ExpectCompoundColumnsToBeUnique(column_list=[key, "version_no"]), CRITICAL),
        (gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="valid_to", column_B="valid_from"), CRITICAL),
    ]


def run_gx(df: DataFrame, suite_name: str, expectations, run_label: str, build_docs: bool = True) -> list[dict[str, Any]]:
    """Chạy suite qua GX Checkpoint (file context: lưu validation result + Data Docs vào artifacts/gx)."""
    import great_expectations as gx
    from great_expectations.checkpoint import UpdateDataDocsAction

    GX_ROOT.mkdir(parents=True, exist_ok=True)
    context = gx.get_context(mode="file", project_root_dir=str(GX_ROOT))
    source = context.data_sources.add_or_update_spark(name="silver_spark")
    try:
        asset = source.get_asset(suite_name)
    except LookupError:
        asset = source.add_dataframe_asset(name=suite_name)
    try:
        batch_def = asset.get_batch_definition("whole")
    except (KeyError, LookupError):
        batch_def = asset.add_batch_definition_whole_dataframe("whole")
    suite = gx.ExpectationSuite(name=suite_name)
    for exp, severity in expectations:
        exp.meta = {"severity": severity}
        suite.add_expectation(exp)
    suite = context.suites.add_or_update(suite)
    validation = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=f"{suite_name}_vd", data=batch_def, suite=suite))
    actions = [UpdateDataDocsAction(name="update_docs")] if build_docs else []
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(name=f"{suite_name}_checkpoint", validation_definitions=[validation], actions=actions,
                      result_format={"result_format": "SUMMARY"}))
    result = checkpoint.run(batch_parameters={"dataframe": df}, run_id=gx.RunIdentifier(run_name=run_label))

    checks = []
    for vr in result.run_results.values():
        for r in vr.results:
            cfg = r.expectation_config
            res = r.result or {}
            kwargs = {k: v for k, v in cfg.kwargs.items() if k not in ("batch_id",)}
            checks.append(_check(
                name=f"{cfg.type}:{json.dumps(kwargs, sort_keys=True, default=str)[:180]}",
                engine=f"gx-{gx.__version__}", severity=(cfg.meta or {}).get("severity", CRITICAL),
                success=r.success, observed=res.get("observed_value"),
                unexpected=res.get("unexpected_count"), elements=res.get("element_count"),
                details={"partial_unexpected_list": res.get("partial_unexpected_list", [])[:10],
                         "exception": (r.exception_info or {}).get("exception_message")},
            ))
    return checks


# ---------------------------------------------------------------------------- Spark checks
def scd2_checks(dim: DataFrame, key: str, name: str) -> list[dict[str, Any]]:
    real = dim.filter(~F.col("is_unknown_member"))
    a, b = real.alias("a"), real.alias("b")
    overlaps = (a.join(b, (F.col(f"a.{key}") == F.col(f"b.{key}")) & (F.col("a.version_no") < F.col("b.version_no"))
                       & (F.col("a.valid_from") < F.col("b.valid_to")) & (F.col("b.valid_from") < F.col("a.valid_to")))
                .count())
    current = real.groupBy(key).agg(F.sum(F.col("is_current").cast("int")).alias("n")).filter("n != 1").count()
    gaps = (real.alias("x").join(real.alias("y"), (F.col(f"x.{key}") == F.col(f"y.{key}"))
                                 & (F.col("y.version_no") == F.col("x.version_no") + 1), "inner")
            .filter(F.col("x.valid_to") != F.col("y.valid_from")).count())
    unknown = dim.filter(F.col("is_unknown_member")).count()
    return [
        _check(f"{name}:scd2_no_overlap", "spark", CRITICAL, overlaps == 0, unexpected=overlaps),
        _check(f"{name}:scd2_one_current_per_key", "spark", CRITICAL, current == 0, unexpected=current),
        _check(f"{name}:scd2_contiguous_versions", "spark", CRITICAL, gaps == 0, unexpected=gaps),
        _check(f"{name}:unknown_member_present", "spark", CRITICAL, unknown == 1, observed=unknown),
    ]


def reconciliation_checks(bronze: DataFrame, facts: DataFrame, replies: DataFrame) -> list[dict[str, Any]]:
    """Bronze (khóa phân biệt) = Silver result; số reply = tổng số phần tử; reply hợp lệ = valid_rtt_count."""
    bronze_keys = bronze.select("measurement_id", "probe_id", "event_ts").distinct().count()
    fact_rows = facts.count()
    agg = facts.agg(F.sum("reply_element_count").alias("elements"), F.sum("valid_rtt_count").alias("valid")).first()
    rep = replies.agg(F.count("*").alias("rows"), F.sum(F.col("is_valid_rtt").cast("int")).alias("valid")).first()
    orphan = replies.join(facts.select("result_id"), "result_id", "left_anti").count()
    elements, valid = agg["elements"] or 0, agg["valid"] or 0
    return [
        _check("reconcile:bronze_keys_eq_fact_rows", "spark", CRITICAL, bronze_keys == fact_rows,
               observed={"bronze_distinct_keys": bronze_keys, "fact_rows": fact_rows}),
        _check("reconcile:reply_rows_eq_elements", "spark", CRITICAL, rep["rows"] == elements,
               observed={"reply_rows": rep["rows"], "result_elements": elements}),
        _check("reconcile:valid_replies_eq_valid_rtt_count", "spark", CRITICAL, (rep["valid"] or 0) == valid,
               observed={"reply_valid": rep["valid"], "fact_valid_rtt_count": valid}),
        _check("reconcile:no_orphan_replies", "spark", CRITICAL, orphan == 0, unexpected=orphan),
    ]


def has_critical_failure(checks: list[dict[str, Any]]) -> bool:
    return any(c["severity"] == CRITICAL and not c["success"] for c in checks)


def has_warning(checks: list[dict[str, Any]]) -> bool:
    return any(c["severity"] == WARN and not c["success"] for c in checks)
