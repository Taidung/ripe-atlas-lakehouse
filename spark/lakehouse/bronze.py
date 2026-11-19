"""Biến đổi staging -> Bronze (hàm thuần trên DataFrame, test được không cần S3).

Chính sách lỗi (docs/data-contracts.md §Quarantine):
- Lỗi mức bản ghi -> quarantine, batch vẫn chạy:
  invalid_utf8, hash_mismatch, malformed_json, missing_key, wrong_measurement, af_mismatch,
  wrong_type, out_of_window (results); hash_mismatch, malformed_json, id_mismatch (metadata).
- Lỗi mức batch -> job thất bại, không ghi gì:
  object trong manifest không tồn tại, số dòng đọc được khác số dòng manifest đã stage,
  _dlt_load_id khác load_id của manifest, tỷ lệ quarantine vượt ngưỡng.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType,
)

STAGED_RESULT_SCHEMA = StructType([
    StructField("measurement_id", LongType()),
    StructField("probe_id", LongType()),
    StructField("event_ts", LongType()),
    StructField("parse_status", StringType()),
    StructField("raw_json", StringType()),
    StructField("raw_sha256", StringType()),
    StructField("line_no", LongType()),
    StructField("request_index", LongType()),
    StructField("change_type", StringType()),
    StructField("run_id", StringType()),
    StructField("fetched_at", StringType()),
    StructField("_dlt_load_id", StringType()),
    StructField("_dlt_id", StringType()),
])

STAGED_METADATA_SCHEMA = StructType([
    StructField("entity_id", LongType()),
    StructField("fetch_status", StringType()),
    StructField("raw_json", StringType()),
    StructField("raw_sha256", StringType()),
    StructField("raw_encoding", StringType()),
    StructField("is_public", BooleanType()),
    StructField("observed_at", StringType()),
    StructField("request_index", LongType()),
    StructField("run_id", StringType()),
    StructField("_dlt_load_id", StringType()),
    StructField("_dlt_id", StringType()),
])

PING_SCHEMA = StructType([
    StructField("msm_id", LongType()),
    StructField("prb_id", LongType()),
    StructField("timestamp", LongType()),
    StructField("af", LongType()),
    StructField("type", StringType()),
    StructField("stored_timestamp", LongType()),
    StructField("fw", LongType()),
    StructField("mver", StringType()),
    StructField("_corrupt", StringType()),
])

ID_SCHEMA = StructType([StructField("id", LongType()), StructField("_corrupt", StringType())])

JSON_OPTS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt"}

# Thông tin manifest gắn vào từng object (một dòng mỗi object).
MANIFEST_OBJECT_COLUMNS = [
    "source_object", "manifest_path", "expected_measurement_id", "expected_af", "window_start_ts",
    "window_end_ts", "ictrl_dt", "probe_set_version", "expected_load_id",
]


def parse_staged(lines: DataFrame, schema: StructType) -> DataFrame:
    """`lines`: cột value (một dòng jsonl của dlt) và source_object."""
    return lines.select(F.from_json("value", schema).alias("s"), "source_object").select("s.*", "source_object")


def _uid(*cols) -> F.Column:
    return F.sha2(F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]), 256)


def result_quarantine_reason(staged: DataFrame) -> DataFrame:
    p = F.col("p")
    reason = (
        F.when(F.col("parse_status") == "invalid_utf8", "invalid_utf8")
        .when(F.sha2("raw_json", 256) != F.col("raw_sha256"), "hash_mismatch")
        .when(p.isNull() | p["_corrupt"].isNotNull(), "malformed_json")
        .when(p["msm_id"].isNull() | p["prb_id"].isNull() | p["timestamp"].isNull() | p["af"].isNull(), "missing_key")
        .when(p["msm_id"] != F.col("expected_measurement_id"), "wrong_measurement")
        .when(p["af"] != F.col("expected_af"), "af_mismatch")
        .when(F.coalesce(p["type"], F.lit("")) != "ping", "wrong_type")
        .when((p["timestamp"] < F.col("window_start_ts")) | (p["timestamp"] >= F.col("window_end_ts")), "out_of_window")
    )
    return staged.withColumn("p", F.from_json("raw_json", PING_SCHEMA, JSON_OPTS)).withColumn("reason", reason)


def transform_results(staged: DataFrame, manifest_objects: DataFrame, bronze_run_id: str, loaded_at) -> tuple[DataFrame, DataFrame]:
    """Trả về (valid_rows, quarantine_rows) theo schema bảng Bronze."""
    joined = staged.join(F.broadcast(manifest_objects), "source_object", "left")
    checked = result_quarantine_reason(joined)
    p = F.col("p")
    valid = checked.filter(F.col("reason").isNull()).select(
        _uid(p["msm_id"], p["prb_id"], p["timestamp"], F.col("raw_sha256")).alias("record_uid"),
        p["msm_id"].cast("int").alias("measurement_id"),
        p["prb_id"].alias("probe_id"),
        p["af"].cast("int").alias("af"),
        p["timestamp"].alias("event_ts"),
        F.timestamp_seconds(p["timestamp"]).alias("event_time"),
        p["stored_timestamp"].alias("stored_ts"),
        F.col("ictrl_dt"),
        F.timestamp_seconds("window_start_ts").alias("window_start"),
        "raw_json", "raw_sha256", "change_type",
        p["fw"].cast("int").alias("fw"),
        p["mver"].alias("mver"),
        "source_object", "manifest_path",
        F.col("run_id").alias("ingest_run_id"),
        F.col("_dlt_load_id").alias("dlt_load_id"),
        F.to_timestamp("fetched_at").alias("fetched_at"),
        "probe_set_version",
        F.lit(bronze_run_id).alias("bronze_run_id"),
        F.lit(loaded_at).cast("timestamp").alias("bronze_loaded_at"),
    )
    # Cùng nội dung được stage nhiều lần (hai run chồng nhau): giữ bản stage sớm nhất, xác định.
    order = Window.partitionBy("record_uid").orderBy("fetched_at", "ingest_run_id", "source_object")
    valid = valid.withColumn("_rn", F.row_number().over(order)).filter("_rn = 1").drop("_rn")

    quarantine = checked.filter(F.col("reason").isNotNull()).select(
        _uid(F.lit("ping_result_raw"), F.col("manifest_path"), F.col("raw_sha256"), F.col("reason"),
             F.col("request_index"), F.col("line_no")).alias("quarantine_uid"),
        F.lit("ping_result_raw").alias("entity"),
        "reason",
        F.col("expected_measurement_id").cast("int").alias("measurement_id"),
        F.col("probe_id").alias("entity_id"),
        F.col("event_ts"),
        "raw_json", "raw_sha256", "source_object", "manifest_path",
        F.col("run_id").alias("ingest_run_id"),
        "ictrl_dt",
        F.lit(loaded_at).cast("timestamp").alias("detected_at"),
        F.lit(bronze_run_id).alias("bronze_run_id"),
        F.lit("pending").alias("reprocess_status"),
    ).dropDuplicates(["quarantine_uid"])
    return valid, quarantine


def transform_metadata(staged: DataFrame, manifest_objects: DataFrame, entity: str, id_column: str,
                       bronze_run_id: str, loaded_at) -> tuple[DataFrame, DataFrame]:
    joined = staged.join(F.broadcast(manifest_objects), "source_object", "left")
    parsed = F.from_json("raw_json", ID_SCHEMA, JSON_OPTS)
    reason = (
        F.when(F.col("entity_id").isNull(), "missing_key")
        .when(F.col("fetch_status") == "not_returned", None)
        .when(F.col("raw_json").isNull() | (F.sha2("raw_json", 256) != F.col("raw_sha256")), "hash_mismatch")
        .when(parsed.isNull() | parsed["_corrupt"].isNotNull(), "malformed_json")
        .when(parsed["id"] != F.col("entity_id"), "id_mismatch")
    )
    checked = joined.withColumn("reason", reason)
    id_type = "int" if id_column == "measurement_id" else "bigint"
    valid = checked.filter(F.col("reason").isNull()).select(
        _uid(F.col("entity_id"), F.col("run_id")).alias("record_uid"),
        F.col("entity_id").cast(id_type).alias(id_column),
        "fetch_status", "is_public", "raw_json", "raw_sha256", "raw_encoding",
        F.to_timestamp("observed_at").alias("observed_at"),
        "ictrl_dt", "source_object", "manifest_path",
        F.col("run_id").alias("ingest_run_id"),
        F.col("_dlt_load_id").alias("dlt_load_id"),
        "probe_set_version",
        F.lit(bronze_run_id).alias("bronze_run_id"),
        F.lit(loaded_at).cast("timestamp").alias("bronze_loaded_at"),
    ).dropDuplicates(["record_uid"])
    quarantine = checked.filter(F.col("reason").isNotNull()).select(
        _uid(F.lit(entity), F.col("manifest_path"), F.col("raw_sha256"), F.col("reason"), F.col("_dlt_id")).alias("quarantine_uid"),
        F.lit(entity).alias("entity"), "reason",
        F.lit(None).cast("int").alias("measurement_id"),
        F.col("entity_id"), F.lit(None).cast("bigint").alias("event_ts"),
        "raw_json", "raw_sha256", "source_object", "manifest_path",
        F.col("run_id").alias("ingest_run_id"), "ictrl_dt",
        F.lit(loaded_at).cast("timestamp").alias("detected_at"),
        F.lit(bronze_run_id).alias("bronze_run_id"),
        F.lit("pending").alias("reprocess_status"),
    ).dropDuplicates(["quarantine_uid"])
    return valid, quarantine


def batch_integrity_errors(staged: DataFrame, manifests: list[dict]) -> list[str]:
    """Kiểm tra mức batch: số dòng mỗi manifest và load_id của từng dòng."""
    errors = []
    counts = {r["source_object"]: (r["n"], r["load_ids"]) for r in staged.groupBy("source_object").agg(
        F.count("*").alias("n"), F.collect_set("_dlt_load_id").alias("load_ids")).collect()}
    for m in manifests:
        expected = m["counts"].get("staged", m["counts"].get("records", 0))
        got = 0
        for obj in m["objects"]:
            n, load_ids = counts.get(obj["path"], (0, []))
            got += n
            if n and set(load_ids) != {m["load_id"]}:
                errors.append(f"{obj['path']}: _dlt_load_id {sorted(load_ids)} != manifest load_id {m['load_id']}")
        if got != expected:
            errors.append(f"{m['manifest_path']}: read {got} rows, manifest says {expected}")
    return errors
