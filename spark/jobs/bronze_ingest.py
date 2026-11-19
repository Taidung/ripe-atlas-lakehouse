"""Staging -> Bronze, chỉ đọc object thuộc manifest status=complete.

  spark-submit /opt/app/spark/jobs/bronze_ingest.py --entity ping_result_raw --start 2026-11-19T00:00:00Z --end 2026-11-19T03:00:00Z
  spark-submit /opt/app/spark/jobs/bronze_ingest.py --entity metadata

Idempotency và phục hồi:
- Manifest đã có trong bronze.ingest_manifest (bronze_status=loaded) bị bỏ qua, trừ khi --reprocess.
- Dữ liệu MERGE theo record_uid (WHEN NOT MATCHED INSERT): xử lý lại cùng manifest không sinh dòng mới.
- Thứ tự commit: (1) bảng dữ liệu, (2) quarantine, (3) ingest_manifest. Mỗi bước là một commit
  Iceberg nguyên tử trên MỘT bảng. Nếu job chết giữa (1) và (3), manifest chưa được đánh dấu nên lần
  sau xử lý lại; MERGE theo khóa bảo đảm không nhân bản.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.bronze import (
    STAGED_METADATA_SCHEMA,
    STAGED_RESULT_SCHEMA,
    batch_integrity_errors,
    parse_staged,
    transform_metadata,
    transform_results,
)
from lakehouse.common import SQL_DIR, STAGING_ROOT, glob_paths, log_event, new_run_id, path_exists, run_sql_file, to_s3, to_s3a
from lakehouse.metrics import job_metrics

REPORT_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
METADATA_ENTITIES = {
    "probe_metadata_raw": "probe_id",
    "measurement_metadata_raw": "measurement_id",
}


def parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include an offset")
    return parsed.astimezone(timezone.utc)


def result_manifest_globs(start: datetime, end: datetime) -> list[str]:
    globs, cursor = [], start
    while cursor < end:
        local = cursor.astimezone(REPORT_TZ)
        globs.append(f"{STAGING_ROOT}/_manifests/ping_result_raw/window_date={local:%Y-%m-%d}/window_hour={local:%H}/msm_id=*/*.json")
        cursor += timedelta(hours=1)
    return globs


def read_manifests(spark: SparkSession, globs: list[str]) -> list[dict]:
    paths = [p for g in globs for p in glob_paths(spark, g)]
    if not paths:
        return []
    manifests = []
    for path, text in spark.sparkContext.wholeTextFiles(",".join(paths)).collect():
        doc = json.loads(text)
        if doc.get("status") != "complete":
            continue
        doc["manifest_path"] = to_s3(path)
        doc["_raw"] = text
        manifests.append(doc)
    return sorted(manifests, key=lambda m: m["manifest_path"])


def pending(spark: SparkSession, manifests: list[dict], reprocess: bool) -> list[dict]:
    if reprocess or not manifests:
        return manifests
    loaded = {r.manifest_path for r in spark.table("nessie.bronze.ingest_manifest")
              .filter(F.col("bronze_status") == "loaded")
              .filter(F.col("manifest_path").isin([m["manifest_path"] for m in manifests]))
              .select("manifest_path").collect()}
    return [m for m in manifests if m["manifest_path"] not in loaded]


def manifest_objects_df(spark: SparkSession, manifests: list[dict], entity: str):
    rows = []
    for m in manifests:
        window_start = parse_instant(m["window_start"]) if m.get("window_start") else None
        window_end = parse_instant(m["window_end"]) if m.get("window_end") else None
        for obj in m["objects"]:
            rows.append((
                obj["path"], m["manifest_path"], m.get("measurement_id"), m.get("af"),
                int(window_start.timestamp()) if window_start else None,
                int(window_end.timestamp()) if window_end else None,
                m["ictrl_dt"], m.get("probe_set_version") or "unknown", m.get("load_id"),
            ))
    schema = ("source_object string, manifest_path string, expected_measurement_id long, expected_af long, "
              "window_start_ts long, window_end_ts long, ictrl_dt string, probe_set_version string, expected_load_id string")
    return spark.createDataFrame(rows, schema).withColumn("ictrl_dt", F.to_date("ictrl_dt"))


def read_objects(spark: SparkSession, manifests: list[dict]):
    paths = [obj["path"] for m in manifests for obj in m["objects"]]
    missing = [p for p in paths if not path_exists(spark, p)]
    if missing:
        raise RuntimeError(f"Objects listed in manifests are missing: {missing[:5]} (+{max(0, len(missing) - 5)})")
    return spark.read.text([to_s3a(p) for p in paths]).withColumn(
        "source_object", F.regexp_replace(F.input_file_name(), "^s3a://", "s3://"))


def merge_insert(spark: SparkSession, source, table: str, key: str, partition_cols: list[str]) -> None:
    view = f"src_{table.split('.')[-1]}"
    source.createOrReplaceTempView(view)
    on = " AND ".join([f"t.{key} = s.{key}"] + [f"t.{c} = s.{c}" for c in partition_cols])
    spark.sql(f"MERGE INTO {table} t USING {view} s ON {on} WHEN NOT MATCHED THEN INSERT *")


def manifest_rows(spark: SparkSession, manifests: list[dict], stats: dict[str, dict], bronze_run_id: str, loaded_at):
    rows = []
    for m in manifests:
        c, s = m["counts"], stats.get(m["manifest_path"], {})
        rows.append({
            "manifest_path": m["manifest_path"], "entity": m["entity"], "ingest_run_id": m["run_id"],
            "run_type": m.get("run_type"), "load_id": m.get("load_id"), "measurement_id": m.get("measurement_id"),
            "window_start": parse_instant(m["window_start"]) if m.get("window_start") else None,
            "window_end": parse_instant(m["window_end"]) if m.get("window_end") else None,
            "ictrl_dt": datetime.fromisoformat(m["ictrl_dt"]).date(),
            "probe_set_version": m.get("probe_set_version"), "probes_requested": m.get("probes_requested"),
            "request_count": len(m.get("requests", [])),
            "fetched": c.get("fetched", c.get("records")), "staged": c.get("staged", c.get("records")),
            "unchanged": c.get("unchanged"), "changed": c.get("changed"),
            "object_count": len(m["objects"]), "object_bytes": sum(o["bytes"] for o in m["objects"]),
            "rows_read": s.get("read", 0), "rows_valid": s.get("valid", 0), "rows_inserted": s.get("inserted", 0),
            "rows_already_present": s.get("already", 0), "rows_quarantined": s.get("quarantined", 0),
            "ingest_started_at": parse_instant(m["started_at"]), "ingest_finished_at": parse_instant(m["finished_at"]),
            "bronze_status": "loaded", "bronze_run_id": bronze_run_id, "bronze_loaded_at": loaded_at,
            "manifest_json": m["_raw"],
        })
    target = spark.table("nessie.bronze.ingest_manifest").schema
    return spark.createDataFrame([tuple(r[f.name] for f in target.fields) for r in rows], target)


def process(spark: SparkSession, entity: str, manifests: list[dict], max_quarantine_ratio: float) -> dict:
    bronze_run_id = new_run_id("bronze")
    loaded_at = datetime.now(timezone.utc)
    with_objects = [m for m in manifests if m["objects"]]
    is_results = entity == "ping_result_raw"
    table = f"nessie.bronze.{entity}"
    stats: dict[str, dict] = {m["manifest_path"]: {} for m in manifests}

    if with_objects:
        schema = STAGED_RESULT_SCHEMA if is_results else STAGED_METADATA_SCHEMA
        staged = parse_staged(read_objects(spark, with_objects), schema).cache()
        errors = batch_integrity_errors(staged, with_objects)
        if errors:
            raise RuntimeError("Batch integrity failed: " + "; ".join(errors[:5]))
        objects = manifest_objects_df(spark, with_objects, entity)
        if is_results:
            valid, quarantine = transform_results(staged, objects, bronze_run_id, loaded_at)
            partitions = ["ictrl_dt", "measurement_id"]
        else:
            valid, quarantine = transform_metadata(staged, objects, entity, METADATA_ENTITIES[entity], bronze_run_id, loaded_at)
            partitions = ["ictrl_dt"]
        valid, quarantine = valid.cache(), quarantine.cache()

        total = staged.count()
        n_quarantine = quarantine.count()
        ratio = n_quarantine / total if total else 0.0
        if ratio > max_quarantine_ratio:
            raise RuntimeError(f"Quarantine ratio {ratio:.4f} > {max_quarantine_ratio} ({n_quarantine}/{total}); nothing written")

        existing = spark.table(table).select("record_uid", *partitions)
        new_rows = valid.join(existing, ["record_uid", *partitions], "left_anti")
        per_manifest = {
            r["manifest_path"]: r for r in valid.groupBy("manifest_path").agg(F.count("*").alias("valid")).collect()
        }
        inserted = {r["manifest_path"]: r["n"] for r in new_rows.groupBy("manifest_path").agg(F.count("*").alias("n")).collect()}
        read = {r["source_object"]: r["n"] for r in staged.groupBy("source_object").agg(F.count("*").alias("n")).collect()}
        quarantined = {r["manifest_path"]: r["n"] for r in quarantine.groupBy("manifest_path").agg(F.count("*").alias("n")).collect()}
        for m in with_objects:
            v = per_manifest.get(m["manifest_path"], {"valid": 0})["valid"]
            ins = inserted.get(m["manifest_path"], 0)
            stats[m["manifest_path"]] = {
                "read": sum(read.get(o["path"], 0) for o in m["objects"]), "valid": v, "inserted": ins,
                "already": v - ins, "quarantined": quarantined.get(m["manifest_path"], 0),
            }

        merge_insert(spark, valid, table, "record_uid", partitions)
        if n_quarantine:
            merge_insert(spark, quarantine, "nessie.bronze.quarantine", "quarantine_uid", ["entity", "ictrl_dt"])

    # Audit append-only: mỗi lần xử lý một manifest là một dòng (khóa manifest_path + bronze_run_id),
    # nên --reprocess không ghi đè lịch sử của lần nạp đầu. MERGE thay vì append để job chết
    # rồi chạy lại cùng bronze_run_id không thể xảy ra trùng (run id mới mỗi lần).
    rows = manifest_rows(spark, manifests, stats, bronze_run_id, loaded_at)
    rows.createOrReplaceTempView("src_manifest")
    spark.sql("""
        MERGE INTO nessie.bronze.ingest_manifest t USING src_manifest s
        ON t.manifest_path = s.manifest_path AND t.bronze_run_id = s.bronze_run_id
           AND t.entity = s.entity AND t.ictrl_dt = s.ictrl_dt
        WHEN NOT MATCHED THEN INSERT *
    """)
    summary = {
        "entity": entity, "bronze_run_id": bronze_run_id, "manifests": len(manifests),
        "rows_read": sum(s.get("read", 0) for s in stats.values()),
        "rows_inserted": sum(s.get("inserted", 0) for s in stats.values()),
        "rows_already_present": sum(s.get("already", 0) for s in stats.values()),
        "rows_quarantined": sum(s.get("quarantined", 0) for s in stats.values()),
        "snapshot_id": spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0],
    }
    log_event("bronze", "batch_loaded", **summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity", required=True, choices=["ping_result_raw", "metadata", *METADATA_ENTITIES])
    parser.add_argument("--start", type=parse_instant)
    parser.add_argument("--end", type=parse_instant)
    parser.add_argument("--reprocess", action="store_true", help="Xử lý lại cả manifest đã loaded (vẫn idempotent)")
    parser.add_argument("--max-quarantine-ratio", type=float, default=0.05)
    args = parser.parse_args()
    if args.entity == "ping_result_raw" and not (args.start and args.end):
        parser.error("--start and --end are required for ping_result_raw")

    spark = SparkSession.builder.appName(f"bronze_ingest_{args.entity}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    stage = "bronze_results" if args.entity == "ping_result_raw" else "bronze_metadata"
    with job_metrics(stage) as m:
        try:
            run_sql_file(spark, SQL_DIR / "bronze" / "001_tables.sql")
            entities = list(METADATA_ENTITIES) if args.entity == "metadata" else [args.entity]
            for entity in entities:
                if entity == "ping_result_raw":
                    globs = result_manifest_globs(args.start, args.end)
                else:
                    globs = [f"{STAGING_ROOT}/_manifests/{entity}/ictrl_dt=*/*.json"]
                todo = pending(spark, read_manifests(spark, globs), args.reprocess)
                summary = process(spark, entity, todo, args.max_quarantine_ratio) if todo else {}
                if not todo:
                    log_event("bronze", "nothing_pending", entity=entity)
                m.set("ripe_bronze_manifests", "Manifest được xử lý, lần chạy gần nhất", len(todo), entity=entity)
                for kind in ("rows_read", "rows_inserted", "rows_already_present", "rows_quarantined"):
                    m.set("ripe_bronze_rows", "Số dòng Bronze theo loại, lần chạy gần nhất", summary.get(kind, 0),
                          entity=entity, kind=kind.removeprefix("rows_"))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
