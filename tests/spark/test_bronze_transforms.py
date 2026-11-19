import hashlib
import json
from datetime import date, datetime, timezone

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.bronze import (
    STAGED_METADATA_SCHEMA,
    STAGED_RESULT_SCHEMA,
    batch_integrity_errors,
    transform_metadata,
    transform_results,
)
from lakehouse.common import split_sql

pytestmark = pytest.mark.spark

W_START, W_END = 1795006800, 1795010400  # 2026-11-18 13:00-14:00 UTC
OBJ = "s3://staging/ripe_atlas/ping_result_raw/window_date=2026-11-18/window_hour=20/msm_id=1001/L1.a.jsonl.gz"
MANIFEST = "s3://staging/ripe_atlas/_manifests/ping_result_raw/x/run-1.json"
LOADED_AT = datetime(2026, 11, 19, tzinfo=timezone.utc)


def with_source(schema):
    # StructType.add() sửa schema tại chỗ; tạo bản sao để không làm bẩn hằng số dùng chung.
    return StructType(list(schema.fields) + [StructField("source_object", StringType())])


def line(record):
    return json.dumps(record, separators=(",", ":"))


def staged_row(raw, run_id="run-1", fetched_at="2026-11-18T14:05:00+00:00", sha=None, parse_status="ok", source=OBJ, line_no=0):
    return {
        "measurement_id": 1001, "probe_id": None, "event_ts": None, "parse_status": parse_status,
        "raw_json": raw, "raw_sha256": sha or hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "line_no": line_no, "request_index": 0, "change_type": "new", "run_id": run_id,
        "fetched_at": fetched_at, "_dlt_load_id": "L1", "_dlt_id": f"d{line_no}", "source_object": source,
    }


def frames(spark, rows):
    staged = spark.createDataFrame([tuple(r[f.name] for f in STAGED_RESULT_SCHEMA.fields) + (r["source_object"],) for r in rows],
                                   with_source(STAGED_RESULT_SCHEMA))
    objects = spark.createDataFrame(
        [(OBJ, MANIFEST, 1001, 4, W_START, W_END, date(2026, 11, 18), "dev-200-v1", "L1")],
        "source_object string, manifest_path string, expected_measurement_id long, expected_af long, window_start_ts long, "
        "window_end_ts long, ictrl_dt date, probe_set_version string, expected_load_id string")
    return staged, objects


def test_real_results_are_valid_and_keep_raw(spark, unique_results):
    records = [r for r in unique_results if r["msm_id"] == 1001]
    staged, objects = frames(spark, [staged_row(line(r), line_no=i) for i, r in enumerate(records)])
    valid, quarantine = transform_results(staged, objects, "bronze-1", LOADED_AT)
    rows = {r.probe_id: r for r in valid.collect()}
    assert quarantine.count() == 0 and len(rows) == len(records)
    for record in records:
        row = rows[record["prb_id"]]
        assert row.raw_json == line(record) and row.af == 4 and row.ictrl_dt == date(2026, 11, 18)
        assert row.event_time == datetime.fromtimestamp(record["timestamp"], timezone.utc).replace(tzinfo=None)
        expected_uid = hashlib.sha256(f"1001|{record['prb_id']}|{record['timestamp']}|{row.raw_sha256}".encode()).hexdigest()
        assert row.record_uid == expected_uid and row.stored_ts == record["stored_timestamp"]


@pytest.mark.parametrize(
    "raw, kwargs, reason",
    [
        ('{"msm_id":1001,"prb_id":', {}, "malformed_json"),
        ('{"msm_id":1001,"timestamp":1795006800,"af":4,"type":"ping"}', {}, "missing_key"),
        ('{"msm_id":1001,"prb_id":"12","timestamp":1795006800,"af":4,"type":"ping"}', {}, "malformed_json"),
        ('{"msm_id":1004,"prb_id":1,"timestamp":1795006800,"af":4,"type":"ping"}', {}, "wrong_measurement"),
        ('{"msm_id":1001,"prb_id":1,"timestamp":1795006800,"af":6,"type":"ping"}', {}, "af_mismatch"),
        ('{"msm_id":1001,"prb_id":1,"timestamp":1795006800,"af":4,"type":"traceroute"}', {}, "wrong_type"),
        ('{"msm_id":1001,"prb_id":1,"timestamp":1795010400,"af":4,"type":"ping"}', {}, "out_of_window"),
        ('{"msm_id":1001,"prb_id":1,"timestamp":1795006800,"af":4,"type":"ping"}', {"sha": "0" * 64}, "hash_mismatch"),
        ('{"x":"�"}', {"parse_status": "invalid_utf8"}, "invalid_utf8"),
    ],
)
def test_bad_records_are_quarantined_with_reason(spark, raw, kwargs, reason):
    staged, objects = frames(spark, [staged_row(raw, **kwargs)])
    valid, quarantine = transform_results(staged, objects, "bronze-1", LOADED_AT)
    rows = quarantine.collect()
    assert valid.count() == 0 and [r.reason for r in rows] == [reason]
    assert rows[0].manifest_path == MANIFEST and rows[0].reprocess_status == "pending"


def test_same_content_staged_twice_keeps_earliest_copy(spark, unique_results):
    raw = line(next(r for r in unique_results if r["msm_id"] == 1001))
    staged, objects = frames(spark, [
        staged_row(raw, run_id="run-2", fetched_at="2026-11-18T15:05:00+00:00"),
        staged_row(raw, run_id="run-1", fetched_at="2026-11-18T14:05:00+00:00", line_no=1),
    ])
    valid, _ = transform_results(staged, objects, "bronze-1", LOADED_AT)
    assert [r.ingest_run_id for r in valid.collect()] == ["run-1"]


def test_same_key_different_payload_yields_two_versions(spark, unique_results):
    record = next(r for r in unique_results if r["msm_id"] == 1001)
    corrected = dict(record, avg=999.0)
    staged, objects = frames(spark, [staged_row(line(record)), staged_row(line(corrected), line_no=1)])
    valid, _ = transform_results(staged, objects, "bronze-1", LOADED_AT)
    assert valid.select("record_uid").distinct().count() == 2
    assert valid.select("probe_id", "event_ts").distinct().count() == 1


def test_metadata_not_returned_kept_and_id_mismatch_quarantined(spark):
    probe = json.dumps({"id": 7, "is_public": True}, sort_keys=True, separators=(",", ":"))
    wrong = json.dumps({"id": 8}, separators=(",", ":"))
    src = "s3://staging/ripe_atlas/probe_metadata_raw/ictrl_dt=2026-11-19/L2.a.jsonl.gz"
    rows = [
        (7, "ok", probe, hashlib.sha256(probe.encode()).hexdigest(), "canonical_json", True, "2026-11-19T02:00:00+00:00", 0, "run-1", "L2", "a", src),
        (9, "not_returned", None, None, "none", None, "2026-11-19T02:00:00+00:00", -1, "run-1", "L2", "b", src),
        (10, "ok", wrong, hashlib.sha256(wrong.encode()).hexdigest(), "canonical_json", None, "2026-11-19T02:00:00+00:00", 0, "run-1", "L2", "c", src),
    ]
    staged = spark.createDataFrame(rows, with_source(STAGED_METADATA_SCHEMA))
    objects = spark.createDataFrame([(src, "m.json", None, None, None, None, date(2026, 11, 19), "dev-200-v1", "L2")],
                                    "source_object string, manifest_path string, expected_measurement_id long, expected_af long, "
                                    "window_start_ts long, window_end_ts long, ictrl_dt date, probe_set_version string, expected_load_id string")
    valid, quarantine = transform_metadata(staged, objects, "probe_metadata_raw", "probe_id", "bronze-1", LOADED_AT)
    assert sorted((r.probe_id, r.fetch_status) for r in valid.collect()) == [(7, "ok"), (9, "not_returned")]
    assert [(r.entity_id, r.reason) for r in quarantine.collect()] == [(10, "id_mismatch")]


def test_batch_integrity_detects_count_and_load_id_mismatch(spark, unique_results):
    raw = line(unique_results[0])
    staged, _ = frames(spark, [staged_row(raw)])
    manifest = {"manifest_path": MANIFEST, "load_id": "L1", "counts": {"staged": 1}, "objects": [{"path": OBJ}]}
    assert batch_integrity_errors(staged, [manifest]) == []
    assert "read 1 rows, manifest says 2" in batch_integrity_errors(staged, [dict(manifest, counts={"staged": 2})])[0]
    assert "_dlt_load_id" in batch_integrity_errors(staged, [dict(manifest, load_id="OTHER")])[0]


def test_bronze_ddl_splits_into_statements():
    from pathlib import Path

    text = (Path(__file__).resolve().parents[2] / "sql" / "bronze" / "001_tables.sql").read_text(encoding="utf-8")
    statements = split_sql(text)
    assert statements[0].startswith("CREATE NAMESPACE") and len(statements) == 7
    assert all(";" not in s.strip()[-1:] for s in statements)
