"""Hỗ trợ chung cho test chạy trong container Spark (tests/transformation, tests/quality).

- Fixture: `spark` (SparkSession local), `real_cases` / `unique_results` (result RIPE Atlas thật trong tests/fixtures).
- Builder: dựng DataFrame Bronze/Silver nhỏ từ fixture để test hàm biến đổi và kiểm tra chất lượng.
Package `lakehouse` (transformation/) và `dq` (quality/) nằm trên PYTHONPATH của image Spark.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import pytest

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "ping_results_real_sample.jsonl"
LOADED = datetime(2026, 11, 19)


# ---------------------------------------------------------------------------- fixtures
@pytest.fixture(scope="session")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    session = (pyspark_sql.SparkSession.builder.master("local[2]").appName("pytest-spark")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def real_cases() -> dict:
    """Tên trường hợp -> result thật (normal, partial loss, all loss, send error, duplicate, IPv6, ...)."""
    return {item["case"]: item["record"] for item in map(json.loads, FIXTURE.read_text(encoding="utf-8").splitlines())}


@pytest.fixture(scope="session")
def unique_results(real_cases) -> list[dict]:
    """Result khác nhau theo (msm, probe, timestamp); case normal_v4 và no_mver_old_fw_v4 là cùng một result thật."""
    seen, out = set(), []
    for record in real_cases.values():
        key = (record["msm_id"], record["prb_id"], record["timestamp"])
        if key not in seen:
            seen.add(key)
            out.append(record)
    return out


# ---------------------------------------------------------------------------- builders
def ts(text: str) -> datetime:
    return datetime.fromisoformat(text)


def probe_raw(spark, rows):
    """rows: (probe_id, observed_at, attrs | None) -> DataFrame như bronze.probe_metadata_raw (None = not_returned)."""
    data = []
    for pid, observed, attrs in rows:
        if attrs is None:
            data.append((pid, ts(observed), "not_returned", None))
        else:
            doc = {"id": pid, "is_public": True, "is_anchor": False, "asn_v6": None, "prefix_v4": None, "prefix_v6": None,
                   "geometry": {"coordinates": [106.7, 10.8]}, **attrs}
            data.append((pid, ts(observed), "ok", json.dumps(doc, sort_keys=True)))
    return spark.createDataFrame(data, "probe_id long, observed_at timestamp, fetch_status string, raw_json string")


def build_probe_dim(spark, rows):
    from lakehouse.silver import PROBE_TRACKED, build_scd2, probe_snapshots, with_unknown_member

    dim = build_scd2(probe_snapshots(probe_raw(spark, rows)), "probe_id", PROBE_TRACKED, "probe", "probe_sk")
    return with_unknown_member(spark, dim, "probe_id", "probe_sk")


def bronze_rows(spark, records, fetched="2026-11-18 14:05:00"):
    """Result thật -> DataFrame như bronze.ping_result_raw."""
    data = []
    for r in records:
        raw = json.dumps(r, separators=(",", ":"))
        sha = hashlib.sha256(raw.encode()).hexdigest()
        data.append((r["msm_id"], r["prb_id"], r["timestamp"], datetime.utcfromtimestamp(r["timestamp"]), r.get("stored_timestamp"),
                     raw, sha, ts(fetched), f"uid-{sha[:8]}", "run-1", "dev-200-v1"))
    return spark.createDataFrame(data, "measurement_id int, probe_id long, event_ts long, event_time timestamp, stored_ts long, "
                                       "raw_json string, raw_sha256 string, fetched_at timestamp, record_uid string, "
                                       "ingest_run_id string, probe_set_version string")


def facts_for(spark, records, probe_rows=None):
    """Result thật -> fact_ping_result (đã as-of join với dim_probe dựng từ probe_rows)."""
    from lakehouse.silver import asof_join, build_fact_result, select_result_versions

    probe_rows = probe_rows or [(r["prb_id"], "2026-11-01 00:00:00", {"country_code": "VN", "asn_v4": 1, "asn_v6": 2}) for r in records]
    probe_dim = build_probe_dim(spark, probe_rows)
    msm_dim = spark.createDataFrame([(m, -m, ts("1970-01-01 00:00:00"), ts("9999-12-31 00:00:00"), False) for m in (1001, 1004, 2001, 2004)],
                                    "measurement_id long, measurement_sk long, join_from timestamp, valid_to timestamp, is_unknown_member boolean")
    return asof_join(build_fact_result(select_result_versions(bronze_rows(spark, records)), "silver-1", LOADED), probe_dim, msm_dim)
