from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "spark"))

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]").appName("pytest-spark")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def real_cases():
    cases = {}
    for line in (ROOT / "tests" / "fixtures" / "ping_results_real_sample.jsonl").read_text(encoding="utf-8").splitlines():
        item = json.loads(line)
        cases[item["case"]] = item["record"]
    return cases


@pytest.fixture(scope="session")
def unique_results():
    seen, out = set(), []
    for line in (ROOT / "tests" / "fixtures" / "ping_results_real_sample.jsonl").read_text(encoding="utf-8").splitlines():
        record = json.loads(line)["record"]
        key = (record["msm_id"], record["prb_id"], record["timestamp"])
        if key not in seen:
            seen.add(key)
            out.append(record)
    return out


def pytest_configure(config):
    config.addinivalue_line("markers", "spark: chạy trong container Spark")
