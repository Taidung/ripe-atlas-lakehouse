import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None

sys.path.insert(0, str(Path(__file__).resolve().parent))  # job legacy nằm cùng thư mục

if SparkSession is not None:
    from staging_to_bronze import prepare_metadata, prepare_ping_results, validate_metadata


@unittest.skipUnless(SparkSession is not None, "Run with PySpark in the Spark container")
class BronzeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("test_bronze").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def raw_rows(self, items):
        return self.spark.createDataFrame(items, ["value", "source_file"])

    def test_metadata_keeps_payload_and_ingest_date(self):
        payload = json.dumps({"id": 123, "country_code": "VN"})
        path = "s3a://staging/ripe_atlas/probes/ingest_date=2026-11-18/load.jsonl.gz"
        rows = prepare_metadata(self.raw_rows([(payload, path)]))
        validate_metadata(rows, "probes")
        row = rows.first()
        self.assertEqual((row.id, row.ingest_date, row.payload, row.source_file), (123, "2026-11-18", payload, path))

    def test_metadata_rejects_missing_id(self):
        path = "s3a://staging/ripe_atlas/probes/ingest_date=2026-11-18/load.jsonl.gz"
        rows = prepare_metadata(self.raw_rows([('{"country_code":"VN"}', path)]))
        with self.assertRaisesRegex(ValueError, "Invalid or empty staging data"):
            validate_metadata(rows, "probes")

    def test_ping_hour_and_invalid_results(self):
        start = datetime(2026, 11, 18, 13, tzinfo=timezone(timedelta(hours=7)))
        first = int(start.timestamp())
        path = "s3a://staging/ripe_atlas/ping_results/window_date=2026-11-18/window_hour=13/load.jsonl.gz"
        payload = json.dumps({"msm_id": 1001, "prb_id": 19967, "timestamp": first, "type": "ping"})
        rows = prepare_ping_results(self.raw_rows([(payload, path)]), start)
        row = rows.first()
        self.assertEqual((row.msm_id, row.prb_id, row.timestamp, row.window_date, row.window_hour), (1001, 19967, first, "2026-11-18", "13"))
        self.assertEqual((row.payload, row.source_file), (payload, path))

        for result in (
            {"msm_id": 1001, "prb_id": 19967, "timestamp": first + 3600, "type": "ping"},
            {"msm_id": 1001, "prb_id": 19967, "timestamp": first},
        ):
            with self.subTest(result=result), self.assertRaisesRegex(ValueError, "Invalid or empty ping results"):
                prepare_ping_results(self.raw_rows([(json.dumps(result), path)]), start)


if __name__ == "__main__":
    unittest.main()
