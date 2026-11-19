import json
import sys
import unittest
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None

sys.path.insert(0, str(Path(__file__).resolve().parent))  # job legacy nằm cùng thư mục

if SparkSession is not None:
    from bronze_to_silver import measurement_dimension, ping_fact, probe_dimension


@unittest.skipUnless(SparkSession is not None, "Run with PySpark in the Spark container")
class SilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("test_silver").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_dimensions_keep_latest_snapshot(self):
        records = [
            (1, json.dumps({"_dlt_load_id": "1.0", "target": "old", "geometry": {"coordinates": [10, 20]}}), "2026-11-17", "old"),
            (1, json.dumps({"_dlt_load_id": "2.0", "target": "new", "geometry": {"coordinates": [30, 40]}, "country_code": "VN"}), "2026-11-18", "new"),
        ]
        bronze = self.spark.createDataFrame(records, ["id", "payload", "ingest_date", "source_file"])
        measurement = measurement_dimension(bronze).first()
        probe = probe_dimension(bronze).first()
        self.assertEqual((measurement.measurement_id, measurement.target, measurement.source_load_id), (1, "new", "2.0"))
        self.assertEqual((probe.probe_id, probe.country_code, probe.longitude, probe.latitude), (1, "VN", 30.0, 40.0))

    def test_ping_fact_deduplicates_and_calculates_loss(self):
        records = [
            (1001, 19967, 1794981600, json.dumps({"_dlt_load_id": "1.0", "avg": 10, "sent": 2, "rcvd": 2}), "2026-11-18", "13"),
            (1001, 19967, 1794981600, json.dumps({"_dlt_load_id": "2.0", "avg": 20, "sent": 2, "rcvd": 1}), "2026-11-18", "13"),
        ]
        bronze = self.spark.createDataFrame(records, ["msm_id", "prb_id", "timestamp", "payload", "window_date", "window_hour"])
        facts = ping_fact(bronze).collect()
        self.assertEqual(len(facts), 1)
        self.assertEqual((facts[0].measurement_id, facts[0].probe_id, facts[0].avg_rtt_ms, facts[0].packet_loss_pct), (1001, 19967, 20.0, 50.0))


if __name__ == "__main__":
    unittest.main()
