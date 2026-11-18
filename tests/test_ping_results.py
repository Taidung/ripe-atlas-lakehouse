import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from ingestion import ingestion


class PingResultsTest(unittest.TestCase):
    def test_selected_probes_and_half_open_hour(self):
        start = datetime(2026, 11, 17, 13, tzinfo=ingestion.TIMEZONE)
        end = start + timedelta(hours=1)
        start_ts, end_ts = int(start.timestamp()), int(end.timestamp())
        selected = ingestion.SELECTED_PROBE_IDS
        self.assertEqual(len(selected), len(set(selected)))
        self.assertEqual(len(selected), 200)
        calls = []

        class Response:
            def __init__(self, measurement_id):
                self.measurement_id = measurement_id

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def raise_for_status(self):
                pass

            def iter_lines(self):
                for timestamp in (start_ts - 1, start_ts, end_ts - 1, end_ts):
                    yield json.dumps({
                        "msm_id": self.measurement_id,
                        "prb_id": selected[0],
                        "timestamp": timestamp,
                        "type": "ping",
                    }).encode()

        def get(path, params, stream):
            self.assertTrue(stream)
            self.assertEqual((params["start"], params["stop"]), (start_ts - 1, end_ts))
            self.assertEqual(set(map(int, params["probe_ids"].split(","))), set(selected))
            calls.append(path)
            return Response(int(path.split("/")[1]))

        with patch.object(ingestion.ripe_atlas_client, "get", side_effect=get):
            rows = list(ingestion.get_ping_results(start, end))

        self.assertEqual(len(calls), len(ingestion.MEASUREMENT_IDS))
        self.assertEqual(len(rows), 2 * len(ingestion.MEASUREMENT_IDS))
        self.assertTrue(all(start_ts <= row["timestamp"] < end_ts for row in rows))


if __name__ == "__main__":
    unittest.main()
