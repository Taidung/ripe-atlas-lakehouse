import unittest
from unittest.mock import patch

from ingestion import ingestion


class MetadataTest(unittest.TestCase):
    def test_measurements(self):
        calls = []

        def fetch(path, params):
            self.assertEqual(params, {"format": "json"})
            calls.append(path)
            return {"id": int(path.split("/")[1]), "type": "ping"}

        with patch.object(ingestion, "fetch_json", side_effect=fetch):
            rows = list(ingestion.get_ping_measurement_metadata())

        self.assertEqual([row["id"] for row in rows], ingestion.MEASUREMENT_IDS)
        self.assertEqual(calls, [f"measurements/{i}/" for i in ingestion.MEASUREMENT_IDS])

    def test_probes_across_pages(self):
        calls = []

        def fetch(path, params):
            self.assertEqual(path, "probes/")
            self.assertEqual(params["is_public"], "true")
            self.assertEqual(params["page_size"], 50)
            ids = [int(i) for i in params["id__in"].split(",")]
            page = params["page"]
            calls.append((tuple(ids), page))
            return {
                "results": [{"id": i} for i in ids[(page - 1) * 25:page * 25]],
                "next": "more" if page * 25 < len(ids) else None,
            }

        with patch.object(ingestion, "fetch_json", side_effect=fetch):
            rows = list(ingestion.get_probe_metadata())

        self.assertEqual({row["id"] for row in rows}, set(ingestion.SELECTED_PROBE_IDS))
        self.assertEqual(len(rows), 200)
        self.assertEqual(len(calls), 8)

    def test_missing_probe_fails(self):
        with (
            patch.object(ingestion, "SELECTED_PROBE_IDS", [1, 2]),
            patch.object(ingestion, "fetch_json", return_value={"results": [{"id": 1}], "next": None}),
        ):
            with self.assertRaisesRegex(Exception, "Probe IDs not returned by RIPE Atlas: \\[2\\]"):
                list(ingestion.get_probe_metadata())


if __name__ == "__main__":
    unittest.main()
