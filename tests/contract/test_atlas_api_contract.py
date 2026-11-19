"""Contract test với RIPE Atlas API thật (marker live). Ít request (6), không phụ thuộc số lượng result để pass.

Kiểm tra lại các giả định đã đo trong docs/api-survey.md: metadata 4 measurement, biên start/stop inclusive,
trường bắt buộc của ping result, semantics rcvd/dup/-1, phân trang probes.
"""

from datetime import datetime, timedelta, timezone

import pytest

from ingestion.api_client import AtlasHttp
from ingestion.result_records import split_lines
from ingestion.settings import MEASUREMENTS, HttpSettings

pytestmark = pytest.mark.live
http = AtlasHttp(HttpSettings(max_attempts=3))


@pytest.mark.parametrize("msm", sorted(MEASUREMENTS))
def test_measurement_metadata_matches_config(msm):
    obj, _, _ = http.get_json(f"measurements/{msm}/", {"format": "json"})
    spec = MEASUREMENTS[msm]
    assert (obj["id"], obj["af"], obj["type"], obj["target"]) == (msm, spec.af, "ping", spec.target)
    assert obj["interval"] == 240 and obj["packets"] == 3


def test_results_contract_bounds_and_semantics():
    import json

    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    start = end - timedelta(minutes=10)
    body, _ = http.get_bytes("measurements/1001/results/", {"start": int(start.timestamp()), "stop": int(end.timestamp()) - 1,
                                                             "probe_ids": "1000,26323,6001,6002,6003", "public_only": "true", "format": "txt"})
    records = [json.loads(line) for line in split_lines(body)]
    for r in records:
        assert {"msm_id", "prb_id", "timestamp", "af", "sent", "rcvd", "dup", "avg", "result", "type"} <= r.keys()
        assert int(start.timestamp()) <= r["timestamp"] <= int(end.timestamp()) - 1
        valid = [e for e in r["result"] if "rtt" in e and not e.get("dup")]
        assert len(valid) == r["rcvd"]
        assert (r["avg"] == -1) == (r["rcvd"] == 0)
    if records:  # biên inclusive: truy vấn đúng một giây trả lại result có timestamp đó
        t = records[0]["timestamp"]
        one, _ = http.get_bytes("measurements/1001/results/", {"start": t, "stop": t, "probe_ids": records[0]["prb_id"], "format": "txt"})
        assert any(json.loads(line)["timestamp"] == t for line in split_lines(one))


def test_probes_pagination_and_filter_respected():
    data, _, _ = http.get_json("probes/", {"id__in": "1,2,3", "page_size": 2, "format": "json"})
    assert set(data) >= {"count", "next", "results"} and len(data["results"]) <= 2
    assert all(p["id"] in (1, 2, 3) for p in data["results"])
