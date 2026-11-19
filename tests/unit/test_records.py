import hashlib
import json
from datetime import datetime, timezone

import pytest

from ingestion.config import MEASUREMENTS
from ingestion.records import PriorIndex, classify, index_tsv, parse_line, split_lines
from ingestion.windows import Window

from .conftest import api_line

W13 = Window(datetime(2026, 11, 18, 13, tzinfo=timezone.utc), datetime(2026, 11, 18, 14, tzinfo=timezone.utc))


def test_every_real_case_parses_ok_and_keeps_exact_bytes(real_cases):
    for name, record in real_cases.items():
        line = api_line(record)
        parsed = parse_line(line, MEASUREMENTS[record["msm_id"]], W13)
        assert parsed["parse_status"] == "ok", name
        assert parsed["raw_json"].encode("utf-8") == line
        assert parsed["raw_sha256"] == hashlib.sha256(line).hexdigest()
        assert (parsed["probe_id"], parsed["event_ts"]) == (record["prb_id"], record["timestamp"])


@pytest.mark.parametrize(
    "line, status",
    [
        (b'{"msm_id":1001,"prb_id":', "malformed_json"),
        (b"[1,2,3]", "not_object"),
        (b'{"msm_id":1001,"timestamp":1795006800,"af":4}', "missing_key"),
        (b'{"msm_id":1001,"prb_id":"12","timestamp":1795006800,"af":4}', "missing_key"),
        (b'{"msm_id":1004,"prb_id":1,"timestamp":1795006800,"af":4}', "wrong_measurement"),
        (b'{"msm_id":1001,"prb_id":1,"timestamp":1795006800,"af":6}', "af_mismatch"),
        (b'{"msm_id":1001,"prb_id":1,"timestamp":1795010400,"af":4}', "out_of_window"),
        (b'{"msm_id":1001,"prb_id":1,"timestamp":1795006800,"af":4,"x":"\xff"}', "invalid_utf8"),
    ],
)
def test_bad_lines_are_flagged_not_raised(line, status):
    parsed = parse_line(line, MEASUREMENTS[1001], W13)
    assert parsed["parse_status"] == status
    assert parsed["raw_sha256"] == hashlib.sha256(line).hexdigest()


def test_split_lines_drops_blank_and_crlf():
    assert split_lines(b'{"a":1}\r\n\n{"b":2}\n') == [b'{"a":1}', b'{"b":2}']


def _records(results):
    return [parse_line(api_line(r), MEASUREMENTS[1001], W13) for r in results if r["msm_id"] == 1001]


def test_fixture_contains_one_real_duplicate(real_cases):
    # Hai case trỏ tới cùng một result: classify phải coi là trùng trong batch.
    staged, counts = classify(_records(real_cases.values()), PriorIndex())
    assert counts["duplicate_in_batch"] == 1 and counts["staged"] == counts["fetched"] - 1


def test_classify_overlap_rerun_correction_and_in_batch_duplicate(unique_results):
    real_cases = {i: r for i, r in enumerate(unique_results)}
    first = _records(real_cases.values())
    staged, counts = classify(first, PriorIndex())
    assert counts["staged"] == counts["new"] == len(first) > 0

    prior = PriorIndex.from_tsv([index_tsv(staged)])
    rerun = _records(real_cases.values())
    staged_again, counts_again = classify(rerun, prior)
    assert staged_again == [] and counts_again["unchanged"] == len(first)

    # Nguồn sửa payload của cùng (probe, timestamp): stage lại với change_type=changed.
    corrected = dict(next(r for r in real_cases.values() if r["msm_id"] == 1001))
    corrected["avg"] = 999.0
    batch = _records(real_cases.values()) + [parse_line(api_line(corrected), MEASUREMENTS[1001], W13)] * 2
    staged_fix, counts_fix = classify(batch, prior)
    assert [r["change_type"] for r in staged_fix] == ["changed"]
    assert counts_fix["duplicate_in_batch"] == 1


def test_index_tsv_roundtrip_handles_missing_keys():
    records = [
        {"probe_id": 1, "event_ts": 2, "raw_sha256": "a"},
        {"probe_id": None, "event_ts": None, "raw_sha256": "b"},
    ]
    index = PriorIndex.from_tsv([index_tsv(records)])
    assert index.hashes == {"a", "b"} and index.by_key == {(1, 2): {"a"}}
