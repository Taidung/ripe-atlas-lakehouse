from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "ping_results_real_sample.jsonl"


@pytest.fixture(scope="session")
def real_cases() -> dict[str, dict[str, Any]]:
    """10 result thật lấy ngày 2026-11-18 (docs/api-survey.md §5), theo tên trường hợp."""
    cases = {}
    for line in FIXTURE.read_text(encoding="utf-8").splitlines():
        item = json.loads(line)
        cases[item["case"]] = item["record"]
    return cases


@pytest.fixture(scope="session")
def unique_results(real_cases) -> list[dict[str, Any]]:
    """Result khác nhau theo (msm, probe, timestamp). Lưu ý: case normal_v4 và no_mver_old_fw_v4
    là cùng một result thật (probe firmware 4790 không có mver), nên chỉ giữ một bản."""
    seen, out = set(), []
    for record in real_cases.values():
        key = (record["msm_id"], record["prb_id"], record["timestamp"])
        if key not in seen:
            seen.add(key)
            out.append(record)
    return out


def api_line(record: dict[str, Any]) -> bytes:
    """Serialize như response format=txt của API (compact JSON, một result mỗi dòng)."""
    return json.dumps(record, separators=(",", ":")).encode("utf-8")
