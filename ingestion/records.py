"""Chuyển body `format=txt` của endpoint results thành record staging, và phân loại delta.

Quy ước bytes (data contract staging):
- Mỗi dòng của body (tách theo b"\\n", bỏ dòng rỗng) là một result nguồn.
- `raw_json` = dòng đó giải mã UTF-8, giữ nguyên từng ký tự; không parse rồi serialize lại.
- `raw_sha256` = SHA-256 của đúng bytes dòng đó (không gồm ký tự xuống dòng), trước khi dlt
  serialize/nén. Bronze tính lại sha2(raw_json) để kiểm tra toàn vẹn.

`parse_status` chỉ mang tính tham khảo cho thống kê ingestion; Bronze kiểm tra lại và quyết định
quarantine.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from ingestion.config import MeasurementSpec
from ingestion.windows import Window

STAGING_COLUMNS = {
    "measurement_id": {"data_type": "bigint", "nullable": False},
    "probe_id": {"data_type": "bigint", "nullable": True},
    "event_ts": {"data_type": "bigint", "nullable": True},
    "parse_status": {"data_type": "text", "nullable": False},
    "raw_json": {"data_type": "text", "nullable": False},
    "raw_sha256": {"data_type": "text", "nullable": False},
    "line_no": {"data_type": "bigint", "nullable": False},
    "request_index": {"data_type": "bigint", "nullable": False},
    "change_type": {"data_type": "text", "nullable": False},
    "run_id": {"data_type": "text", "nullable": False},
    "fetched_at": {"data_type": "text", "nullable": False},
}


def split_lines(body: bytes) -> list[bytes]:
    return [line.rstrip(b"\r") for line in body.split(b"\n") if line.strip()]


def parse_line(line: bytes, spec: MeasurementSpec, window: Window) -> dict[str, Any]:
    record: dict[str, Any] = {
        "measurement_id": spec.measurement_id,
        "probe_id": None,
        "event_ts": None,
        "raw_sha256": hashlib.sha256(line).hexdigest(),
    }
    try:
        text = line.decode("utf-8")
    except UnicodeDecodeError:
        record.update(raw_json=line.decode("utf-8", errors="replace"), parse_status="invalid_utf8")
        return record
    record["raw_json"] = text
    try:
        obj = json.loads(text)
    except ValueError:
        record["parse_status"] = "malformed_json"
        return record
    if not isinstance(obj, dict):
        record["parse_status"] = "not_object"
        return record
    prb, ts = obj.get("prb_id"), obj.get("timestamp")
    record["probe_id"] = prb if type(prb) is int else None
    record["event_ts"] = ts if type(ts) is int else None
    if record["probe_id"] is None or record["event_ts"] is None or type(obj.get("msm_id")) is not int:
        status = "missing_key"
    elif obj["msm_id"] != spec.measurement_id:
        status = "wrong_measurement"
    elif obj.get("af") != spec.af:
        status = "af_mismatch"
    elif not window.contains(record["event_ts"]):
        status = "out_of_window"
    else:
        status = "ok"
    record["parse_status"] = status
    return record


@dataclass
class PriorIndex:
    """Các bản ghi đã được stage cho cùng (measurement, cửa sổ) ở các manifest hoàn tất trước đó."""

    hashes: set[str] = field(default_factory=set)
    by_key: dict[tuple[int, int], set[str]] = field(default_factory=dict)

    def add(self, probe_id: int | None, event_ts: int | None, sha: str) -> None:
        self.hashes.add(sha)
        if probe_id is not None and event_ts is not None:
            self.by_key.setdefault((probe_id, event_ts), set()).add(sha)

    @classmethod
    def from_tsv(cls, texts: Iterable[str]) -> PriorIndex:
        index = cls()
        for text in texts:
            for line in text.splitlines():
                if not line.strip():
                    continue
                prb, ts, sha = line.split("\t")
                index.add(None if prb == "-" else int(prb), None if ts == "-" else int(ts), sha)
        return index


def classify(records: list[dict[str, Any]], prior: PriorIndex) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Giữ lại record mới hoặc đã thay đổi; bỏ record trùng nội dung với lần stage trước.

    - unchanged: cùng raw_sha256 đã được stage (do tải chồng lấn) -> không stage lại.
    - changed: cùng (probe_id, event_ts) nhưng payload khác -> stage, Silver chọn bản theo quy tắc.
    - new: chưa từng thấy.
    Trùng lặp ngay trong một response (cùng sha) chỉ stage một lần, đếm vào duplicate_in_batch.
    """
    counts = {"fetched": len(records), "new": 0, "changed": 0, "unchanged": 0, "duplicate_in_batch": 0}
    seen_now: set[str] = set()
    staged = []
    for record in records:
        sha = record["raw_sha256"]
        if sha in prior.hashes:
            counts["unchanged"] += 1
            continue
        if sha in seen_now:
            counts["duplicate_in_batch"] += 1
            continue
        seen_now.add(sha)
        key = (record["probe_id"], record["event_ts"])
        record["change_type"] = "changed" if key in prior.by_key else "new"
        counts[record["change_type"]] += 1
        staged.append(record)
    for status in ("ok", "malformed_json", "not_object", "missing_key", "wrong_measurement", "af_mismatch", "out_of_window", "invalid_utf8"):
        counts[f"status_{status}"] = sum(1 for r in records if r["parse_status"] == status)
    counts["staged"] = len(staged)
    return staged, counts


def index_tsv(records: Iterable[dict[str, Any]]) -> str:
    lines = []
    for r in records:
        prb = "-" if r["probe_id"] is None else str(r["probe_id"])
        ts = "-" if r["event_ts"] is None else str(r["event_ts"])
        lines.append(f"{prb}\t{ts}\t{r['raw_sha256']}")
    return "\n".join(lines) + ("\n" if lines else "")
