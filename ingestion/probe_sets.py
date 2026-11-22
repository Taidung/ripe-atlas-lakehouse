"""Tập probe có phiên bản (probe_set_version) để kết quả tái lập được.

- dev-200-v2: 200 probe cố định của repo (dev-200-v1 + mọi probe Việt Nam đang hoạt động), dùng cho cả IPv4 và IPv6.
  Cách chọn ghi trong trường `method` của file JSON.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ingestion.settings import REPO_ROOT

PROBE_SET_DIR = REPO_ROOT / "config" / "probe_sets"


@dataclass(frozen=True)
class ProbeSet:
    version: str
    probes_v4: tuple[int, ...]
    probes_v6: tuple[int, ...]
    document: dict[str, Any]

    def for_af(self, af: int) -> tuple[int, ...]:
        if af == 4:
            return self.probes_v4
        if af == 6:
            return self.probes_v6
        raise ValueError(f"Unsupported address family {af}")

    @property
    def all_probes(self) -> list[int]:
        return sorted(set(self.probes_v4) | set(self.probes_v6))

    @property
    def sha256(self) -> str:
        return hashlib.sha256(json.dumps(self.document, sort_keys=True).encode()).hexdigest()


def load_probe_set(version: str, directory: Path = PROBE_SET_DIR) -> ProbeSet:
    path = directory / f"{version}.json"
    if not path.exists():
        raise FileNotFoundError(f"Probe set {version} not found at {path}")
    doc = json.loads(path.read_text(encoding="utf-8"))
    if doc.get("version") != version:
        raise ValueError(f"Probe set file {path} declares version {doc.get('version')!r}")
    v4, v6 = doc["probes_v4"], doc["probes_v6"]
    for name, ids in (("probes_v4", v4), ("probes_v6", v6)):
        if len(ids) != len(set(ids)) or not all(isinstance(i, int) and i > 0 for i in ids):
            raise ValueError(f"{name} in {path} must be unique positive integers")
    return ProbeSet(version, tuple(sorted(v4)), tuple(sorted(v6)), doc)


def chunked(ids: Iterable[int], size: int) -> list[list[int]]:
    ids = list(ids)
    return [ids[i : i + size] for i in range(0, len(ids), size)]
