"""Tập probe có phiên bản (probe_set_version) để kết quả tái lập được.

- dev-200-v1: 200 probe cố định của repo (chọn tay theo châu lục), dùng cho cả IPv4 và IPv6.
- strat-*: mẫu phân tầng theo (country_code, ASN theo af), xem docs/adr/0001-probe-sampling.md.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
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


def _hash_rank(seed: str, probe_id: int) -> str:
    return hashlib.sha256(f"{seed}:{probe_id}".encode()).hexdigest()


def stratified_select(
    population: list[dict[str, Any]],
    asn_field: str,
    k: int,
    country_cap: int | None,
    seed: str,
    prefer: set[int] | None = None,
) -> list[dict[str, Any]]:
    """Chọn tối đa k probe mỗi tầng (country, asn), rồi tối đa country_cap probe mỗi quốc gia.

    Thứ tự trong tầng: probe thuộc `prefer` trước (ví dụ dual-stack), sau đó theo sha256(seed:id),
    nên kết quả deterministic. Khi cắt theo trần quốc gia, lấy lần lượt hạng 1 của mọi tầng rồi
    mới tới hạng 2, để giữ được nhiều ASN nhất.

    Trọng số: w = N_tầng / n_tầng, rồi co giãn trong mỗi quốc gia để tổng trọng số = N_quốc_gia
    (bù cho các tầng bị trần quốc gia loại hẳn). Trả về danh sách dict có probe_id, country_code,
    asn, stratum_size, rank_in_stratum, weight.
    """
    if k < 1:
        raise ValueError("k must be >= 1")
    prefer = prefer or set()
    strata: dict[tuple[str, Any], list[dict[str, Any]]] = defaultdict(list)
    for probe in population:
        country = probe.get("country_code") or "ZZ"
        strata[(country, probe.get(asn_field))].append(probe)

    candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (country, asn), members in strata.items():
        members.sort(key=lambda p: (p["id"] not in prefer, _hash_rank(seed, p["id"])))
        for rank, probe in enumerate(members[:k]):
            candidates[country].append(
                {
                    "probe_id": probe["id"],
                    "country_code": country,
                    "asn": asn,
                    "stratum_size": len(members),
                    "rank_in_stratum": rank,
                    "_order": (rank, probe["id"] not in prefer, _hash_rank(seed, probe["id"])),
                }
            )

    country_population = defaultdict(int)
    for (country, _), members in strata.items():
        country_population[country] += len(members)

    selected: list[dict[str, Any]] = []
    for country, items in candidates.items():
        items.sort(key=lambda item: item["_order"])
        kept = items if country_cap is None else items[:country_cap]
        per_stratum = defaultdict(int)
        for item in kept:
            per_stratum[item["asn"]] += 1
        raw = {item["probe_id"]: item["stratum_size"] / per_stratum[item["asn"]] for item in kept}
        scale = country_population[country] / sum(raw.values())
        for item in kept:
            item.pop("_order")
            item["weight"] = round(raw[item["probe_id"]] * scale, 6)
            selected.append(item)
    selected.sort(key=lambda item: item["probe_id"])
    return selected
