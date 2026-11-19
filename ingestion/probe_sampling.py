"""Sinh tập probe phân tầng (ADR-0001) từ dữ liệu API thật, ghi ra config/probe_sets/<version>.json.

Tập nguồn của mỗi af = probe có result public trong cửa sổ tham chiếu (đủ một interval 240 giây)
của các measurement cùng af, giao với metadata `is_public = true` tại thời điểm chạy.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ingestion.api_client import AtlasHttp, iter_probe_pages
from ingestion.logs import log_event
from ingestion.probe_sets import PROBE_SET_DIR, stratified_select
from ingestion.result_records import split_lines
from ingestion.settings import MEASUREMENTS, HttpSettings


def active_probes(http: AtlasHttp, start: datetime, end: datetime) -> dict[int, set[int]]:
    active = {4: set(), 6: set()}
    for msm, spec in MEASUREMENTS.items():
        params = {"start": int(start.timestamp()), "stop": int(end.timestamp()) - 1, "public_only": "true", "format": "txt"}
        body, _ = http.get_bytes(f"measurements/{msm}/results/", params)
        for line in split_lines(body):
            try:
                prb = json.loads(line).get("prb_id")
            except ValueError:
                continue
            if isinstance(prb, int):
                active[spec.af].add(prb)
    return active


def probe_metadata(http: AtlasHttp, ids: set[int]) -> dict[int, dict[str, Any]]:
    pages = iter_probe_pages(http, sorted(ids), fields="id,country_code,asn_v4,asn_v6,is_public")
    return {probe["id"]: probe for _, data, _ in pages for probe in data["results"]}


def build(version: str, k: int, cap: int, seed: str, reference_end: datetime, reference_minutes: int, http: AtlasHttp) -> dict[str, Any]:
    reference_start = reference_end - timedelta(minutes=reference_minutes)
    active = active_probes(http, reference_start, reference_end)
    meta = probe_metadata(http, active[4] | active[6])
    population = {af: [meta[i] for i in sorted(ids) if i in meta and meta[i].get("is_public") is True] for af, ids in active.items()}
    selected_v4 = stratified_select(population[4], "asn_v4", k, cap, seed, prefer=active[6])
    selected_v6 = stratified_select(population[6], "asn_v6", k, cap, seed, prefer={p["probe_id"] for p in selected_v4})

    def stats(af: int, selected: list[dict[str, Any]]) -> dict[str, Any]:
        asn_field = "asn_v4" if af == 4 else "asn_v6"
        strata = Counter(((p.get("country_code") or "ZZ"), p.get(asn_field)) for p in population[af])
        return {
            "active_in_reference": len(active[af]),
            "public_with_metadata": len(population[af]),
            "countries": len({c for c, _ in strata}),
            "strata": len(strata),
            "strata_with_null_asn": sum(1 for (_, a) in strata if a is None),
            "selected": len(selected),
            "selected_countries": len({p["country_code"] for p in selected}),
        }

    return {
        "version": version,
        "mode": "analysis",
        "method": (
            "stratified by (country_code, asn per af); k per stratum; per-country cap; "
            "deterministic sha256(seed:probe_id) order; dual-stack preferred"
        ),
        "params": {
            "k": k,
            "country_cap": cap,
            "seed": seed,
            "reference_window": [reference_start.isoformat(), reference_end.isoformat()],
            "population_filter": "public_only=true results in reference window AND probes API is_public=true",
            "measurements": sorted(MEASUREMENTS),
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
        "stats": {"af4": stats(4, selected_v4), "af6": stats(6, selected_v6)},
        "probes_v4": [p["probe_id"] for p in selected_v4],
        "probes_v6": [p["probe_id"] for p in selected_v6],
        "members_v4": selected_v4,
        "members_v6": selected_v6,
    }


def write(document: dict[str, Any], directory: Path = PROBE_SET_DIR) -> Path:
    path = directory / f"{document['version']}.json"
    if path.exists():
        raise FileExistsError(f"{path} exists; probe sets are immutable, create a new version instead")
    path.write_bytes((json.dumps(document, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8"))  # LF trên mọi OS
    log_event(
        "probe_set", "written", version=document["version"], path=str(path), v4=len(document["probes_v4"]), v6=len(document["probes_v6"])
    )
    return path


def main(version: str, k: int, cap: int, seed: str, reference_end: datetime, reference_minutes: int) -> Path:
    http = AtlasHttp(HttpSettings.from_env())
    return write(build(version, k, cap, seed, reference_end, reference_minutes, http))
