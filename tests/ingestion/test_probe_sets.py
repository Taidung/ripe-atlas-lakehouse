import json
from collections import defaultdict

import pytest

from ingestion.probe_sets import load_probe_set, stratified_select


def population():
    probes = []
    probes += [{"id": i, "country_code": "AA", "asn_v4": 1} for i in range(1, 6)]  # tầng 5 probe
    probes += [{"id": 10, "country_code": "AA", "asn_v4": 2}]  # tầng 1 probe
    probes += [{"id": i, "country_code": "BB", "asn_v4": 3} for i in range(20, 23)]
    probes += [{"id": 30, "country_code": None, "asn_v4": None}]  # tầng UNKNOWN
    return probes


def test_k_per_stratum_and_unknown_stratum():
    selected = stratified_select(population(), "asn_v4", k=2, country_cap=None, seed="s")
    by_stratum = defaultdict(list)
    for p in selected:
        by_stratum[(p["country_code"], p["asn"])].append(p["probe_id"])
    assert {k: len(v) for k, v in by_stratum.items()} == {("AA", 1): 2, ("AA", 2): 1, ("BB", 3): 2, ("ZZ", None): 1}


def test_selection_is_deterministic_regardless_of_input_order():
    a = stratified_select(population(), "asn_v4", 2, None, "seed-1")
    b = stratified_select(list(reversed(population())), "asn_v4", 2, None, "seed-1")
    assert a == b


def test_preferred_probes_rank_first():
    selected = stratified_select(population(), "asn_v4", 1, None, "s", prefer={4})
    assert 4 in {p["probe_id"] for p in selected}


def test_country_cap_takes_one_per_stratum_before_second():
    selected = stratified_select(population(), "asn_v4", k=2, country_cap=2, seed="s")
    aa = [p for p in selected if p["country_code"] == "AA"]
    assert len(aa) == 2 and {p["asn"] for p in aa} == {1, 2}


def test_weights_sum_to_country_population():
    selected = stratified_select(population(), "asn_v4", k=2, country_cap=2, seed="s")
    totals = defaultdict(float)
    for p in selected:
        totals[p["country_code"]] += p["weight"]
    assert totals["AA"] == pytest.approx(6) and totals["BB"] == pytest.approx(3) and totals["ZZ"] == pytest.approx(1)


def test_dev_probe_set_loads():
    ps = load_probe_set("dev-200-v1")
    assert len(ps.probes_v4) == len(ps.probes_v6) == 200 and len(ps.sha256) == 64


def test_probe_set_rejects_duplicates_and_version_mismatch(tmp_path):
    (tmp_path / "x.json").write_text(json.dumps({"version": "x", "probes_v4": [1, 1], "probes_v6": []}))
    with pytest.raises(ValueError):
        load_probe_set("x", tmp_path)
    (tmp_path / "y.json").write_text(json.dumps({"version": "z", "probes_v4": [1], "probes_v6": []}))
    with pytest.raises(ValueError):
        load_probe_set("y", tmp_path)
