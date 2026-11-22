import json

import pytest

from ingestion.probe_sets import load_probe_set


def test_dev_probe_set_loads():
    ps = load_probe_set("dev-200-v2")
    assert len(ps.probes_v4) == len(ps.probes_v6) == 200 and len(ps.sha256) == 64
    assert set(ps.document["added_vn"]) <= set(ps.probes_v4)


def test_probe_set_rejects_duplicates_and_version_mismatch(tmp_path):
    (tmp_path / "x.json").write_text(json.dumps({"version": "x", "probes_v4": [1, 1], "probes_v6": []}))
    with pytest.raises(ValueError):
        load_probe_set("x", tmp_path)
    (tmp_path / "y.json").write_text(json.dumps({"version": "z", "probes_v4": [1], "probes_v6": []}))
    with pytest.raises(ValueError):
        load_probe_set("y", tmp_path)
