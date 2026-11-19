"""Chạy job ingestion với dlt thật ghi ra thư mục local (thay SeaweedFS) và API giả lập bằng fixture thật."""

import gzip
import hashlib
import json
from datetime import datetime, timezone

import pytest

from ingestion.jobs import IngestContext, run_measurement_metadata, run_probe_metadata, run_results_window
from ingestion.probe_sets import ProbeSet
from ingestion.settings import HttpSettings, IngestSettings
from ingestion.staging_store import StagingStore
from ingestion.time_windows import Window

from .conftest import api_line

W13 = Window(datetime(2026, 11, 18, 13, tzinfo=timezone.utc), datetime(2026, 11, 18, 14, tzinfo=timezone.utc))


class FakeInfo:
    def __init__(self, body, url="fake"):
        self.body_sha256 = hashlib.sha256(body).hexdigest()
        self.fetched_at = "2026-11-18T14:05:00+00:00"
        self.url = url

    def as_dict(self):
        return {"url": self.url, "body_sha256": self.body_sha256, "fetched_at": self.fetched_at}


class FakeAtlas:
    """Trả về results theo probe_ids được yêu cầu, như API thật; ghi lại các request."""

    def __init__(self, results, probes=None, measurements=None, extra_lines=b""):
        self.results, self.probes, self.measurements, self.extra_lines = results, probes or {}, measurements or {}, extra_lines
        self.calls = []

    def get_bytes(self, path, params):
        self.calls.append((path, params))
        msm = int(path.split("/")[1])
        wanted = set(map(int, params["probe_ids"].split(",")))
        lines = [api_line(r) for r in self.results if r["msm_id"] == msm and r["prb_id"] in wanted
                 and params["start"] <= r["timestamp"] <= params["stop"]]
        body = b"\n".join(lines) + b"\n" + (self.extra_lines if self.calls[0] == (path, params) else b"")
        return body, FakeInfo(body)

    def get_json(self, path, params):
        if path.startswith("measurements/"):
            obj = self.measurements[int(path.split("/")[1])]
            body = json.dumps(obj).encode()
            return obj, body, FakeInfo(body)
        ids = [int(i) for i in params["id__in"].split(",")]
        page = {"count": 0, "next": None, "results": [self.probes[i] for i in ids if i in self.probes]}
        return page, json.dumps(page).encode(), FakeInfo(b"")


def make_ctx(tmp_path, atlas, run_id, chunk_size=2):
    v4 = sorted({r["prb_id"] for r in atlas.results if r["af"] == 4})
    v6 = sorted({r["prb_id"] for r in atlas.results if r["af"] == 6})
    probe_set = ProbeSet("test-v1", tuple(v4), tuple(v6), {"version": "test-v1", "probes_v4": v4, "probes_v6": v6})
    return IngestContext(
        StagingStore.local(str(tmp_path)), probe_set, IngestSettings("test-v1", chunk_size, 3),
        HttpSettings(concurrency=2), run_id, http_factory=lambda: atlas,
    )


def read_staged(store, manifest):
    rows = []
    for obj in manifest["objects"]:
        with store.fs.open(obj["path"], "rb") as fh:
            rows += [json.loads(line) for line in gzip.decompress(fh.read()).splitlines()]
    return rows


@pytest.fixture
def results_1001(unique_results):
    return [r for r in unique_results if r["msm_id"] == 1001]


def test_window_stages_exact_bytes_and_writes_manifest(tmp_path, results_1001):
    atlas = FakeAtlas(results_1001, extra_lines=b'{"broken": \n')
    ctx = make_ctx(tmp_path, atlas, "run-1")
    manifest = run_results_window(ctx, 1001, W13)

    assert manifest["status"] == "complete" and manifest["probe_set_version"] == "test-v1"
    assert manifest["counts"]["fetched"] == len(results_1001) + 1
    assert manifest["counts"]["status_malformed_json"] == 1
    assert len(manifest["requests"]) == len(atlas.calls) == -(-len(ctx.probe_set.probes_v4) // 2)
    assert all(c[1]["stop"] - c[1]["start"] == 3599 and c[1]["public_only"] == "true" for c in atlas.calls)
    assert "window_date=2026-11-18/window_hour=20/msm_id=1001" in manifest["objects"][0]["path"]

    rows = read_staged(ctx.store, manifest)
    assert len(rows) == manifest["counts"]["staged"]
    sources = {api_line(r) for r in results_1001}
    for row in rows:
        assert hashlib.sha256(row["raw_json"].encode("utf-8")).hexdigest() == row["raw_sha256"]
        assert row["_dlt_load_id"] == manifest["load_id"]
    assert {row["raw_json"].encode() for row in rows if row["parse_status"] == "ok"} == sources


def test_rerun_overlap_stages_nothing_and_correction_stages_one(tmp_path, results_1001):
    atlas = FakeAtlas(results_1001)
    first = run_results_window(make_ctx(tmp_path, atlas, "run-1"), 1001, W13)
    second = run_results_window(make_ctx(tmp_path, atlas, "run-2"), 1001, W13)
    assert second["counts"]["staged"] == 0 and second["objects"] == [] and second["load_id"] is None
    assert second["counts"]["unchanged"] == first["counts"]["staged"]
    assert len(second["prior_manifests"]) == 1

    corrected = [dict(r) for r in results_1001]
    corrected[0]["avg"] = 123.456
    third = run_results_window(make_ctx(tmp_path, FakeAtlas(corrected), "run-3"), 1001, W13)
    assert third["counts"]["changed"] == 1 and third["counts"]["staged"] == 1
    assert read_staged(make_ctx(tmp_path, atlas, "x").store, third)[0]["change_type"] == "changed"


def test_failed_load_leaves_no_manifest(tmp_path, results_1001, monkeypatch):
    ctx = make_ctx(tmp_path, FakeAtlas(results_1001), "run-1")
    monkeypatch.setattr(ctx.store, "load_with_dlt", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("s3 down")))
    with pytest.raises(RuntimeError):
        run_results_window(ctx, 1001, W13)
    assert ctx.store.glob(f"{ctx.store.root}/_manifests/**/*.json") == []
    # Lần sau không coi gì là đã stage.
    rerun = run_results_window(make_ctx(tmp_path, FakeAtlas(results_1001), "run-2"), 1001, W13)
    assert rerun["counts"]["new"] == len(results_1001)


def test_empty_ipv6_window_is_recorded_as_fetched_empty(tmp_path, real_cases):
    atlas = FakeAtlas([r for r in real_cases.values() if r["af"] == 6])
    ctx = make_ctx(tmp_path, atlas, "run-1")
    late = Window(datetime(2026, 11, 18, 15, tzinfo=timezone.utc), datetime(2026, 11, 18, 16, tzinfo=timezone.utc))
    manifest = run_results_window(ctx, 2001, late)
    assert manifest["counts"]["fetched"] == 0 and manifest["status"] == "complete" and manifest["objects"] == []


def test_probe_metadata_records_not_returned_and_is_public(tmp_path, results_1001):
    probes = {r["prb_id"]: {"id": r["prb_id"], "is_public": True, "country_code": "VN"} for r in results_1001[:2]}
    ctx = make_ctx(tmp_path, FakeAtlas(results_1001, probes=probes), "run-1")
    wanted = [r["prb_id"] for r in results_1001[:2]] + [99999999]
    manifest = run_probe_metadata(ctx, wanted)
    rows = read_staged(ctx.store, manifest)
    assert manifest["counts"] == {"records": 3, "status_ok": 2, "status_not_returned": 1}
    missing = next(r for r in rows if r["entity_id"] == 99999999)
    assert missing["fetch_status"] == "not_returned" and missing.get("raw_json") is None
    ok = next(r for r in rows if r["fetch_status"] == "ok")
    assert json.loads(ok["raw_json"])["is_public"] is True and ok["raw_encoding"] == "canonical_json"


def test_measurement_metadata_must_match_config(tmp_path, results_1001):
    good = {1001: {"id": 1001, "af": 4, "type": "ping", "is_public": True}}
    ctx = make_ctx(tmp_path, FakeAtlas(results_1001, measurements=good), "run-1")
    manifest = run_measurement_metadata(ctx, [1001])
    assert read_staged(ctx.store, manifest)[0]["raw_encoding"] == "http_body"

    bad = {1001: {"id": 1001, "af": 6, "type": "ping"}}
    with pytest.raises(ValueError, match="disagrees"):
        run_measurement_metadata(make_ctx(tmp_path, FakeAtlas(results_1001, measurements=bad), "run-2"), [1001])


def test_cleanup_finds_only_unreferenced_old_objects(tmp_path, results_1001, monkeypatch):
    from datetime import timedelta

    from ingestion.staging_cleanup import find_orphans, run_cleanup

    ctx = make_ctx(tmp_path, FakeAtlas(results_1001), "run-1")
    manifest = run_results_window(ctx, 1001, W13)
    referenced = manifest["objects"][0]["path"]
    orphan_dir = f"{ctx.store.root}/ping_result_raw/window_date=2026-11-18/window_hour=20/msm_id=1001"
    orphan = f"{orphan_dir}/9999.orphan.jsonl.gz"
    ctx.store.write_gzip_text(orphan, "{}\n")
    # Mốc "now" tiêm vào để test không phụ thuộc độ phân giải mtime của hệ điều hành.
    later = datetime.now(timezone.utc) + timedelta(hours=1)
    found, _ = find_orphans(ctx.store, timedelta(hours=0), now=later)
    assert [p.replace("\\", "/").split("/")[-1] for p in found] == ["9999.orphan.jsonl.gz"]
    assert find_orphans(ctx.store, timedelta(hours=48), now=later)[0] == []          # còn mới -> không động vào
    assert run_cleanup(ctx.store, 0, apply=False, now=later) and ctx.store.fs.exists(orphan)   # dry-run không xóa
    run_cleanup(ctx.store, 0, apply=True, now=later)
    assert not ctx.store.fs.exists(orphan) and ctx.store.fs.exists(referenced)
