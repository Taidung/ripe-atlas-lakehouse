"""Job ingestion. Đơn vị tiến độ: (measurement_id, cửa sổ 1 giờ).

Quy trình một job results:
  1. Đọc hash index của các manifest hoàn tất trước đó cho cùng (measurement, cửa sổ).
  2. Gọi API theo từng nhóm probe (song song có giới hạn), stream format=txt, giữ nguyên bytes.
  3. Bỏ record đã stage (trùng sha), stage record mới hoặc thay đổi bằng dlt.
  4. Ghi hash index, rồi ghi manifest (commit marker).
Lỗi ở bước nào trước 4 thì không có manifest: lần chạy sau tự làm lại cửa sổ đó.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from ingestion.atlas_http import AtlasHttp, ResponseInfo
from ingestion.config import MEASUREMENTS, REPORT_TZ, HttpSettings, IngestSettings, MeasurementSpec
from ingestion.logs import log_event
from ingestion.probe_sets import ProbeSet, chunked
from ingestion.records import STAGING_COLUMNS, PriorIndex, classify, index_tsv, parse_line, split_lines
from ingestion.staging import MANIFEST_VERSION, StagingStore
from ingestion.windows import Window

RESULTS_TABLE = "ping_result_raw"
RESULTS_LAYOUT = "{table_name}/window_date={window_date}/window_hour={window_hour}/msm_id={msm_id}/{load_id}.{file_id}.{ext}"
METADATA_LAYOUT = "{table_name}/ictrl_dt={ictrl_dt}/{load_id}.{file_id}.{ext}"

METADATA_COLUMNS = {
    "entity_id": {"data_type": "bigint", "nullable": False},
    "fetch_status": {"data_type": "text", "nullable": False},
    "raw_json": {"data_type": "text", "nullable": True},
    "raw_sha256": {"data_type": "text", "nullable": True},
    "raw_encoding": {"data_type": "text", "nullable": False},
    "is_public": {"data_type": "bool", "nullable": True},
    "observed_at": {"data_type": "text", "nullable": False},
    "request_index": {"data_type": "bigint", "nullable": False},
    "run_id": {"data_type": "text", "nullable": False},
}


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


@dataclass
class IngestContext:
    store: StagingStore
    probe_set: ProbeSet
    settings: IngestSettings
    http_settings: HttpSettings
    run_id: str
    run_type: str = "scheduled"  # scheduled | backfill | manual
    http_factory: Callable[[], Any] | None = None
    now: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))

    def http(self) -> Any:
        # Mỗi worker một session riêng: requests.Session không đảm bảo thread-safe.
        return self.http_factory() if self.http_factory else AtlasHttp(self.http_settings)


# ---------------------------------------------------------------------- paths
def results_partition(spec: MeasurementSpec, window: Window) -> str:
    return f"{window.path_fragment}/msm_id={spec.measurement_id}"


def results_manifest_glob(store: StagingStore, spec: MeasurementSpec, window: Window) -> str:
    return f"{store.root}/_manifests/{RESULTS_TABLE}/{results_partition(spec, window)}/*.json"


def load_prior_index(store: StagingStore, spec: MeasurementSpec, window: Window) -> tuple[PriorIndex, list[str]]:
    manifests = store.glob(results_manifest_glob(store, spec, window))
    texts = []
    for path in manifests:
        manifest = store.read_json(path)
        if manifest.get("status") == "complete" and manifest.get("hash_index"):
            texts.append(store.read_gzip_text(manifest["hash_index"]))
    return PriorIndex.from_tsv(texts), manifests


# ---------------------------------------------------------------------- results
def fetch_chunk(ctx: IngestContext, spec: MeasurementSpec, window: Window, index: int, probe_ids: list[int]) -> tuple[int, list[bytes], ResponseInfo]:
    params = {
        **window.api_params(),
        "probe_ids": ",".join(map(str, probe_ids)),
        "public_only": "true",
        "format": "txt",
    }
    body, info = ctx.http().get_bytes(f"measurements/{spec.measurement_id}/results/", params)
    return index, split_lines(body), info


def run_results_window(ctx: IngestContext, measurement_id: int, window: Window) -> dict[str, Any]:
    spec = MEASUREMENTS[measurement_id]
    started_at = ctx.now()
    probe_ids = ctx.probe_set.for_af(spec.af)
    chunks = chunked(probe_ids, ctx.settings.probe_chunk_size)
    log_event("ingest", "window_start", run_id=ctx.run_id, measurement_id=measurement_id, window=window.label, chunks=len(chunks), probes=len(probe_ids))

    prior, prior_manifests = load_prior_index(ctx.store, spec, window)

    records: list[dict[str, Any]] = []
    requests_meta: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=ctx.http_settings.concurrency) as pool:
        futures = [pool.submit(fetch_chunk, ctx, spec, window, i, ids) for i, ids in enumerate(chunks)]
        for future in futures:  # giữ thứ tự chunk để line_no ổn định
            index, lines, info = future.result()
            meta = info.as_dict()
            meta.update(request_index=index, probe_count=len(chunks[index]), lines=len(lines))
            requests_meta.append(meta)
            for line_no, line in enumerate(lines):
                record = parse_line(line, spec, window)
                record.update(line_no=line_no, request_index=index, run_id=ctx.run_id, fetched_at=info.fetched_at)
                records.append(record)

    staged, counts = classify(records, prior)
    placeholders = {
        "window_date": f"{window.local_start:%Y-%m-%d}",
        "window_hour": f"{window.local_start:%H}",
        "msm_id": str(measurement_id),
    }
    load_id, objects = None, []
    if staged:
        load_id, objects = ctx.store.load_with_dlt(RESULTS_TABLE, staged, STAGING_COLUMNS, RESULTS_LAYOUT, placeholders)

    partition = results_partition(spec, window)
    index_path = f"{ctx.store.root}/_index/{RESULTS_TABLE}/{partition}/{ctx.run_id}.tsv.gz"
    ctx.store.write_gzip_text(index_path, index_tsv(staged))

    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "entity": RESULTS_TABLE,
        "status": "complete",
        "run_id": ctx.run_id,
        "run_type": ctx.run_type,
        "load_id": load_id,
        "measurement_id": measurement_id,
        "af": spec.af,
        "target": spec.target,
        "window_start": window.start.isoformat(),
        "window_end": window.end.isoformat(),
        "window_start_local": window.local_start.isoformat(),
        "ictrl_dt": window.ictrl_dt.isoformat(),
        "probe_set_version": ctx.probe_set.version,
        "probe_set_sha256": ctx.probe_set.sha256,
        "probes_requested": len(probe_ids),
        "requests": requests_meta,
        "counts": counts,
        "objects": [{"path": p, "bytes": ctx.store.size(p)} for p in objects],
        "hash_index": index_path,
        "prior_manifests": prior_manifests,
        "started_at": started_at.isoformat(),
        "finished_at": ctx.now().isoformat(),
    }
    manifest_path = f"{ctx.store.root}/_manifests/{RESULTS_TABLE}/{partition}/{ctx.run_id}.json"
    ctx.store.write_json(manifest_path, manifest)
    log_event("ingest", "window_complete", run_id=ctx.run_id, measurement_id=measurement_id, window=window.label, manifest=manifest_path, **counts)
    manifest["manifest_path"] = manifest_path
    return manifest


# ---------------------------------------------------------------------- metadata
def _metadata_manifest(ctx: IngestContext, entity: str, records, requests_meta, load_id, objects, started_at, extra=None) -> dict[str, Any]:
    ictrl_dt = started_at.astimezone(REPORT_TZ).date().isoformat()
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "entity": entity,
        "status": "complete",
        "run_id": ctx.run_id,
        "run_type": ctx.run_type,
        "load_id": load_id,
        "ictrl_dt": ictrl_dt,
        "probe_set_version": ctx.probe_set.version,
        "requests": requests_meta,
        "counts": {
            "records": len(records),
            **{f"status_{s}": sum(1 for r in records if r["fetch_status"] == s) for s in ("ok", "not_returned")},
        },
        "objects": [{"path": p, "bytes": ctx.store.size(p)} for p in objects],
        "started_at": started_at.isoformat(),
        "finished_at": ctx.now().isoformat(),
        **(extra or {}),
    }
    path = f"{ctx.store.root}/_manifests/{entity}/ictrl_dt={ictrl_dt}/{ctx.run_id}.json"
    ctx.store.write_json(path, manifest)
    manifest["manifest_path"] = path
    log_event("ingest", "metadata_complete", run_id=ctx.run_id, entity=entity, manifest=path, **manifest["counts"])
    return manifest


def _load_metadata(ctx: IngestContext, entity: str, records: list[dict[str, Any]], started_at: datetime) -> tuple[str, list[str]]:
    ictrl_dt = started_at.astimezone(REPORT_TZ).date().isoformat()
    return ctx.store.load_with_dlt(entity, records, METADATA_COLUMNS, METADATA_LAYOUT, {"ictrl_dt": ictrl_dt})


def run_measurement_metadata(ctx: IngestContext, measurement_ids: list[int]) -> dict[str, Any]:
    """Snapshot metadata measurement. raw_json = nguyên bytes body của GET /measurements/{id}/."""
    started_at = ctx.now()
    http = ctx.http()
    records, requests_meta = [], []
    for i, msm in enumerate(measurement_ids):
        obj, body, info = http.get_json(f"measurements/{msm}/", {"format": "json"})
        if not isinstance(obj, dict) or obj.get("id") != msm:
            raise ValueError(f"Unexpected metadata payload for measurement {msm}")
        spec = MEASUREMENTS[msm]
        if obj.get("af") != spec.af or obj.get("type") != "ping":
            raise ValueError(f"Measurement {msm} metadata disagrees with config: af={obj.get('af')} type={obj.get('type')}")
        requests_meta.append({**info.as_dict(), "request_index": i})
        records.append({
            "entity_id": msm,
            "fetch_status": "ok",
            "raw_json": body.decode("utf-8"),
            "raw_sha256": info.body_sha256,
            "raw_encoding": "http_body",
            "is_public": obj.get("is_public"),
            "observed_at": info.fetched_at,
            "request_index": i,
            "run_id": ctx.run_id,
        })
    load_id, objects = _load_metadata(ctx, "measurement_metadata_raw", records, started_at)
    return _metadata_manifest(ctx, "measurement_metadata_raw", records, requests_meta, load_id, objects, started_at)


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def run_probe_metadata(ctx: IngestContext, probe_ids: list[int] | None = None, page_size: int = 500) -> dict[str, Any]:
    """Snapshot metadata probe theo id__in, KHÔNG lọc is_public để ghi nhận cả probe đã chuyển private.

    raw_json là JSON canonical (sort_keys) của từng phần tử trong trang `results`, vì API trả về một
    trang chứa nhiều probe; SHA-256 của nguyên body từng trang nằm trong manifest.requests.
    Probe được yêu cầu nhưng không có trong response: record fetch_status=not_returned.
    """
    started_at = ctx.now()
    wanted = sorted(set(probe_ids if probe_ids is not None else ctx.probe_set.all_probes))
    http = ctx.http()
    records: list[dict[str, Any]] = []
    requests_meta: list[dict[str, Any]] = []
    found: set[int] = set()
    request_index = 0
    for chunk in chunked(wanted, page_size):
        page = 1
        while True:
            params = {"id__in": ",".join(map(str, chunk)), "page_size": page_size, "page": page, "format": "json"}
            data, _, info = http.get_json("probes/", params)
            if not isinstance(data, dict) or not isinstance(data.get("results"), list):
                raise ValueError(f"Unexpected probes page: {info.url}")
            requests_meta.append({**info.as_dict(), "request_index": request_index, "returned": len(data["results"])})
            for probe in data["results"]:
                pid = probe.get("id") if isinstance(probe, dict) else None
                if pid not in chunk:
                    raise ValueError(f"API returned probe {pid} that was not requested (filter ignored?)")
                found.add(pid)
                text = canonical_json(probe)
                records.append({
                    "entity_id": pid,
                    "fetch_status": "ok",
                    "raw_json": text,
                    "raw_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                    "raw_encoding": "canonical_json",
                    "is_public": probe.get("is_public"),
                    "observed_at": info.fetched_at,
                    "request_index": request_index,
                    "run_id": ctx.run_id,
                })
            request_index += 1
            if not data.get("next"):
                break
            page += 1
    observed = ctx.now().isoformat()
    for pid in sorted(set(wanted) - found):
        records.append({
            "entity_id": pid, "fetch_status": "not_returned", "raw_json": None, "raw_sha256": None,
            "raw_encoding": "none", "is_public": None, "observed_at": observed, "request_index": -1, "run_id": ctx.run_id,
        })
    load_id, objects = _load_metadata(ctx, "probe_metadata_raw", records, started_at)
    return _metadata_manifest(ctx, "probe_metadata_raw", records, requests_meta, load_id, objects, started_at,
                              {"probes_requested": len(wanted)})
