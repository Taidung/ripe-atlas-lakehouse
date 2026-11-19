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
import queue
import threading
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ingestion.api_client import AtlasHttp, ResponseInfo, iter_probe_pages
from ingestion.logs import log_event
from ingestion.probe_sets import ProbeSet, chunked
from ingestion.result_records import STAGING_COLUMNS, PriorIndex, classify, index_tsv, parse_line, split_lines
from ingestion.settings import MEASUREMENTS, REPORT_TZ, HttpSettings, IngestSettings, MeasurementSpec
from ingestion.staging_store import MANIFEST_VERSION, StagingStore
from ingestion.time_windows import Window

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


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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
    _idle_clients: queue.SimpleQueue = field(default_factory=queue.SimpleQueue, repr=False)
    _all_clients: list = field(default_factory=list, repr=False)
    _clients_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @contextmanager
    def http(self) -> Iterator[Any]:
        """Mượn một HTTP client cho một luồng, trả lại khi xong.

        requests.Session không đảm bảo thread-safe, nên mỗi luồng dùng một client riêng; client được tái sử dụng
        giữa các cửa sổ và nhóm probe (số session = số luồng đồng thời, không phải số request), giữ kết nối TLS.
        """
        try:
            client = self._idle_clients.get_nowait()
        except queue.Empty:
            client = self.http_factory() if self.http_factory else AtlasHttp(self.http_settings)
            with self._clients_lock:
                self._all_clients.append(client)
        try:
            yield client
        finally:
            self._idle_clients.put(client)

    def close(self) -> None:
        """Đóng mọi HTTP client đã tạo (gọi một lần khi lệnh kết thúc)."""
        with self._clients_lock:
            clients, self._all_clients = self._all_clients, []
        for client in clients:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def manifest_header(self, entity: str, load_id: str | None, objects: list[str], started_at: datetime) -> dict[str, Any]:
        """Các trường chung của mọi manifest (results và metadata)."""
        return {
            "manifest_version": MANIFEST_VERSION,
            "entity": entity,
            "status": "complete",
            "run_id": self.run_id,
            "run_type": self.run_type,
            "load_id": load_id,
            "probe_set_version": self.probe_set.version,
            "objects": [{"path": p, "bytes": self.store.size(p)} for p in objects],
            "started_at": started_at.isoformat(),
            "finished_at": self.now().isoformat(),
        }

    def write_manifest(self, path: str, manifest: dict[str, Any]) -> dict[str, Any]:
        self.store.write_json(path, manifest)
        return {**manifest, "manifest_path": path}


# ---------------------------------------------------------------------- results
def results_partition(spec: MeasurementSpec, window: Window) -> str:
    return f"{window.path_fragment}/msm_id={spec.measurement_id}"


def load_prior_index(store: StagingStore, spec: MeasurementSpec, window: Window) -> tuple[PriorIndex, list[str]]:
    manifests = store.glob(f"{store.root}/_manifests/{RESULTS_TABLE}/{results_partition(spec, window)}/*.json")
    texts = []
    for path in manifests:
        manifest = store.read_json(path)
        if manifest.get("status") == "complete" and manifest.get("hash_index"):
            texts.append(store.read_gzip_text(manifest["hash_index"]))
    return PriorIndex.from_tsv(texts), manifests


def fetch_chunk(ctx: IngestContext, spec: MeasurementSpec, window: Window, probe_ids: list[int]) -> tuple[list[bytes], ResponseInfo]:
    params = {**window.api_params(), "probe_ids": ",".join(map(str, probe_ids)), "public_only": "true", "format": "txt"}
    with ctx.http() as http:
        body, info = http.get_bytes(f"measurements/{spec.measurement_id}/results/", params)
    return split_lines(body), info


def fetch_results(ctx: IngestContext, spec: MeasurementSpec, window: Window, chunks: list[list[int]]) -> tuple[list[dict], list[dict]]:
    """Gọi API song song theo nhóm probe. Trả về (record staging, metadata từng request)."""
    records: list[dict[str, Any]] = []
    requests_meta: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=ctx.http_settings.concurrency) as pool:
        futures = [pool.submit(fetch_chunk, ctx, spec, window, ids) for ids in chunks]
        for index, future in enumerate(futures):  # giữ thứ tự chunk để line_no ổn định
            lines, info = future.result()
            requests_meta.append({**info.as_dict(), "request_index": index, "probe_count": len(chunks[index]), "lines": len(lines)})
            for line_no, line in enumerate(lines):
                record = parse_line(line, spec, window)
                record.update(line_no=line_no, request_index=index, run_id=ctx.run_id, fetched_at=info.fetched_at)
                records.append(record)
    return records, requests_meta


def run_results_window(ctx: IngestContext, measurement_id: int, window: Window) -> dict[str, Any]:
    spec = MEASUREMENTS[measurement_id]
    started_at = ctx.now()
    probe_ids = ctx.probe_set.for_af(spec.af)
    chunks = chunked(probe_ids, ctx.settings.probe_chunk_size)
    log_event(
        "ingest",
        "window_start",
        run_id=ctx.run_id,
        measurement_id=measurement_id,
        window=window.label,
        chunks=len(chunks),
        probes=len(probe_ids),
    )

    prior, prior_manifests = load_prior_index(ctx.store, spec, window)
    records, requests_meta = fetch_results(ctx, spec, window, chunks)
    staged, counts = classify(records, prior)

    load_id, objects = None, []
    if staged:
        placeholders = {
            "window_date": f"{window.local_start:%Y-%m-%d}",
            "window_hour": f"{window.local_start:%H}",
            "msm_id": str(measurement_id),
        }
        load_id, objects = ctx.store.load_with_dlt(RESULTS_TABLE, staged, STAGING_COLUMNS, RESULTS_LAYOUT, placeholders)

    partition = results_partition(spec, window)
    index_path = f"{ctx.store.root}/_index/{RESULTS_TABLE}/{partition}/{ctx.run_id}.tsv.gz"
    ctx.store.write_gzip_text(index_path, index_tsv(staged))

    manifest = {
        **ctx.manifest_header(RESULTS_TABLE, load_id, objects, started_at),
        "measurement_id": measurement_id,
        "af": spec.af,
        "target": spec.target,
        "window_start": window.start.isoformat(),
        "window_end": window.end.isoformat(),
        "window_start_local": window.local_start.isoformat(),
        "ictrl_dt": window.ictrl_dt.isoformat(),
        "probe_set_sha256": ctx.probe_set.sha256,
        "probes_requested": len(probe_ids),
        "requests": requests_meta,
        "counts": counts,
        "hash_index": index_path,
        "prior_manifests": prior_manifests,
    }
    manifest = ctx.write_manifest(f"{ctx.store.root}/_manifests/{RESULTS_TABLE}/{partition}/{ctx.run_id}.json", manifest)
    log_event(
        "ingest",
        "window_complete",
        run_id=ctx.run_id,
        measurement_id=measurement_id,
        window=window.label,
        manifest=manifest["manifest_path"],
        **counts,
    )
    return manifest


# ---------------------------------------------------------------------- metadata
def metadata_record(
    ctx: IngestContext,
    entity_id: int,
    raw_json: str | None,
    raw_sha256: str | None,
    raw_encoding: str,
    is_public: bool | None,
    observed_at: str,
    request_index: int,
    fetch_status: str = "ok",
) -> dict[str, Any]:
    return {
        "entity_id": entity_id,
        "fetch_status": fetch_status,
        "raw_json": raw_json,
        "raw_sha256": raw_sha256,
        "raw_encoding": raw_encoding,
        "is_public": is_public,
        "observed_at": observed_at,
        "request_index": request_index,
        "run_id": ctx.run_id,
    }


def stage_metadata(
    ctx: IngestContext, entity: str, records: list[dict], requests_meta: list[dict], started_at: datetime, **extra: Any
) -> dict[str, Any]:
    """Ghi record metadata bằng dlt rồi ghi manifest, partition theo ngày (giờ VN) bắt đầu chạy."""
    ictrl_dt = started_at.astimezone(REPORT_TZ).date().isoformat()
    load_id, objects = ctx.store.load_with_dlt(entity, records, METADATA_COLUMNS, METADATA_LAYOUT, {"ictrl_dt": ictrl_dt})
    counts = {"records": len(records), **{f"status_{s}": sum(r["fetch_status"] == s for r in records) for s in ("ok", "not_returned")}}
    manifest = {
        **ctx.manifest_header(entity, load_id, objects, started_at),
        "ictrl_dt": ictrl_dt,
        "requests": requests_meta,
        "counts": counts,
        **extra,
    }
    manifest = ctx.write_manifest(f"{ctx.store.root}/_manifests/{entity}/ictrl_dt={ictrl_dt}/{ctx.run_id}.json", manifest)
    log_event("ingest", "metadata_complete", run_id=ctx.run_id, entity=entity, manifest=manifest["manifest_path"], **counts)
    return manifest


def run_measurement_metadata(ctx: IngestContext, measurement_ids: list[int]) -> dict[str, Any]:
    """Snapshot metadata measurement. raw_json = nguyên bytes body của GET /measurements/{id}/."""
    started_at = ctx.now()
    with ctx.http() as http:
        responses = [http.get_json(f"measurements/{msm}/", {"format": "json"}) for msm in measurement_ids]
    records, requests_meta = [], []
    for i, (msm, (obj, body, info)) in enumerate(zip(measurement_ids, responses, strict=True)):
        if not isinstance(obj, dict) or obj.get("id") != msm:
            raise ValueError(f"Unexpected metadata payload for measurement {msm}")
        spec = MEASUREMENTS[msm]
        if obj.get("af") != spec.af or obj.get("type") != "ping":
            raise ValueError(f"Measurement {msm} metadata disagrees with config: af={obj.get('af')} type={obj.get('type')}")
        requests_meta.append({**info.as_dict(), "request_index": i})
        records.append(
            metadata_record(ctx, msm, body.decode("utf-8"), info.body_sha256, "http_body", obj.get("is_public"), info.fetched_at, i)
        )
    return stage_metadata(ctx, "measurement_metadata_raw", records, requests_meta, started_at)


def run_probe_metadata(ctx: IngestContext, probe_ids: list[int] | None = None, page_size: int = 500) -> dict[str, Any]:
    """Snapshot metadata probe theo id__in, KHÔNG lọc is_public để ghi nhận cả probe đã chuyển private.

    raw_json là JSON canonical (sort_keys) của từng phần tử trong trang `results`, vì API trả về một
    trang chứa nhiều probe; SHA-256 của nguyên body từng trang nằm trong manifest.requests.
    Probe được yêu cầu nhưng không có trong response: record fetch_status=not_returned.
    """
    started_at = ctx.now()
    wanted = sorted(set(probe_ids if probe_ids is not None else ctx.probe_set.all_probes))
    records: list[dict[str, Any]] = []
    requests_meta: list[dict[str, Any]] = []
    found: set[int] = set()
    with ctx.http() as http:
        pages = list(iter_probe_pages(http, wanted, page_size))
    for request_index, (requested, data, info) in enumerate(pages):
        requests_meta.append({**info.as_dict(), "request_index": request_index, "returned": len(data["results"])})
        for probe in data["results"]:
            pid = probe.get("id") if isinstance(probe, dict) else None
            if pid not in requested:
                raise ValueError(f"API returned probe {pid} that was not requested (filter ignored?)")
            found.add(pid)
            text = canonical_json(probe)
            sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
            records.append(metadata_record(ctx, pid, text, sha, "canonical_json", probe.get("is_public"), info.fetched_at, request_index))
    observed = ctx.now().isoformat()
    for pid in sorted(set(wanted) - found):
        records.append(metadata_record(ctx, pid, None, None, "none", None, observed, -1, fetch_status="not_returned"))
    return stage_metadata(ctx, "probe_metadata_raw", records, requests_meta, started_at, probes_requested=len(wanted))
