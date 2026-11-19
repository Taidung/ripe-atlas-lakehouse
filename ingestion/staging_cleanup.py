"""Dọn object mồ côi trong staging: object dữ liệu do dlt ghi nhưng không manifest hoàn tất nào tham chiếu
(load thất bại sau khi ghi object, hoặc chết trước khi ghi manifest). Mặc định chỉ liệt kê (dry-run); `--apply` mới xóa.

Chỉ xóa object đủ cũ (mặc định > 48 giờ) để không đụng vào job đang ghi dở nhưng sắp ghi manifest.
Không đụng vào manifest, hash index, probe set, hay thư mục nội bộ của dlt (_dlt_*).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from ingestion.logs import log_event
from ingestion.staging_store import StagingStore

DATA_PREFIXES = ("ping_result_raw", "probe_metadata_raw", "measurement_metadata_raw")


def find_orphans(store: StagingStore, min_age: timedelta, now: datetime | None = None) -> tuple[list[str], int]:
    now = now or datetime.now(timezone.utc)
    referenced: set[str] = set()
    manifests = store.glob(f"{store.root}/_manifests/**/*.json")
    for path in manifests:
        doc = store.read_json(path)
        if doc.get("status") == "complete":
            referenced.update(o["path"] for o in doc.get("objects", []))
    orphans = []
    for prefix in DATA_PREFIXES:
        for path in store.glob(f"{store.root}/{prefix}/**/*.gz"):
            if path in referenced:
                continue
            info = store.fs.info(path)
            modified = info.get("LastModified") or info.get("mtime")
            if isinstance(modified, (int, float)):
                modified = datetime.fromtimestamp(modified, timezone.utc)
            if modified is not None and now - modified >= min_age:
                orphans.append(path)
    return sorted(orphans), len(manifests)


def run_cleanup(store: StagingStore, min_age_hours: int, apply: bool, now: datetime | None = None) -> list[str]:
    orphans, manifests = find_orphans(store, timedelta(hours=min_age_hours), now)
    for path in orphans:
        log_event("cleanup", "orphan_deleted" if apply else "orphan_found", path=path)
        if apply:
            store.fs.rm(path)
    log_event("cleanup", "finished", manifests_scanned=manifests, orphans=len(orphans), applied=apply, min_age_hours=min_age_hours)
    url = os.environ.get("PUSHGATEWAY_URL")
    if url:
        from prometheus_client import CollectorRegistry, Gauge

        from ingestion.metrics import push_safely

        reg = CollectorRegistry()
        Gauge("ripe_staging_orphan_objects", "Object staging mồ côi tìm thấy lần chạy gần nhất", ["applied"], registry=reg).labels(
            str(apply).lower()
        ).set(len(orphans))
        push_safely(url, {"stage": "staging_cleanup"}, reg)
    return orphans
