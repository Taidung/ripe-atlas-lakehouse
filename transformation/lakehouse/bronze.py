"""Staging -> Bronze. Runner chọn manifest, dbt (trên Trino) đọc file staging, kiểm tra và ghi.

  python -m lakehouse bronze --entity ping_result_raw --start 2026-11-19T00:00:00Z --end 2026-11-19T03:00:00Z
  python -m lakehouse bronze --entity metadata

Luồng:
  1. Liệt kê manifest status=complete trong phạm vi (giờ của --start/--end; metadata: --lookback-days ngày gần nhất).
  2. Bỏ manifest đã có trong bronze.ingest_manifest (bronze_status=loaded), trừ khi --reprocess.
  3. Ghi danh sách object của manifest đang chờ vào work.bronze_pending_object; đăng ký partition staging tương ứng.
  4. dbt build --select tag:bronze_<entity>: bảng kiểm tra tạm -> test lô (số dòng, load_id, tỷ lệ quarantine)
     -> bronze.<entity> (chỉ chèn record_uid chưa có) và bronze.quarantine. Test lỗi -> không ghi gì, job thất bại.
  5. Ghi nhật ký bronze.ingest_manifest (một dòng mỗi manifest, append-only).

Idempotency: chèn theo record_uid chưa có, nên xử lý lại cùng manifest không sinh dòng mới. Nếu job chết giữa (4) và (5),
manifest chưa được đánh dấu nên lần sau xử lý lại.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from typing import Any

from lakehouse.common import (
    REPORT_ZONE,
    STAGING_ROOT,
    days_for_range,
    iso_z,
    log_event,
    new_run_id,
    parse_instant,
    report_today,
    sql_str,
    utc_now,
)
from lakehouse.dbt_runner import dbt
from lakehouse.trino_db import Trino, insert_rows

RESULTS = "ping_result_raw"
METADATA_ENTITIES = {"probe_metadata_raw": "probe_id", "measurement_metadata_raw": "measurement_id"}
MANIFEST_TABLE = "bronze.ingest_manifest"
PENDING_TABLE = "work.bronze_pending_object"
ROW_KINDS = ("read", "inserted", "already_present", "quarantined")
PENDING_COLUMNS = [
    "entity",
    "manifest_path",
    "source_object",
    "window_date",
    "window_hour",
    "msm_id",
    "part_ictrl_dt",
    "expected_measurement_id",
    "expected_af",
    "window_start_ts",
    "window_end_ts",
    "ictrl_dt",
    "probe_set_version",
    "expected_load_id",
    "manifest_staged_rows",
]
AUDIT_COLUMNS = [
    "manifest_path",
    "entity",
    "ingest_run_id",
    "run_type",
    "load_id",
    "measurement_id",
    "window_start",
    "window_end",
    "ictrl_dt",
    "probe_set_version",
    "probes_requested",
    "request_count",
    "fetched",
    "staged",
    "unchanged",
    "changed",
    "object_count",
    "object_bytes",
    "rows_read",
    "rows_valid",
    "rows_inserted",
    "rows_already_present",
    "rows_quarantined",
    "ingest_started_at",
    "ingest_finished_at",
    "bronze_status",
    "bronze_run_id",
    "bronze_loaded_at",
    "manifest_json",
]


# ---------------------------------------------------------------------------- manifest
def manifest_globs(entity: str, start: datetime | None, end: datetime | None, lookback_days: int) -> list[str]:
    """Glob manifest cần xét.

    - results: từng giờ (giờ VN) trong [start, end).
    - metadata: các ngày (giờ VN) trong [start, end) nếu có, ngược lại `lookback_days` ngày gần nhất tính tới hôm nay.
      Không quét toàn bộ lịch sử: chi phí mỗi lần chạy không tăng theo số ngày đã ingest.
    """
    if entity != RESULTS:
        days = days_for_range(start, end) if start and end else [report_today() - timedelta(days=i) for i in range(lookback_days)]
        return [f"{STAGING_ROOT}/_manifests/{entity}/ictrl_dt={d}/*.json" for d in sorted(days)]
    globs, cursor = [], start
    while cursor < end:
        local = cursor.astimezone(REPORT_ZONE)
        globs.append(f"{STAGING_ROOT}/_manifests/{RESULTS}/window_date={local:%Y-%m-%d}/window_hour={local:%H}/msm_id=*/*.json")
        cursor += timedelta(hours=1)
    return globs


def read_manifests(fs, globs: list[str]) -> list[dict]:
    manifests = []
    for pattern in globs:
        for path in fs.glob(pattern.removeprefix("s3://")):
            text = fs.cat_file(path).decode("utf-8")
            doc = json.loads(text)
            if doc.get("status") == "complete":
                manifests.append({**doc, "manifest_path": "s3://" + path.removeprefix("s3://"), "_raw": text})
    return sorted(manifests, key=lambda m: m["manifest_path"])


def pending(db: Trino, manifests: list[dict], reprocess: bool) -> list[dict]:
    if reprocess or not manifests:
        return manifests
    # Lọc theo partition (entity, ictrl_dt) trước để Iceberg chỉ đọc vài file thay vì cả bảng nhật ký.
    entities = ", ".join(sql_str(e) for e in sorted({m["entity"] for m in manifests}))
    days = ", ".join(f"DATE '{d}'" for d in sorted({m["ictrl_dt"] for m in manifests}))
    paths = ", ".join(sql_str(m["manifest_path"]) for m in manifests)
    loaded = {
        r[0]
        for r in db.query(
            f"SELECT DISTINCT manifest_path FROM {MANIFEST_TABLE} "
            f"WHERE entity IN ({entities}) AND ictrl_dt IN ({days}) AND bronze_status = 'loaded' AND manifest_path IN ({paths})"
        )
    }
    return [m for m in manifests if m["manifest_path"] not in loaded]


# ---------------------------------------------------------------------------- object đang chờ
def object_partition(path: str) -> dict[str, str]:
    """'s3://staging/ripe_atlas/ping_result_raw/window_date=2026-11-21/window_hour=07/msm_id=1001/x.jsonl.gz'
    -> {'window_date': '2026-11-21', 'window_hour': '07', 'msm_id': '1001'}"""
    return dict(re.findall(r"/(\w+)=([^/]+)", path))


def _epoch_or_none(value: str | None) -> int | None:
    return int(parse_instant(value).timestamp()) if value else None


def _instant_or_none(value: str | None) -> datetime | None:
    return parse_instant(value) if value else None


def staged_rows(m: dict) -> int:
    counts = m["counts"]
    return counts.get("staged", counts.get("records", 0))


def pending_rows(entity: str, manifests: list[dict]) -> list[tuple]:
    rows = []
    for m in manifests:
        for obj in m["objects"]:
            part = object_partition(obj["path"])
            rows.append(
                (
                    entity,
                    m["manifest_path"],
                    obj["path"],
                    part.get("window_date"),
                    part.get("window_hour"),
                    part.get("msm_id"),
                    part.get("ictrl_dt"),
                    m.get("measurement_id"),
                    m.get("af"),
                    _epoch_or_none(m.get("window_start")),
                    _epoch_or_none(m.get("window_end")),
                    datetime.fromisoformat(m["ictrl_dt"]).date(),
                    m.get("probe_set_version") or "unknown",
                    m.get("load_id"),
                    staged_rows(m),
                )
            )
    return rows


def register_partitions(db: Trino, entity: str, manifests: list[dict]) -> int:
    """Đăng ký (idempotent) partition staging của các object đang chờ vào metastore của catalog `staging`."""
    partitions = {tuple(object_partition(o["path"]).items()) for m in manifests for o in m["objects"]}
    registered = 0
    for part in sorted(partitions):
        columns, values = zip(*part, strict=True)
        try:
            db.execute(
                f"CALL staging.system.register_partition('ripe_atlas', '{entity}', "
                f"ARRAY[{', '.join(map(sql_str, columns))}], ARRAY[{', '.join(map(sql_str, values))}])"
            )
            registered += 1
        except Exception as exc:  # partition đã có từ lần chạy trước
            if getattr(exc, "error_name", None) != "ALREADY_EXISTS":
                raise
    return registered


# ---------------------------------------------------------------------------- ghi
def row_stats(db: Trino, entity: str, manifests: list[dict], run_id: str) -> dict[str, dict]:
    """manifest_path -> số dòng read / valid / inserted / already_present / quarantined của lần chạy này."""
    checked = {
        r["manifest_path"]: r
        for r in db.dicts(
            f"SELECT manifest_path, count(*) AS read, count_if(reason IS NULL AND dedup_rank = 1) AS valid, "
            f"count(DISTINCT CASE WHEN reason IS NOT NULL THEN quarantine_uid END) AS quarantined "
            f"FROM work.bronze_checked_{entity} GROUP BY 1"
        )
    }
    days = ", ".join(f"DATE '{d}'" for d in sorted({m["ictrl_dt"] for m in manifests}))
    inserted = {
        r[0]: r[1]
        for r in db.query(
            f"SELECT manifest_path, count(*) FROM bronze.{entity} "
            f"WHERE ictrl_dt IN ({days}) AND bronze_run_id = {sql_str(run_id)} GROUP BY 1"
        )
    }
    stats = {}
    for m in manifests:
        path = m["manifest_path"]
        c = checked.get(path, {})
        n_valid, n_inserted = c.get("valid", 0), inserted.get(path, 0)
        stats[path] = {
            "read": c.get("read", 0),
            "valid": n_valid,
            "inserted": n_inserted,
            "already_present": n_valid - n_inserted,
            "quarantined": c.get("quarantined", 0),
        }
    return stats


def audit_rows(manifests: list[dict], stats: dict[str, dict], run_id: str, loaded_at: datetime) -> list[tuple]:
    rows = []
    for m in manifests:
        counts, s = m["counts"], stats.get(m["manifest_path"], {})
        rows.append(
            (
                m["manifest_path"],
                m["entity"],
                m["run_id"],
                m.get("run_type"),
                m.get("load_id"),
                m.get("measurement_id"),
                _instant_or_none(m.get("window_start")),
                _instant_or_none(m.get("window_end")),
                datetime.fromisoformat(m["ictrl_dt"]).date(),
                m.get("probe_set_version"),
                m.get("probes_requested"),
                len(m.get("requests", [])),
                counts.get("fetched", counts.get("records")),
                counts.get("staged", counts.get("records")),
                counts.get("unchanged"),
                counts.get("changed"),
                len(m["objects"]),
                sum(o["bytes"] for o in m["objects"]),
                *(s.get(k, 0) for k in ("read", "valid", "inserted", "already_present", "quarantined")),
                parse_instant(m["started_at"]),
                parse_instant(m["finished_at"]),
                "loaded",
                run_id,
                loaded_at,
                m["_raw"],
            )
        )
    return rows


def process(db: Trino, entity: str, manifests: list[dict], max_quarantine_ratio: float) -> dict[str, Any]:
    run_id = new_run_id("bronze")
    loaded_at = utc_now()
    with_objects = [m for m in manifests if m["objects"]]
    stats: dict[str, dict] = {}
    if with_objects:
        rows = pending_rows(entity, with_objects)
        db.execute(f"DELETE FROM {PENDING_TABLE}")
        insert_rows(db, PENDING_TABLE, PENDING_COLUMNS, rows)
        registered = register_partitions(db, entity, with_objects)
        log_event("bronze", "pending_objects", entity=entity, manifests=len(with_objects), objects=len(rows), partitions_new=registered)
        run = dbt(
            "build",
            f"tag:bronze_{entity}",
            {"entity": entity, "run_id": run_id, "run_ts": iso_z(loaded_at), "max_quarantine_ratio": max_quarantine_ratio},
        )
        if not run.success:
            failed = [f"{n.name}: {n.status}" for n in run.nodes if n.status in ("error", "fail")]
            raise RuntimeError(f"Bronze {entity}: kiểm tra lô thất bại hoặc dbt lỗi, không ghi gì: {failed}")
        stats = row_stats(db, entity, with_objects, run_id)

    # Audit append-only: mỗi lần xử lý một manifest là một dòng (khóa manifest_path + bronze_run_id),
    # nên --reprocess không ghi đè lịch sử của lần nạp đầu.
    insert_rows(db, MANIFEST_TABLE, AUDIT_COLUMNS, audit_rows(manifests, stats, run_id, loaded_at), chunk=50)
    summary = {
        "entity": entity,
        "bronze_run_id": run_id,
        "manifests": len(manifests),
        **{f"rows_{kind}": sum(s[kind] for s in stats.values()) for kind in ROW_KINDS},
        "snapshot_id": db.latest_snapshot(f"bronze.{entity}") if with_objects else None,
    }
    log_event("bronze", "batch_loaded", **summary)
    return summary


def staging_fs():
    import s3fs  # chỉ có trong image transform

    return s3fs.S3FileSystem(
        key=os.environ["S3_PROCESSING_ACCESS_KEY"],
        secret=os.environ["S3_PROCESSING_SECRET_KEY"],
        client_kwargs={"endpoint_url": os.environ.get("S3_ENDPOINT", "http://s3:8333"), "region_name": "us-east-1"},
    )


def run(args, m) -> int:
    from lakehouse.common import SQL_DIR

    db = Trino.connect()
    db.run_file(SQL_DIR / "ops.sql")
    fs = staging_fs()
    entities = list(METADATA_ENTITIES) if args.entity == "metadata" else [args.entity]
    for entity in entities:
        todo = pending(db, read_manifests(fs, manifest_globs(entity, args.start, args.end, args.lookback_days)), args.reprocess)
        if todo:
            summary = process(db, entity, todo, args.max_quarantine_ratio)
        else:
            summary = {}
            log_event("bronze", "nothing_pending", entity=entity)
        m.set("ripe_bronze_manifests", "Manifest được xử lý, lần chạy gần nhất", len(todo), entity=entity)
        for kind in ROW_KINDS:
            m.set(
                "ripe_bronze_rows", "Số dòng Bronze theo loại, lần chạy gần nhất", summary.get(f"rows_{kind}", 0), entity=entity, kind=kind
            )
    return 0


def add_args(parser) -> None:
    parser.add_argument("--entity", required=True, choices=[RESULTS, "metadata", *METADATA_ENTITIES])
    parser.add_argument("--start", type=parse_instant)
    parser.add_argument("--end", type=parse_instant)
    parser.add_argument("--reprocess", action="store_true", help="Xử lý lại cả manifest đã loaded (vẫn idempotent)")
    parser.add_argument("--max-quarantine-ratio", type=float, default=0.05)
    parser.add_argument(
        "--lookback-days", type=int, default=3, help="metadata: số ngày manifest gần nhất được xét khi không có --start/--end (mặc định 3)"
    )


def validate(parser, args) -> None:
    if args.entity == RESULTS and not (args.start and args.end):
        parser.error("--start and --end are required for ping_result_raw")


def stage_name(args) -> str:
    return "bronze_results" if args.entity == RESULTS else "bronze_metadata"
