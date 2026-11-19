"""CLI ingestion. Ví dụ:

  python -m ingestion metadata
  python -m ingestion results --start 2026-11-18T06:00:00Z --end 2026-11-18T08:00:00Z
  python -m ingestion incremental                 # giờ vừa kết thúc + OVERLAP_HOURS giờ trước
  python -m ingestion backfill --start ... --end ...
  python -m ingestion build-probe-set --version strat-k2-c100-v1 --k 2 --cap 100 --reference-end 2026-11-19T00:00:00Z

Mã thoát khác 0 nếu bất kỳ job (measurement, cửa sổ) nào thất bại; các job thành công vẫn giữ
manifest, nên chạy lại chỉ làm lại phần thiếu (và phần chồng lấn).
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from ingestion import metrics, probe_sampling
from ingestion.jobs import IngestContext, new_run_id, run_measurement_metadata, run_probe_metadata, run_results_window
from ingestion.logs import log_event
from ingestion.probe_sets import load_probe_set
from ingestion.settings import HttpSettings, IngestSettings, StagingSettings, parse_measurement_ids
from ingestion.staging_store import StagingStore
from ingestion.time_windows import ensure_completed, hourly_windows, incremental_windows, parse_instant


def make_context(args: argparse.Namespace, run_type: str) -> IngestContext:
    settings = IngestSettings.from_env()
    if getattr(args, "probe_set", None):
        settings = IngestSettings(args.probe_set, settings.probe_chunk_size, settings.overlap_hours)
    store = StagingStore.from_settings(StagingSettings.from_env())
    probe_set = load_probe_set(settings.probe_set_version)
    ctx = IngestContext(store, probe_set, settings, HttpSettings.from_env(), new_run_id(), run_type)
    snapshot = f"{store.root}/_probe_sets/{probe_set.version}.json"
    if not store.fs.exists(snapshot):
        store.write_json(snapshot, probe_set.document)
    return ctx


def run_windows(ctx: IngestContext, measurement_ids: list[int], windows) -> int:
    ensure_completed(windows, ctx.now())
    started = time.time()
    metrics.reset()
    failures = 0
    per_msm: dict[int, Counter] = {m: Counter() for m in measurement_ids}
    status: Counter = Counter()
    for window in windows:
        for msm in measurement_ids:
            try:
                manifest = run_results_window(ctx, msm, window)
                per_msm[msm].update({k: v for k, v in manifest["counts"].items() if isinstance(v, int)})
                status["ok"] += 1
            except Exception as exc:  # noqa: BLE001 - ghi nhận rồi tiếp tục các job độc lập
                failures += 1
                status["failed"] += 1
                log_event("ingest", "window_failed", run_id=ctx.run_id, measurement_id=msm, window=window.label,
                          error=type(exc).__name__, message=str(exc))
    log_event("ingest", "run_finished", run_id=ctx.run_id, run_type=ctx.run_type, jobs=len(windows) * len(measurement_ids), failures=failures)
    metrics.push_ingest_metrics(ctx.run_type, started, failures == 0, per_msm, status)
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ingestion")
    sub = parser.add_subparsers(dest="command", required=True)

    meta = sub.add_parser("metadata", help="Snapshot metadata measurement + probe")
    meta.add_argument("--probe-set")
    meta.add_argument("--measurements")

    for name in ("results", "backfill"):
        p = sub.add_parser(name, help="Ingest ping results cho [start, end) theo từng giờ")
        p.add_argument("--start", required=True, type=parse_instant)
        p.add_argument("--end", required=True, type=parse_instant)
        p.add_argument("--measurements")
        p.add_argument("--probe-set")

    inc = sub.add_parser("incremental", help="Giờ vừa kết thúc + OVERLAP_HOURS giờ chồng lấn")
    inc.add_argument("--now", type=parse_instant, help="Mốc thời gian logic (mặc định: hiện tại)")
    inc.add_argument("--measurements")
    inc.add_argument("--probe-set")

    cln = sub.add_parser("cleanup", help="Liệt kê/xóa object staging mồ côi (không manifest nào tham chiếu)")
    cln.add_argument("--orphan-min-age-hours", type=int, default=48)
    cln.add_argument("--apply", action="store_true", help="Xóa thật; mặc định chỉ liệt kê")

    bps = sub.add_parser("build-probe-set", help="Sinh tập probe phân tầng (ADR-0001)")
    bps.add_argument("--version", required=True)
    bps.add_argument("--k", type=int, default=2)
    bps.add_argument("--cap", type=int, default=100)
    bps.add_argument("--seed", default="ripe-atlas-lakehouse")
    bps.add_argument("--reference-end", required=True, type=parse_instant)
    bps.add_argument("--reference-minutes", type=int, default=15)

    args = parser.parse_args(argv)

    if args.command == "build-probe-set":
        probe_sampling.main(args.version, args.k, args.cap, args.seed, args.reference_end, args.reference_minutes)
        return 0

    if args.command == "cleanup":
        from ingestion.staging_cleanup import run_cleanup

        run_cleanup(StagingStore.from_settings(StagingSettings.from_env()), args.orphan_min_age_hours, args.apply)
        return 0

    if args.command == "metadata":
        ctx = make_context(args, "scheduled")
        ok = False
        try:
            run_measurement_metadata(ctx, parse_measurement_ids(args.measurements))
            run_probe_metadata(ctx)
            ok = True
        finally:
            url = metrics.os.environ.get("PUSHGATEWAY_URL")
            if url:
                metrics.push_outcome(url, "metadata", "scheduled", ok, time.time())
        return 0

    run_type = {"results": "manual", "backfill": "backfill", "incremental": "scheduled"}[args.command]
    ctx = make_context(args, run_type)
    if args.command == "incremental":
        now = args.now or datetime.now(timezone.utc)
        windows = incremental_windows(now, ctx.settings.overlap_hours)
    else:
        windows = hourly_windows(args.start, args.end)
    return run_windows(ctx, parse_measurement_ids(args.measurements), windows)


if __name__ == "__main__":  # cho phép chạy trực tiếp `python ingestion/cli.py`
    sys.exit(main())
