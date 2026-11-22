"""CLI ingestion. Ví dụ:

  python -m ingestion metadata
  python -m ingestion results --start 2026-11-18T06:00:00Z --end 2026-11-18T08:00:00Z
  python -m ingestion incremental                 # giờ vừa kết thúc + OVERLAP_HOURS giờ trước
  python -m ingestion backfill --start ... --end ...

Mã thoát khác 0 nếu bất kỳ job (measurement, cửa sổ) nào thất bại; các job thành công vẫn giữ
manifest, nên chạy lại chỉ làm lại phần thiếu (và phần chồng lấn).
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from ingestion import metrics
from ingestion.jobs import IngestContext, new_run_id, run_measurement_metadata, run_probe_metadata, run_results_window
from ingestion.logs import log_event
from ingestion.probe_sets import load_probe_set
from ingestion.settings import HttpSettings, IngestSettings, StagingSettings, parse_measurement_ids
from ingestion.staging_store import StagingStore
from ingestion.time_windows import ensure_completed, hourly_windows, incremental_windows, parse_instant

# Loại lần chạy (nhãn metric và trường run_type của manifest). Cùng tập giá trị với transformation/lakehouse/common.py.
RUN_TYPES = ("scheduled", "backfill", "reconcile", "repair", "manual")
# Mặc định khi không truyền --run-type. Lịch chạy (Airflow) luôn truyền rõ; chạy tay không bao giờ mang nhãn scheduled.
DEFAULT_RUN_TYPE = {"metadata": "manual", "results": "manual", "backfill": "backfill", "incremental": "scheduled"}


def make_context(args: argparse.Namespace, run_type: str) -> IngestContext:
    settings = IngestSettings.from_env()
    if args.probe_set:
        settings = dataclasses.replace(settings, probe_set_version=args.probe_set)
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
                log_event(
                    "ingest",
                    "window_failed",
                    run_id=ctx.run_id,
                    measurement_id=msm,
                    window=window.label,
                    error=type(exc).__name__,
                    message=str(exc),
                )
    log_event(
        "ingest", "run_finished", run_id=ctx.run_id, run_type=ctx.run_type, jobs=len(windows) * len(measurement_ids), failures=failures
    )
    metrics.push_ingest_metrics(ctx.run_type, started, failures == 0, per_msm, status)
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ingestion")
    sub = parser.add_subparsers(dest="command", required=True)
    # Tham số chung của các lệnh gọi API theo measurement / probe set.
    scope = argparse.ArgumentParser(add_help=False)
    scope.add_argument("--measurements", help="Danh sách measurement_id, phân tách bằng dấu phẩy (mặc định: cả 4)")
    scope.add_argument("--probe-set", help="probe_set_version (mặc định: biến PROBE_SET_VERSION)")
    scope.add_argument(
        "--run-type",
        choices=RUN_TYPES,
        help="Nhãn metric của lần chạy (mặc định: incremental=scheduled, backfill=backfill, còn lại manual)",
    )

    sub.add_parser("metadata", parents=[scope], help="Snapshot metadata measurement + probe")

    for name in ("results", "backfill"):
        p = sub.add_parser(name, parents=[scope], help="Ingest ping results cho [start, end) theo từng giờ")
        p.add_argument("--start", required=True, type=parse_instant)
        p.add_argument("--end", required=True, type=parse_instant)

    inc = sub.add_parser("incremental", parents=[scope], help="Giờ vừa kết thúc + OVERLAP_HOURS giờ chồng lấn")
    inc.add_argument("--now", type=parse_instant, help="Mốc thời gian logic (mặc định: hiện tại)")

    cln = sub.add_parser("cleanup", help="Liệt kê/xóa object staging mồ côi (không manifest nào tham chiếu)")
    cln.add_argument("--orphan-min-age-hours", type=int, default=48)
    cln.add_argument("--apply", action="store_true", help="Xóa thật; mặc định chỉ liệt kê")

    args = parser.parse_args(argv)

    if args.command == "cleanup":
        from ingestion.staging_cleanup import run_cleanup

        run_cleanup(StagingStore.from_settings(StagingSettings.from_env()), args.orphan_min_age_hours, args.apply)
        return 0

    run_type = args.run_type or DEFAULT_RUN_TYPE[args.command]
    if args.command == "metadata":
        ctx = make_context(args, run_type)
        ok = False
        try:
            run_measurement_metadata(ctx, parse_measurement_ids(args.measurements))
            run_probe_metadata(ctx)
            ok = True
        finally:
            ctx.close()
            url = os.environ.get("PUSHGATEWAY_URL")
            if url:
                metrics.push_outcome(url, "metadata", run_type, ok, time.time())
        return 0

    ctx = make_context(args, run_type)
    if args.command == "incremental":
        now = args.now or datetime.now(timezone.utc)
        windows = incremental_windows(now, ctx.settings.overlap_hours)
    else:
        windows = hourly_windows(args.start, args.end)
    try:
        return run_windows(ctx, parse_measurement_ids(args.measurements), windows)
    finally:
        ctx.close()


if __name__ == "__main__":  # cho phép chạy trực tiếp `python ingestion/cli.py`
    sys.exit(main())
