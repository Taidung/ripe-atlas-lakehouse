"""Metric vận hành của ingestion, đẩy lên Pushgateway khi job kết thúc.

Nguyên tắc (https://prometheus.io/docs/practices/pushing/):
- Job batch ngắn -> push một lần cuối run, PUT thay thế toàn bộ group => không còn series cũ.
- Grouping key: job="ripe_pipeline", stage="ingest", run_type (scheduled/backfill/manual) để backfill
  không ghi đè số liệu của lịch chạy chính.
- Label cardinality thấp: measurement_id (4 giá trị), endpoint (3), reason (4), status_class (2xx, 4xx, 5xx,
  network_error, too_large, invalid_body).
  run_id, URL, object path chỉ nằm trong log.
- Không có PUSHGATEWAY_URL hoặc push lỗi: chỉ ghi log cảnh báo, KHÔNG làm job thất bại.
"""

from __future__ import annotations

import os
import threading
import time
from collections import Counter

from ingestion.logs import log_event

_LOCK = threading.Lock()
HTTP_REQUESTS: Counter = Counter()  # (endpoint, status_class) -> số request logic, theo kết quả cuối cùng
HTTP_RETRIES: Counter = Counter()  # (endpoint, reason) -> số lần retry
HTTP_DURATION: Counter = Counter()  # endpoint -> tổng giây
HTTP_BYTES: Counter = Counter()  # endpoint -> tổng bytes


def endpoint_of(path: str) -> str:
    if path.startswith("probes"):
        return "probes"
    return "results" if path.rstrip("/").endswith("results") else "measurements"


def status_class(status: int | None) -> str:
    return "network_error" if status is None else f"{status // 100}xx"


def retry_reason(token: str) -> str:
    if token == "429":
        return "rate_limited_429"
    if token.isdigit():
        return "server_5xx"
    return "network"


def record_http(path: str, status: int | None, duration_s: float, retries: list[str], nbytes: int = 0, outcome: str | None = None) -> None:
    """Ghi một request logic. `outcome` thay cho status_class khi HTTP thành công nhưng body không dùng được."""
    ep = endpoint_of(path)
    with _LOCK:
        HTTP_REQUESTS[(ep, outcome or status_class(status))] += 1
        HTTP_DURATION[ep] += duration_s
        HTTP_BYTES[ep] += nbytes
        for token in retries:
            HTTP_RETRIES[(ep, retry_reason(token))] += 1


def reset() -> None:
    with _LOCK:
        for c in (HTTP_REQUESTS, HTTP_RETRIES, HTTP_DURATION, HTTP_BYTES):
            c.clear()


def push_ingest_metrics(
    run_type: str, started: float, ok: bool, window_counts: dict[int, Counter], window_status: Counter, url: str | None = None
) -> None:
    url = url or os.environ.get("PUSHGATEWAY_URL")
    if not url:
        return
    from prometheus_client import CollectorRegistry, Gauge

    reg = CollectorRegistry()
    labels = ["stage", "run_type"]
    now = time.time()
    Gauge("ripe_job_duration_seconds", "Thời gian chạy của lần chạy gần nhất", labels, registry=reg).labels("ingest", run_type).set(
        now - started
    )
    Gauge("ripe_job_last_run_success", "1 nếu lần chạy gần nhất thành công", labels, registry=reg).labels("ingest", run_type).set(
        1 if ok else 0
    )
    windows = Gauge(
        "ripe_ingest_windows", "Số job (measurement, cửa sổ) theo trạng thái, lần chạy gần nhất", ["run_type", "status"], registry=reg
    )
    for status, n in window_status.items():
        windows.labels(run_type, status).set(n)
    records = Gauge("ripe_ingest_records", "Số result theo loại, lần chạy gần nhất", ["run_type", "measurement_id", "kind"], registry=reg)
    for msm, counts in window_counts.items():
        for kind in (
            "fetched",
            "staged",
            "new",
            "changed",
            "unchanged",
            "duplicate_in_batch",
            "status_malformed_json",
            "status_missing_key",
        ):
            records.labels(run_type, str(msm), kind).set(counts.get(kind, 0))
    with _LOCK:
        req = Gauge(
            "ripe_api_requests",
            "Request tới RIPE Atlas API theo kết quả cuối, lần chạy gần nhất",
            ["run_type", "endpoint", "status_class"],
            registry=reg,
        )
        for (ep, sc), n in HTTP_REQUESTS.items():
            req.labels(run_type, ep, sc).set(n)
        ret = Gauge("ripe_api_retries", "Số lần retry theo lý do, lần chạy gần nhất", ["run_type", "endpoint", "reason"], registry=reg)
        for (ep, reason), n in HTTP_RETRIES.items():
            ret.labels(run_type, ep, reason).set(n)
        dur = Gauge(
            "ripe_api_request_duration_seconds_total",
            "Tổng thời gian request (kể cả retry), lần chạy gần nhất",
            ["run_type", "endpoint"],
            registry=reg,
        )
        cnt = Gauge("ripe_api_request_count_total", "Tổng số request, lần chạy gần nhất", ["run_type", "endpoint"], registry=reg)
        byt = Gauge("ripe_api_response_bytes_total", "Tổng bytes body, lần chạy gần nhất", ["run_type", "endpoint"], registry=reg)
        for ep, s in HTTP_DURATION.items():
            dur.labels(run_type, ep).set(s)
            cnt.labels(run_type, ep).set(sum(n for (e, _), n in HTTP_REQUESTS.items() if e == ep))
            byt.labels(run_type, ep).set(HTTP_BYTES[ep])
    push_safely(url, {"stage": "ingest", "run_type": run_type}, reg)
    push_outcome(url, "ingest", run_type, ok, now)


def push_safely(url: str, grouping_key: dict[str, str], registry) -> None:
    from prometheus_client import push_to_gateway

    try:
        push_to_gateway(url, job="ripe_pipeline", grouping_key=grouping_key, registry=registry)
    except Exception as exc:  # noqa: BLE001 - metric không được làm hỏng job
        log_event("metrics", "push_failed", grouping_key=grouping_key, error=str(exc))


def push_outcome(url: str, stage: str, run_type: str, ok: bool, when: float) -> None:
    """Timestamp thành công/thất bại ở group riêng (outcome=...), nên lần chạy lỗi không xóa mốc thành công."""
    from prometheus_client import CollectorRegistry, Gauge

    reg = CollectorRegistry()
    name = "ripe_job_last_success_timestamp_seconds" if ok else "ripe_job_last_failure_timestamp_seconds"
    Gauge(name, "Unix time của lần chạy thành công/thất bại gần nhất", ["stage", "run_type"], registry=reg).labels(stage, run_type).set(
        when
    )
    push_safely(url, {"stage": stage, "run_type": run_type, "outcome": "success" if ok else "failure"}, reg)
