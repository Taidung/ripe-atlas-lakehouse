"""Đẩy metric job transform (Bronze/Silver/Gold) lên Pushgateway khi job kết thúc. Cùng quy ước với ingestion/metrics.py:
- group {job=ripe_pipeline, stage, run_type} chứa số liệu lần chạy gần nhất (PUT thay thế toàn bộ group), nên một
  lần backfill hay chạy tay không ghi đè số liệu của lịch chạy chính;
- group {stage, run_type, outcome} chứa mốc thời gian thành công/thất bại, nên lần lỗi không xóa mốc thành công;
- không có PUSHGATEWAY_URL hoặc push lỗi thì chỉ ghi log, không làm job thất bại.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager

from lakehouse.common import log_event

Sample = tuple[dict[str, str], float]
JOB_LABELS = ["stage", "run_type"]


class StageMetrics:
    def __init__(self, stage: str, run_type: str):
        self.stage = stage
        self.run_type = run_type
        self.ok = True  # job đặt False khi kết thúc "không thành công" mà không ném lỗi (vd partition bị chặn)
        self.gauges: dict[str, tuple[str, list[str], list[Sample]]] = {}

    def set(self, name: str, help_text: str, value: float, **labels: str) -> None:
        keys = sorted(labels)
        entry = self.gauges.setdefault(name, (help_text, keys, []))
        entry[2].append(({k: str(labels[k]) for k in keys}, float(value)))

    def push(self, ok: bool, started: float) -> None:
        url = os.environ.get("PUSHGATEWAY_URL")
        if not url:
            return
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        now = time.time()
        job = (self.stage, self.run_type)
        reg = CollectorRegistry()
        Gauge("ripe_job_duration_seconds", "Thời gian chạy lần gần nhất", JOB_LABELS, registry=reg).labels(*job).set(now - started)
        Gauge("ripe_job_last_run_success", "1 nếu lần chạy gần nhất thành công", JOB_LABELS, registry=reg).labels(*job).set(1 if ok else 0)
        for name, (help_text, keys, samples) in self.gauges.items():
            g = Gauge(name, help_text, keys, registry=reg)
            for labels, value in samples:
                (g.labels(**labels) if keys else g).set(value)

        outcome = CollectorRegistry()
        name = "ripe_job_last_success_timestamp_seconds" if ok else "ripe_job_last_failure_timestamp_seconds"
        Gauge(name, "Unix time lần chạy thành công/thất bại gần nhất", JOB_LABELS, registry=outcome).labels(*job).set(now)

        group = {"stage": self.stage, "run_type": self.run_type}
        for key, registry in ((group, reg), ({**group, "outcome": "success" if ok else "failure"}, outcome)):
            try:
                push_to_gateway(url, job="ripe_pipeline", grouping_key=key, registry=registry)
            except Exception as exc:  # noqa: BLE001 - metric không được làm hỏng job
                log_event("metrics", "push_failed", grouping_key=key, error=str(exc))


@contextmanager
def job_metrics(stage: str, run_type: str) -> Iterator[StageMetrics]:
    """Bao quanh main() của job: push metric kèm trạng thái kể cả khi job ném lỗi hoặc thoát mã khác 0."""
    started = time.time()
    m = StageMetrics(stage, run_type)
    ok = False
    try:
        yield m
        ok = m.ok
    finally:
        m.push(ok, started)
