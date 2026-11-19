"""Structured log: mỗi sự kiện là một dòng JSON trên stderr.

Trường bắt buộc: ts, stage, event. Định danh có cardinality cao (run_id, object path)
nằm trong log thay vì metric label.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any


def log_event(stage: str, event: str, **fields: Any) -> None:
    record = {"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"), "stage": stage, "event": event}
    record.update(fields)
    sys.stderr.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
    sys.stderr.flush()
