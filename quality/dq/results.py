"""Kiểu kết quả kiểm tra chất lượng dùng chung cho GX và kiểm tra Spark.

Mỗi kiểm tra là một dict:
  {check_name, engine, severity, success, observed_value, unexpected_count, element_count, details}
"""

from __future__ import annotations

import json
from typing import Any

CRITICAL = "critical"
WARN = "warn"

CheckResult = dict[str, Any]


def make_check(name: str, engine: str, severity: str, success: bool, observed: Any = None,
               unexpected: int | None = None, elements: int | None = None, details: Any = None) -> CheckResult:
    return {
        "check_name": name,
        "engine": engine,
        "severity": severity,
        "success": bool(success),
        "observed_value": None if observed is None else json.dumps(observed, default=str),
        "unexpected_count": None if unexpected is None else int(unexpected),
        "element_count": None if elements is None else int(elements),
        "details": None if details is None else json.dumps(details, default=str)[:4000],
    }


def has_critical_failure(checks: list[CheckResult]) -> bool:
    return any(c["severity"] == CRITICAL and not c["success"] for c in checks)


def has_warning(checks: list[CheckResult]) -> bool:
    return any(c["severity"] == WARN and not c["success"] for c in checks)


def failed_names(checks: list[CheckResult], severity: str | None = None) -> list[str]:
    return [c["check_name"] for c in checks if not c["success"] and (severity is None or c["severity"] == severity)]
