"""Gọi dbt (dbtRunner, cùng tiến trình) với vars của runner và rút gọn kết quả từng node.

Bronze: một lần `dbt build`; test lô gắn vào bảng kiểm tra tạm nên lỗi -> bảng Bronze bị SKIP, không ghi gì.
Silver/Gold: `dbt build` bản nháp + test -> runner đọc kết quả, quyết định -> `dbt run` bảng công bố (hai bước tách rõ).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from lakehouse.common import DBT_DIR, log_event


@dataclass
class NodeResult:
    name: str
    resource_type: str  # model | test | seed
    status: str  # success | error | fail | warn | pass | skipped
    severity: str | None  # test: error | warn
    failures: int | None
    message: str | None
    attached_to: str | None  # test: model mà test gắn vào


@dataclass
class DbtRun:
    success: bool
    nodes: list[NodeResult]

    def tests(self) -> list[NodeResult]:
        return [n for n in self.nodes if n.resource_type == "test"]

    def errors(self) -> list[NodeResult]:
        """Node lỗi do SQL/hạ tầng (không phải test fail): job phải thất bại, không phải 'blocked'."""
        return [n for n in self.nodes if n.status == "error"]


def _node_result(r: Any) -> NodeResult:
    node = r.node
    attached = getattr(node, "attached_node", None)
    return NodeResult(
        name=node.name,
        resource_type=str(node.resource_type),
        status=str(r.status),
        severity=str(getattr(node.config, "severity", "") or "").lower() or None,
        failures=getattr(r, "failures", None),
        message=r.message if isinstance(r.message, str) and r.message else None,
        attached_to=attached.split(".")[-1] if attached else None,
    )


def dbt(command: str, select: str, variables: dict[str, Any]) -> DbtRun:
    """Chạy `dbt <command> --select <select> --vars <json>`; ném lỗi nếu dbt không chạy được (parse, kết nối)."""
    from dbt.cli.main import dbtRunner

    args = [
        command,
        *("--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)),
        *("--select", select, "--vars", json.dumps(variables, default=str)),
        # Chỉ chạy test mà MỌI model cha đều được chọn (cộng test được chọn trực tiếp qua tag): test của tầng sau
        # tham chiếu bảng tầng trước không bị kéo theo khi chạy tầng trước.
        *("--indirect-selection", "cautious"),
    ]
    log_event("dbt", "invoke", command=command, select=select, vars=variables)
    res = dbtRunner().invoke(args)
    if res.exception is not None:
        raise RuntimeError(f"dbt {command} --select {select} failed to run: {res.exception}")
    nodes = [_node_result(r) for r in (res.result or [])]
    log_event("dbt", "finished", command=command, select=select, success=res.success, nodes={n.name: n.status for n in nodes})
    return DbtRun(success=bool(res.success), nodes=nodes)
