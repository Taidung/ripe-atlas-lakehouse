"""Gọi dbt (dbtRunner, cùng tiến trình) với vars của runner và rút gọn kết quả từng node.

dbt `build` chạy model rồi test của nó; test mức error thất bại -> các model phía sau bị SKIP, nên bảng đích
(Bronze/Silver/Gold) không bị ghi. Runner đọc kết quả để ghi ops.dq_result và quyết định trạng thái công bố.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from lakehouse.common import DBT_DIR, log_event


@dataclass
class NodeResult:
    unique_id: str
    name: str
    resource_type: str  # model | test | seed
    status: str  # success | error | fail | warn | pass | skipped
    severity: str | None  # test: error | warn
    failures: int | None
    message: str | None
    attached_to: str | None  # test: model mà test gắn vào

    @property
    def is_test(self) -> bool:
        return self.resource_type == "test"


@dataclass
class DbtRun:
    success: bool
    nodes: list[NodeResult]

    def tests(self) -> list[NodeResult]:
        return [n for n in self.nodes if n.is_test]

    def errors(self) -> list[NodeResult]:
        """Node lỗi do SQL/hạ tầng (không phải test fail): job phải thất bại, không phải 'blocked'."""
        return [n for n in self.nodes if n.status == "error"]


def _node_result(r: Any) -> NodeResult:
    node = r.node
    config = getattr(node, "config", None)
    attached = getattr(node, "attached_node", None) or None
    return NodeResult(
        unique_id=node.unique_id,
        name=node.name,
        resource_type=str(node.resource_type),
        status=str(r.status),
        severity=str(getattr(config, "severity", "") or "").lower() or None,
        failures=getattr(r, "failures", None),
        message=(r.message or None) if isinstance(r.message, str) else None,
        attached_to=attached.split(".")[-1] if attached else None,
    )


def dbt(command: str, select: str, variables: dict[str, Any], *extra: str) -> DbtRun:
    """Chạy `dbt <command> --select <select> --vars <json>`; ném lỗi nếu dbt không chạy được (parse, kết nối)."""
    from dbt.cli.main import dbtRunner

    args = [
        command,
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--select",
        select,
        "--vars",
        json.dumps(variables, default=str),
        # Chỉ chạy test mà MỌI model cha đều được chọn (cộng test được chọn trực tiếp qua tag): test của tầng sau
        # tham chiếu bảng tầng trước không bị kéo theo khi chạy tầng trước.
        "--indirect-selection",
        "cautious",
        *extra,
    ]
    log_event("dbt", "invoke", command=command, select=select, vars=variables)
    res = dbtRunner().invoke(args)
    if res.exception is not None:
        raise RuntimeError(f"dbt {command} --select {select} failed to run: {res.exception}")
    nodes = [_node_result(r) for r in (res.result or [])]
    summary = {n.name: n.status for n in nodes}
    log_event("dbt", "finished", command=command, select=select, success=res.success, nodes=summary)
    return DbtRun(success=bool(res.success), nodes=nodes)
