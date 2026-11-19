"""Đưa metadata vào OpenMetadata 2.0.8 và kiểm tra kết quả.

  docker compose --profile governance run --rm om-ingestion

1. Lấy JWT của ingestion-bot (đăng nhập admin mặc định của OM local).
2. Connector Trino (bảng/cột bronze, silver, gold, ops) và Superset (dashboard, chart, lineage dataset -> bảng).
3. Làm giàu từ catalog.yaml: mô tả bảng/cột, owner team, classification DataLayer, glossary metric (metrics.json).
4. Lineage khai báo: container staging -> Bronze -> Silver -> Gold (+ column-level cho cột chính).
5. Đọc lại lineage từ OM và in số cạnh thực có, làm bằng chứng.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from pathlib import Path

import requests
import yaml

HOST = os.environ.get("OM_HOST", "http://om-server:8585")
API = f"{HOST}/api/v1"
ROOT = Path(__file__).resolve().parent
CATALOG = yaml.safe_load((ROOT / "catalog.yaml").read_text(encoding="utf-8"))
METRICS = json.loads(Path("/app/config/metrics.json").read_text(encoding="utf-8"))
TRINO, DB = CATALOG["trino_service"], CATALOG["database"]


class Om:
    def __init__(self):
        for _ in range(60):
            try:
                if requests.get(f"{API}/system/version", timeout=5).ok:
                    break
            except requests.RequestException:
                pass
            time.sleep(5)
        r = requests.post(
            f"{API}/users/login", json={"email": "admin@open-metadata.org", "password": base64.b64encode(b"admin").decode()}, timeout=30
        )
        r.raise_for_status()
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"

    def get(self, path, **params):
        r = self.s.get(f"{API}{path}", params=params, timeout=60)
        return r.json() if r.ok else None

    def put(self, path, body):
        r = self.s.put(f"{API}{path}", json=body, timeout=60)
        if not r.ok:
            raise RuntimeError(f"PUT {path} -> {r.status_code} {r.text[:400]}")
        return r.json() if r.content else {}  # PUT /lineage trả 200 với body rỗng

    def patch(self, path, ops):
        if not ops:
            return None
        r = self.s.patch(f"{API}{path}", data=json.dumps(ops), headers={"Content-Type": "application/json-patch+json"}, timeout=60)
        if not r.ok:
            raise RuntimeError(f"PATCH {path} -> {r.status_code} {r.text[:400]}")
        return r.json()

    def bot_jwt(self) -> str:
        # OM 2.0.8: field authenticationMechanism không còn trên /users; JWT lấy qua /users/auth-mechanism/{id} (đã kiểm chứng).
        bot = self.get("/bots/name/ingestion-bot")
        return self.get(f"/users/auth-mechanism/{bot['botUser']['id']}")["config"]["JWTToken"]


def workflow_config(source: dict, jwt: str) -> dict:
    return {
        "source": source,
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {"hostPort": f"{HOST}/api", "authProvider": "openmetadata", "securityConfig": {"jwtToken": jwt}},
        },
    }


def run_workflow(config: dict) -> None:
    from metadata.workflow.metadata import MetadataWorkflow

    wf = MetadataWorkflow.create(config)
    wf.execute()
    wf.print_status()
    wf.raise_from_status()
    wf.stop()


def trino_source() -> dict:
    return {
        "type": "trino",
        "serviceName": TRINO,
        "serviceConnection": {"config": {"type": "Trino", "hostPort": "trino:8080", "username": "openmetadata", "catalog": DB}},
        "sourceConfig": {
            "config": {
                "type": "DatabaseMetadata",
                "schemaFilterPattern": {"includes": ["^bronze$", "^silver$", "^gold$", "^ops$"]},
                "tableFilterPattern": {"excludes": ["^legacy_.*", "^ping_results$", "^measurements$", "^probes$"]},
            }
        },
    }


def superset_source() -> dict:
    return {
        "type": "superset",
        "serviceName": "superset_dashboards",
        "serviceConnection": {
            "config": {
                "type": "Superset",
                "hostPort": "http://superset:8088",
                "connection": {"provider": "db", "username": "admin", "password": os.environ["SUPERSET_ADMIN_PASSWORD"]},
            }
        },
        "sourceConfig": {"config": {"type": "DashboardMetadata", "lineageInformation": {"dbServicePrefixes": [TRINO]}}},
    }


def table_fqn(name: str) -> str:
    return f"{TRINO}.{DB}.{name}"


def enrich(om: Om) -> dict:
    team = om.put("/teams", {"name": CATALOG["owner_team"], "displayName": "Data Platform", "teamType": "Group"})
    om.put("/classifications", {"name": "DataLayer", "description": "Tầng dữ liệu của lakehouse"})
    for tag, desc in CATALOG["layers"].items():
        om.put("/tags", {"classification": "DataLayer", "name": tag, "description": desc})
    stats = {"tables": 0, "columns": 0, "missing": []}
    for name, spec in CATALOG["tables"].items():
        t = om.get(f"/tables/name/{table_fqn(name)}", fields="columns,owners,tags")
        if not t:
            stats["missing"].append(name)
            continue
        ops = [
            {"op": "add", "path": "/description", "value": spec["description"]},
            {"op": "add", "path": "/owners", "value": [{"id": team["id"], "type": "team"}]},
            {
                "op": "add",
                "path": "/tags",
                "value": [
                    {"tagFQN": f"DataLayer.{spec['layer']}", "source": "Classification", "labelType": "Manual", "state": "Confirmed"}
                ],
            },
        ]
        index = {c["name"]: i for i, c in enumerate(t["columns"])}
        for col, desc in (spec.get("columns") or {}).items():
            if col in index:
                ops.append({"op": "add", "path": f"/columns/{index[col]}/description", "value": desc})
                stats["columns"] += 1
        om.patch(f"/tables/{t['id']}", ops)
        stats["tables"] += 1
    glossary = om.put(
        "/glossaries",
        {
            "name": "RIPE_Atlas_Metrics",
            "displayName": "RIPE Atlas Metrics",
            "description": "Định nghĩa metric dùng trong Gold và Superset (docs/metric-dictionary.md).",
        },
    )
    terms = 0
    for group, metrics in METRICS.items():
        if group.startswith("_"):
            continue
        for name, m in metrics.items():
            om.put(
                "/glossaryTerms",
                {
                    "glossary": glossary["fullyQualifiedName"],
                    "name": name,
                    "displayName": m["label"],
                    "description": f"{m['label']}.\n\nBiểu thức (Trino/Superset): `{m['expression']}`\n\nNhóm: {group}",
                },
            )
            terms += 1
    stats["glossary_terms"] = terms
    return stats


def ensure_containers(om: Om) -> dict[str, str]:
    svc = om.put(
        "/services/storageServices",
        {
            "name": CATALOG["storage_service"],
            "serviceType": "S3",
            "description": "SeaweedFS (S3 API), bucket staging",
            "connection": {"config": {"type": "S3", "awsConfig": {"awsRegion": "us-east-1", "endPointURL": "http://s3:8333"}}},
        },
    )
    ids = {}
    for name, path in CATALOG["containers"].items():
        c = om.put(
            "/containers",
            {"name": name, "service": svc["fullyQualifiedName"], "description": f"Staging: {path}", "fullPath": path.split(" ")[0]},
        )
        ids[name] = c["id"]
    return ids


def add_lineage(om: Om, containers: dict[str, str]) -> int:
    def ref(name: str) -> dict:
        if name.startswith("container:"):
            return {"id": containers[name.split(":", 1)[1]], "type": "container"}
        t = om.get(f"/tables/name/{table_fqn(name)}")
        if not t:
            raise RuntimeError(f"table {name} not found in OpenMetadata")
        return {"id": t["id"], "type": "table"}

    n = 0
    for edge in CATALOG["lineage"]:
        details = {"description": f"Khai báo từ code: {edge['via']} (provenance: declared-from-code)", "source": "Manual"}
        if edge.get("columns"):
            details["columnsLineage"] = [
                {"fromColumns": [f"{table_fqn(edge['from'])}.{c}" for c in cl["from"]], "toColumn": f"{table_fqn(edge['to'])}.{cl['to']}"}
                for cl in edge["columns"]
            ]
        om.put("/lineage", {"edge": {"fromEntity": ref(edge["from"]), "toEntity": ref(edge["to"]), "lineageDetails": details}})
        n += 1
    return n


def verify(om: Om) -> dict:
    lin = om.get(f"/lineage/table/name/{table_fqn('silver.fact_ping_result')}", upstreamDepth=3, downstreamDepth=3)
    nodes = {n.get("fullyQualifiedName") for n in lin.get("nodes", [])}
    up = len(lin.get("upstreamEdges", []))
    down = len(lin.get("downstreamEdges", []))
    col_edges = sum(
        len(e.get("lineageDetails", {}).get("columnsLineage") or []) for e in lin.get("upstreamEdges", []) + lin.get("downstreamEdges", [])
    )
    dashboards = (om.get("/dashboards", service="superset_dashboards", limit=50) or {}).get("data", [])
    charts = (om.get("/charts", service="superset_dashboards", limit=100) or {}).get("paging", {}).get("total")
    gold_lin = om.get(f"/lineage/table/name/{table_fqn('gold.quality_hourly_probe')}", upstreamDepth=1, downstreamDepth=3) or {}
    to_dash = [n for n in gold_lin.get("nodes", []) if n.get("type") in ("dashboard", "dashboardDataModel", "chart")]
    tables = (om.get("/tables", database=f"{TRINO}.{DB}", limit=200) or {}).get("paging", {}).get("total")
    return {
        "tables_in_om": tables,
        "fact_lineage_nodes": len(nodes),
        "fact_upstream_edges": up,
        "fact_downstream_edges": down,
        "column_lineage_pairs_around_fact": col_edges,
        "dashboards": [d["displayName"] for d in dashboards],
        "charts": charts,
        "gold_probe_downstream_superset_nodes": len(to_dash),
    }


def main() -> int:
    om = Om()
    jwt = om.bot_jwt()
    print("== Trino metadata")
    run_workflow(workflow_config(trino_source(), jwt))
    print("== Superset metadata")
    run_workflow(workflow_config(superset_source(), jwt))
    print("== Enrichment", enrich(om))
    containers = ensure_containers(om)
    print("== Declared lineage edges", add_lineage(om, containers))
    result = verify(om)
    print("== VERIFY", json.dumps(result, ensure_ascii=False))
    return 0 if result["fact_upstream_edges"] > 0 and result["fact_downstream_edges"] > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
