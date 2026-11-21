"""Kết nối Trino (user `transform`, catalog `lakehouse`) cho runner: truy vấn nhỏ, DDL, ghi bảng vận hành.

Dữ liệu lớn KHÔNG đi qua Python: biến đổi nằm trong dbt; runner chỉ đọc số đếm, danh sách, snapshot id.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from lakehouse.common import split_sql, sql_str

CATALOG = os.environ.get("LAKEHOUSE_CATALOG", "lakehouse")


class Trino:
    def __init__(self, conn):
        self.conn = conn

    @classmethod
    def connect(cls) -> Trino:
        import trino

        conn = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "trino"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user="transform",
            catalog=CATALOG,
            schema="work",
            http_scheme="http",
            session_properties={"query_max_execution_time": "30m", "query_max_run_time": "30m"},
        )
        return cls(conn)

    def query(self, sql: str) -> list[tuple]:
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def dicts(self, sql: str) -> list[dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r, strict=True)) for r in rows]

    def scalar(self, sql: str) -> Any:
        rows = self.query(sql)
        return rows[0][0] if rows else None

    def execute(self, sql: str) -> None:
        self.query(sql)

    def run_file(self, path: Path) -> None:
        for statement in split_sql(path.read_text(encoding="utf-8")):
            self.execute(statement)

    def latest_snapshot(self, table: str) -> int | None:
        """Snapshot mới nhất của bảng Iceberg `<schema>.<table>` trong catalog của phiên (mặc định lakehouse)."""
        schema, name = table.split(".")[-2:]
        return self.scalar(f'SELECT snapshot_id FROM {schema}."{name}$snapshots" ORDER BY committed_at DESC LIMIT 1')

    def table_exists(self, table: str) -> bool:
        schema, name = table.split(".")[-2:]
        return bool(
            self.scalar(f"SELECT count(*) FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{name}'")
        )


def insert_rows(db: Trino, table: str, rows: list[dict[str, Any]], chunk: int = 200) -> None:
    """INSERT nhiều dòng (cùng bộ khóa) bằng VALUES, dành cho bảng vận hành nhỏ: manifest, dq_result."""
    for i in range(0, len(rows), chunk):
        batch = rows[i : i + chunk]
        values = ",\n".join("(" + ", ".join(map(sql_str, r.values())) + ")" for r in batch)
        db.execute(f"INSERT INTO {table} ({', '.join(batch[0])}) VALUES\n{values}")
