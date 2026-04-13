from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.db.plugin_registry import DatabaseSource
from src.distributed.contracts import LocalSubQueryFilter, LocalSubQueryRequest
from src.distributed.local_agent_service import LocalDomainAgentService


def test_local_agent_execute_subquery_sqlite(tmp_path: Path) -> None:
    db_path = tmp_path / "domain_a.db"
    connection = sqlite3.connect(str(db_path))
    try:
        connection.execute("CREATE TABLE students (student_id TEXT, name TEXT, grade INTEGER)")
        connection.executemany(
            "INSERT INTO students (student_id, name, grade) VALUES (?, ?, ?)",
            [
                ("20230001", "Alice", 95),
                ("20230002", "Bob", 88),
            ],
        )
        connection.commit()
    finally:
        connection.close()

    source = DatabaseSource(
        name="domain_a",
        driver="sqlite",
        dsn=str(db_path),
        options={},
    )
    service = LocalDomainAgentService(
        node_id="node_exec",
        source_loader=lambda: {"domain_a": source},
    )

    rows = service.execute_local_subquery(
        LocalSubQueryRequest(
            source_name="domain_a",
            table="students",
            select_fields=("student_id", "name", "grade"),
            filters=(
                LocalSubQueryFilter(field="student_id", operator="eq", value="20230001"),
                LocalSubQueryFilter(field="grade", operator="gt", value=90),
            ),
            limit=20,
        )
    )
    assert len(rows) == 1
    assert rows[0].domain_id == "domain_a"
    assert rows[0].table == "students"
    assert rows[0].data["student_id"] == "20230001"
    assert rows[0].data["name"] == "Alice"
