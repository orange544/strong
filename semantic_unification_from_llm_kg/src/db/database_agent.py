import random
import sqlite3
from typing import Any

from src.configs.config import DB_SAMPLE_MAX, DB_SAMPLE_MIN, DB_SAMPLE_RATIO


def _quote_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


class DatabaseAgent:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)

    def close(self) -> None:
        self.conn.close()

    def sample_field(self, table: str, field: str) -> dict[str, Any]:
        cursor = self.conn.cursor()
        try:
            query = f"SELECT {_quote_identifier(field)} FROM {_quote_identifier(table)}"
            cursor.execute(query)
            rows = [r[0] for r in cursor.fetchall()]
        except Exception as e:
            print(f"read field failed {table}.{field}: {e}")
            rows = []

        cleaned_rows: list[Any] = []
        for r in rows:
            if r is None:
                continue
            if isinstance(r, str) and (r.strip() == "" or r.strip().upper() == "NULL"):
                continue
            cleaned_rows.append(r)

        total = len(cleaned_rows)
        if total == 0:
            samples: list[Any] = []
        else:
            n = max(int(total * DB_SAMPLE_RATIO), DB_SAMPLE_MIN)
            n = min(n, total, DB_SAMPLE_MAX)
            samples = random.sample(cleaned_rows, n)

        return {
            "table": table,
            "field": field,
            "type": "VARCHAR",
            "samples": samples,
        }


def get_all_fields(agent: DatabaseAgent) -> list[dict[str, Any]]:
    cursor = agent.conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [t[0] for t in cursor.fetchall()]
    all_field_samples: list[dict[str, Any]] = []
    for table in tables:
        cursor.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        columns = [c[1] for c in cursor.fetchall()]
        for field in columns:
            sample = agent.sample_field(table, field)
            if sample["samples"]:
                all_field_samples.append(sample)
    return all_field_samples


def generate_db_data(db_agents: dict[str, DatabaseAgent]) -> dict[str, dict[str, list[str]]]:
    db_data: dict[str, dict[str, list[str]]] = {}
    for db_name, agent in db_agents.items():
        cursor = agent.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [t[0] for t in cursor.fetchall()]
        db_data[db_name] = {}
        for table in tables:
            cursor.execute(f"PRAGMA table_info({_quote_identifier(table)})")
            columns = [c[1] for c in cursor.fetchall()]
            db_data[db_name][table] = columns
    return db_data
