from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.db.unified.relational_adapter as relational_adapter
from src.db.plugin_registry import DatabaseSource
from src.db.unified.adapter_factory import AdapterFactory
from src.db.unified.relational_adapter import (
    ClickHouseRelationalAdapter,
    MySQLTiDBRelationalAdapter,
    OracleRelationalAdapter,
    PostgreSQLRelationalAdapter,
    SQLiteRelationalAdapter,
)


class FakeMySQLCursor:
    def __init__(self, connection: FakeMySQLConnection) -> None:
        self._connection = connection
        self._last_query = ""

    def execute(self, query: str, params: object | None = None) -> int:
        self._last_query = query
        self._connection.executions.append((query, params))
        return 0

    def fetchall(self) -> list[tuple[object, ...]]:
        if "INFORMATION_SCHEMA.COLUMNS" in self._last_query:
            return self._connection.columns_rows

        table_match = re.search(
            r"FROM\s+`(?P<schema>(?:``|[^`])+)`\.`(?P<table>(?:``|[^`])+)`",
            self._last_query,
            flags=re.IGNORECASE,
        )
        column_match = re.search(
            r"SELECT\s+`(?P<column>(?:``|[^`])+)`\s+FROM",
            self._last_query,
            flags=re.IGNORECASE,
        )
        if table_match is None or column_match is None:
            return []

        table_name = table_match.group("table").replace("``", "`")
        column_name = column_match.group("column").replace("``", "`")
        return self._connection.values_by_column.get((table_name, column_name), [])


class FakeMySQLConnection:
    def __init__(
        self,
        columns_rows: list[tuple[object, ...]],
        values_by_column: dict[tuple[str, str], list[tuple[object, ...]]],
    ) -> None:
        self.columns_rows = columns_rows
        self.values_by_column = values_by_column
        self.executions: list[tuple[str, object | None]] = []
        self.closed = False

    def cursor(self) -> FakeMySQLCursor:
        return FakeMySQLCursor(self)

    def close(self) -> None:
        self.closed = True


class FakePostgreSQLCursor:
    def __init__(self, connection: FakePostgreSQLConnection) -> None:
        self._connection = connection
        self._last_query = ""

    def execute(self, query: str, params: object | None = None) -> int:
        self._last_query = query
        self._connection.executions.append((query, params))
        return 0

    def fetchall(self) -> list[tuple[object, ...]]:
        if "information_schema.columns" in self._last_query.lower():
            return self._connection.columns_rows

        table_match = re.search(
            r'FROM\s+"(?P<schema>(?:""|[^"])*)"\."(?P<table>(?:""|[^"])*)"',
            self._last_query,
            flags=re.IGNORECASE,
        )
        column_match = re.search(
            r'SELECT\s+"(?P<column>(?:""|[^"])*)"\s+FROM',
            self._last_query,
            flags=re.IGNORECASE,
        )
        if table_match is None or column_match is None:
            return []

        table_name = table_match.group("table").replace('""', '"')
        column_name = column_match.group("column").replace('""', '"')
        return self._connection.values_by_column.get((table_name, column_name), [])


class FakePostgreSQLConnection:
    def __init__(
        self,
        columns_rows: list[tuple[object, ...]],
        values_by_column: dict[tuple[str, str], list[tuple[object, ...]]],
    ) -> None:
        self.columns_rows = columns_rows
        self.values_by_column = values_by_column
        self.executions: list[tuple[str, object | None]] = []
        self.closed = False

    def cursor(self) -> FakePostgreSQLCursor:
        return FakePostgreSQLCursor(self)

    def close(self) -> None:
        self.closed = True


class FakeOracleCursor:
    def __init__(self, connection: FakeOracleConnection) -> None:
        self._connection = connection
        self._last_query = ""

    def execute(self, query: str, params: object | None = None) -> int:
        self._last_query = query
        self._connection.executions.append((query, params))
        return 0

    def fetchall(self) -> list[tuple[object, ...]]:
        if "all_tab_columns" in self._last_query.lower():
            return self._connection.columns_rows

        table_match = re.search(
            r'FROM\s+"(?P<schema>(?:""|[^"])*)"\."(?P<table>(?:""|[^"])*)"',
            self._last_query,
            flags=re.IGNORECASE,
        )
        column_match = re.search(
            r'SELECT\s+"(?P<column>(?:""|[^"])*)"\s+FROM',
            self._last_query,
            flags=re.IGNORECASE,
        )
        if table_match is None or column_match is None:
            return []

        table_name = table_match.group("table").replace('""', '"')
        column_name = column_match.group("column").replace('""', '"')
        return self._connection.values_by_column.get((table_name, column_name), [])


class FakeOracleConnection:
    def __init__(
        self,
        columns_rows: list[tuple[object, ...]],
        values_by_column: dict[tuple[str, str], list[tuple[object, ...]]],
    ) -> None:
        self.columns_rows = columns_rows
        self.values_by_column = values_by_column
        self.executions: list[tuple[str, object | None]] = []
        self.closed = False

    def cursor(self) -> FakeOracleCursor:
        return FakeOracleCursor(self)

    def close(self) -> None:
        self.closed = True


class FakeClickHouseCursor:
    def __init__(self, connection: FakeClickHouseConnection) -> None:
        self._connection = connection
        self._last_query = ""

    def execute(self, query: str, params: object | None = None) -> int:
        self._last_query = query
        self._connection.executions.append((query, params))
        return 0

    def fetchall(self) -> list[tuple[object, ...]]:
        if "system.columns" in self._last_query.lower():
            return self._connection.columns_rows

        table_match = re.search(
            r"FROM\s+`(?P<schema>(?:``|[^`])+)`\.`(?P<table>(?:``|[^`])+)`",
            self._last_query,
            flags=re.IGNORECASE,
        )
        column_match = re.search(
            r"SELECT\s+`(?P<column>(?:``|[^`])+)`\s+FROM",
            self._last_query,
            flags=re.IGNORECASE,
        )
        if table_match is None or column_match is None:
            return []

        table_name = table_match.group("table").replace("``", "`")
        column_name = column_match.group("column").replace("``", "`")
        return self._connection.values_by_column.get((table_name, column_name), [])


class FakeClickHouseConnection:
    def __init__(
        self,
        columns_rows: list[tuple[object, ...]],
        values_by_column: dict[tuple[str, str], list[tuple[object, ...]]],
    ) -> None:
        self.columns_rows = columns_rows
        self.values_by_column = values_by_column
        self.executions: list[tuple[str, object | None]] = []
        self.closed = False

    def cursor(self) -> FakeClickHouseCursor:
        return FakeClickHouseCursor(self)

    def close(self) -> None:
        self.closed = True


def test_sqlite_relational_adapter_filters_null_like_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MAX", 50)

    db_path = tmp_path / "filter_null_like.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE movies (title TEXT, note TEXT)")
        conn.executemany(
            "INSERT INTO movies (title, note) VALUES (?, ?)",
            [
                ("Inception", "good"),
                ("Interstellar", "NULL"),
                ("Tenet", ""),
                ("Memento", "  "),
                ("Dunkirk", None),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    source = DatabaseSource(name="MOVIES", driver="sqlite", dsn=str(db_path), options={})
    adapter = SQLiteRelationalAdapter(source)
    units = adapter.extract_field_units()
    unit_by_field = {unit.field_path: unit for unit in units}

    assert "note" in unit_by_field
    assert set(unit_by_field["note"].samples) == {"good"}
    assert all(sample.strip() and sample.upper() != "NULL" for sample in unit_by_field["note"].samples)


def test_sqlite_relational_adapter_respects_sample_min_max(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_RATIO", 0.02)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MIN", 10)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MAX", 20)

    db_path = tmp_path / "sample_bounds.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE events (event_name TEXT)")
        conn.executemany(
            "INSERT INTO events (event_name) VALUES (?)",
            [(f"event-{index}",) for index in range(2000)],
        )
        conn.commit()
    finally:
        conn.close()

    source = DatabaseSource(name="EVENTS", driver="sqlite", dsn=str(db_path), options={})
    adapter = SQLiteRelationalAdapter(source)
    units = adapter.extract_field_units()
    unit_by_field = {unit.field_path: unit for unit in units}

    assert "event_name" in unit_by_field
    assert len(unit_by_field["event_name"].samples) == 20


def test_mysql_tidb_relational_adapter_extracts_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MAX", 20)

    fake_connection = FakeMySQLConnection(
        columns_rows=[
            ("movies", "title", "varchar"),
            ("movies", "rating", "int"),
            ("movies", "empty_col", "varchar"),
        ],
        values_by_column={
            ("movies", "title"): [
                ("Inception",),
                ("Interstellar",),
                ("",),
                ("NULL",),
                ("Tenet",),
            ],
            ("movies", "rating"): [(9,), (8,), (None,), (7,)],
            ("movies", "empty_col"): [(None,), ("",), ("NULL",)],
        },
    )
    captured: dict[str, object] = {}

    def fake_connect(
        dsn: relational_adapter.MySQLLikeDsn,
        source_options: dict[str, str],
    ) -> FakeMySQLConnection:
        captured["dsn"] = dsn
        captured["source_options"] = source_options
        return fake_connection

    monkeypatch.setattr(relational_adapter, "_connect_mysql_like", fake_connect)

    source = DatabaseSource(
        name="MYSQL_SOURCE",
        driver="mysql",
        dsn="mysql://demo_user:demo_pass@127.0.0.1:3306/demo_db?charset=utf8mb4",
        options={"connect_timeout": "8"},
    )
    adapter = MySQLTiDBRelationalAdapter(source)
    units = adapter.extract_field_units()
    unit_by_field = {unit.field_path: unit for unit in units}

    assert set(unit_by_field.keys()) == {"title", "rating"}
    assert set(unit_by_field["title"].samples) == {"Inception", "Interstellar", "Tenet"}
    assert set(unit_by_field["rating"].samples) == {"9", "8", "7"}
    assert fake_connection.closed

    captured_dsn = captured["dsn"]
    assert isinstance(captured_dsn, relational_adapter.MySQLLikeDsn)
    assert captured_dsn.database == "demo_db"
    assert captured_dsn.username == "demo_user"
    assert captured_dsn.port == 3306
    assert captured["source_options"] == {"connect_timeout": "8"}

    assert any(
        "INFORMATION_SCHEMA.COLUMNS" in query and params == ("demo_db",)
        for query, params in fake_connection.executions
    )


def test_mysql_tidb_relational_adapter_rejects_invalid_dsn() -> None:
    source = DatabaseSource(
        name="BAD_MYSQL",
        driver="mysql",
        dsn="mysql://demo_user:demo_pass@127.0.0.1:3306/",
        options={},
    )

    with pytest.raises(ValueError, match="database name must not be empty"):
        MySQLTiDBRelationalAdapter(source).extract_field_units()


def test_postgresql_relational_adapter_extracts_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MAX", 20)

    fake_connection = FakePostgreSQLConnection(
        columns_rows=[
            ("movies", "title", "text"),
            ("movies", "score", "integer"),
            ("movies", "empty_col", "text"),
        ],
        values_by_column={
            ("movies", "title"): [("Inception",), ("Interstellar",), ("NULL",), ("",)],
            ("movies", "score"): [(10,), (9,), (None,), (8,)],
            ("movies", "empty_col"): [("",), ("NULL",), (None,)],
        },
    )
    captured: dict[str, object] = {}

    def fake_connect(
        dsn: relational_adapter.PostgreSQLDsn,
        source_options: dict[str, str],
    ) -> FakePostgreSQLConnection:
        captured["dsn"] = dsn
        captured["source_options"] = source_options
        return fake_connection

    monkeypatch.setattr(relational_adapter, "_connect_postgresql", fake_connect)

    source = DatabaseSource(
        name="PG_SOURCE",
        driver="postgresql",
        dsn="postgresql://pg_user:pg_pass@127.0.0.1:5432/demo_db?schema=analytics&sslmode=require",
        options={"connect_timeout": "7"},
    )
    adapter = PostgreSQLRelationalAdapter(source)
    units = adapter.extract_field_units()
    unit_by_field = {unit.field_path: unit for unit in units}

    assert set(unit_by_field.keys()) == {"title", "score"}
    assert set(unit_by_field["title"].samples) == {"Inception", "Interstellar"}
    assert set(unit_by_field["score"].samples) == {"10", "9", "8"}
    assert fake_connection.closed

    captured_dsn = captured["dsn"]
    assert isinstance(captured_dsn, relational_adapter.PostgreSQLDsn)
    assert captured_dsn.database == "demo_db"
    assert captured_dsn.schema == "analytics"
    assert captured_dsn.sslmode == "require"
    assert captured["source_options"] == {"connect_timeout": "7"}

    assert any(
        "information_schema.columns" in query.lower() and params == ("analytics",)
        for query, params in fake_connection.executions
    )


def test_postgresql_relational_adapter_rejects_invalid_dsn() -> None:
    source = DatabaseSource(
        name="BAD_PG",
        driver="postgresql",
        dsn="postgresql://pg_user:pg_pass@127.0.0.1:5432/",
        options={},
    )

    with pytest.raises(ValueError, match="database name must not be empty"):
        PostgreSQLRelationalAdapter(source).extract_field_units()


def test_oracle_relational_adapter_extracts_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MAX", 20)

    fake_connection = FakeOracleConnection(
        columns_rows=[
            ("MOVIES", "TITLE", "VARCHAR2"),
            ("MOVIES", "SCORE", "NUMBER"),
            ("MOVIES", "EMPTY_COL", "VARCHAR2"),
        ],
        values_by_column={
            ("MOVIES", "TITLE"): [("Inception",), ("Interstellar",), ("NULL",)],
            ("MOVIES", "SCORE"): [(10,), (9,), (None,), (8,)],
            ("MOVIES", "EMPTY_COL"): [("",), ("NULL",), (None,)],
        },
    )
    captured: dict[str, object] = {}

    def fake_connect(
        dsn: relational_adapter.OracleDsn,
        source_options: dict[str, str],
    ) -> FakeOracleConnection:
        captured["dsn"] = dsn
        captured["source_options"] = source_options
        return fake_connection

    monkeypatch.setattr(relational_adapter, "_connect_oracle", fake_connect)

    source = DatabaseSource(
        name="ORACLE_SOURCE",
        driver="oracle",
        dsn="oracle://ora_user:ora_pass@127.0.0.1:1521/ORCLPDB1",
        options={"owner": "APP"},
    )
    adapter = OracleRelationalAdapter(source)
    units = adapter.extract_field_units()
    unit_by_field = {unit.field_path: unit for unit in units}

    assert set(unit_by_field.keys()) == {"TITLE", "SCORE"}
    assert set(unit_by_field["TITLE"].samples) == {"Inception", "Interstellar"}
    assert set(unit_by_field["SCORE"].samples) == {"10", "9", "8"}
    assert fake_connection.closed

    captured_dsn = captured["dsn"]
    assert isinstance(captured_dsn, relational_adapter.OracleDsn)
    assert captured_dsn.owner == "APP"
    assert captured_dsn.service_name == "ORCLPDB1"
    assert captured["source_options"] == {"owner": "APP"}

    assert any(
        "all_tab_columns" in query.lower() and params == {"owner": "APP"}
        for query, params in fake_connection.executions
    )


def test_oracle_relational_adapter_rejects_invalid_dsn() -> None:
    source = DatabaseSource(
        name="BAD_ORACLE",
        driver="oracle",
        dsn="oracle://ora_user:ora_pass@127.0.0.1:1521/",
        options={},
    )

    with pytest.raises(ValueError, match="service name must not be empty"):
        OracleRelationalAdapter(source).extract_field_units()


def test_clickhouse_relational_adapter_extracts_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(relational_adapter, "DB_SAMPLE_MAX", 20)

    fake_connection = FakeClickHouseConnection(
        columns_rows=[
            ("movies", "title", "String"),
            ("movies", "score", "Int32"),
            ("movies", "empty_col", "String"),
        ],
        values_by_column={
            ("movies", "title"): [("Inception",), ("Interstellar",), ("NULL",)],
            ("movies", "score"): [(10,), (9,), (None,), (8,)],
            ("movies", "empty_col"): [("",), ("NULL",), (None,)],
        },
    )
    captured: dict[str, object] = {}

    def fake_connect(
        dsn: relational_adapter.ClickHouseDsn,
        source_options: dict[str, str],
    ) -> FakeClickHouseConnection:
        captured["dsn"] = dsn
        captured["source_options"] = source_options
        return fake_connection

    monkeypatch.setattr(relational_adapter, "_connect_clickhouse", fake_connect)

    source = DatabaseSource(
        name="CLICKHOUSE_SOURCE",
        driver="clickhouse",
        dsn="clickhouse://ch_user:ch_pass@127.0.0.1:9000/analytics",
        options={"secure": "false"},
    )
    adapter = ClickHouseRelationalAdapter(source)
    units = adapter.extract_field_units()
    unit_by_field = {unit.field_path: unit for unit in units}

    assert set(unit_by_field.keys()) == {"title", "score"}
    assert set(unit_by_field["title"].samples) == {"Inception", "Interstellar"}
    assert set(unit_by_field["score"].samples) == {"10", "9", "8"}
    assert fake_connection.closed

    captured_dsn = captured["dsn"]
    assert isinstance(captured_dsn, relational_adapter.ClickHouseDsn)
    assert captured_dsn.database == "analytics"
    assert captured_dsn.secure is False
    assert captured["source_options"] == {"secure": "false"}

    assert any(
        "system.columns" in query.lower() and "database = 'analytics'" in query
        for query, _ in fake_connection.executions
    )


def test_factory_routes_relational_engines_to_expected_adapters() -> None:
    factory = AdapterFactory()
    mysql_adapter = factory.create(
        DatabaseSource(
            name="MYSQL_SOURCE",
            driver="mysql",
            dsn="mysql://user:pass@127.0.0.1:3306/demo",
            options={},
        )
    )
    tidb_adapter = factory.create(
        DatabaseSource(
            name="TIDB_SOURCE",
            driver="tidb",
            dsn="mysql://user:pass@127.0.0.1:4000/demo",
            options={},
        )
    )
    postgresql_adapter = factory.create(
        DatabaseSource(
            name="PG_SOURCE",
            driver="postgresql",
            dsn="postgresql://user:pass@127.0.0.1:5432/demo",
            options={},
        )
    )
    postgres_alias_adapter = factory.create(
        DatabaseSource(
            name="POSTGRES_ALIAS_SOURCE",
            driver="postgres",
            dsn="postgresql://user:pass@127.0.0.1:5432/demo",
            options={},
        )
    )
    oracle_adapter = factory.create(
        DatabaseSource(
            name="ORACLE_SOURCE",
            driver="oracle",
            dsn="oracle://user:pass@127.0.0.1:1521/ORCLPDB1",
            options={},
        )
    )
    clickhouse_adapter = factory.create(
        DatabaseSource(
            name="CLICKHOUSE_SOURCE",
            driver="clickhouse",
            dsn="clickhouse://user:pass@127.0.0.1:9000/default",
            options={},
        )
    )

    assert isinstance(mysql_adapter, MySQLTiDBRelationalAdapter)
    assert isinstance(tidb_adapter, MySQLTiDBRelationalAdapter)
    assert isinstance(postgresql_adapter, PostgreSQLRelationalAdapter)
    assert isinstance(postgres_alias_adapter, PostgreSQLRelationalAdapter)
    assert isinstance(oracle_adapter, OracleRelationalAdapter)
    assert isinstance(clickhouse_adapter, ClickHouseRelationalAdapter)
