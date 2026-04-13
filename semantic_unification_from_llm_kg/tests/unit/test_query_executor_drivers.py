from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.db.plugin_registry import DatabaseSource
from src.distributed.contracts import LocalSubQueryFilter
from src.query.query_executor import (
    CassandraQueryExecutor,
    ClickHouseQueryExecutor,
    HBaseQueryExecutor,
    MongoQueryExecutor,
    MySQLQueryExecutor,
    Neo4jQueryExecutor,
    OracleQueryExecutor,
    PostgreSQLQueryExecutor,
    RedisQueryExecutor,
    SQLiteQueryExecutor,
    create_query_executor,
)


@pytest.mark.parametrize(
    ("driver", "expected_class"),
    [
        ("sqlite", SQLiteQueryExecutor),
        ("mysql", MySQLQueryExecutor),
        ("tidb", MySQLQueryExecutor),
        ("postgresql", PostgreSQLQueryExecutor),
        ("postgres", PostgreSQLQueryExecutor),
        ("mongodb", MongoQueryExecutor),
        ("neo4j", Neo4jQueryExecutor),
        ("clickhouse", ClickHouseQueryExecutor),
        ("ch", ClickHouseQueryExecutor),
        ("oracle", OracleQueryExecutor),
        ("redis", RedisQueryExecutor),
        ("cassandra", CassandraQueryExecutor),
        ("scylla", CassandraQueryExecutor),
        ("hbase", HBaseQueryExecutor),
        ("thrift", HBaseQueryExecutor),
    ],
)
def test_factory_supports_extended_drivers(driver: str, expected_class: type[object]) -> None:
    source = DatabaseSource(name="x", driver=driver, dsn="sqlite:///tmp/placeholder", options={})
    executor = create_query_executor(source)
    assert isinstance(executor, expected_class)


def test_factory_rejects_unknown_driver() -> None:
    with pytest.raises(RuntimeError, match="not implemented"):
        create_query_executor(
            DatabaseSource(name="x", driver="unknown_driver", dsn="none://", options={})
        )


def test_clickhouse_executor_runs_with_fake_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def __init__(self, **kwargs: object) -> None:
            captured["init"] = kwargs

        def execute(self, sql: str, params: dict[str, object]) -> list[tuple[int, str]]:
            captured["sql"] = sql
            captured["params"] = params
            return [(1, "alice")]

    monkeypatch.setitem(sys.modules, "clickhouse_driver", types.SimpleNamespace(Client=_FakeClient))

    source = DatabaseSource(
        name="domain_clickhouse",
        driver="clickhouse",
        dsn="clickhouse://user:pass@127.0.0.1:9000/demo_db",
        options={},
    )
    executor = ClickHouseQueryExecutor(source)
    rows = executor.execute(
        table="students",
        select_fields=("student_id", "name"),
        filters=(LocalSubQueryFilter(field="student_id", operator="eq", value=1),),
        limit=5,
    )

    assert rows == [{"student_id": 1, "name": "alice"}]
    assert "SELECT" in str(captured["sql"])
    assert captured["params"] == {"p0": 1}


def test_oracle_executor_runs_with_fake_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _FakeCursor:
        def execute(self, sql: str, params: dict[str, object]) -> None:
            captured["sql"] = sql
            captured["params"] = params

        def fetchall(self) -> list[tuple[int, str]]:
            return [(1, "ok")]

        def close(self) -> None:
            return

    class _FakeConnection:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def close(self) -> None:
            return

    def _fake_connect(**kwargs: object) -> _FakeConnection:
        captured["connect_kwargs"] = kwargs
        return _FakeConnection()

    monkeypatch.setitem(sys.modules, "oracledb", types.SimpleNamespace(connect=_fake_connect))

    source = DatabaseSource(
        name="domain_oracle",
        driver="oracle",
        dsn="oracle://scott:tiger@127.0.0.1:1521/XEPDB1",
        options={},
    )
    executor = OracleQueryExecutor(source)
    rows = executor.execute(
        table="student",
        select_fields=("student_id", "name"),
        filters=(LocalSubQueryFilter(field="student_id", operator="eq", value=1),),
        limit=3,
    )

    assert rows == [{"student_id": 1, "name": "ok"}]
    assert "FETCH FIRST 3 ROWS ONLY" in str(captured["sql"])
    assert captured["params"] == {"p0": 1}


def test_redis_executor_runs_with_fake_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeRedisClient:
        def scan_iter(self, *, match: str, count: int) -> list[bytes]:
            del match
            del count
            return [b"students:1", b"students:2"]

        def type(self, key: str) -> bytes:
            del key
            return b"hash"

        def hgetall(self, key: str) -> dict[bytes, bytes]:
            if key == "students:1":
                return {b"student_id": b"1", b"name": b"Alice", b"grade": b"95"}
            return {b"student_id": b"2", b"name": b"Bob", b"grade": b"88"}

    class _FakeRedisFacade:
        @staticmethod
        def from_url(url: str) -> _FakeRedisClient:
            del url
            return _FakeRedisClient()

    monkeypatch.setitem(sys.modules, "redis", types.SimpleNamespace(Redis=_FakeRedisFacade))

    source = DatabaseSource(
        name="domain_redis",
        driver="redis",
        dsn="redis://127.0.0.1:6379/0",
        options={},
    )
    executor = RedisQueryExecutor(source)
    rows = executor.execute(
        table="students",
        select_fields=("row_key", "student_id", "name"),
        filters=(LocalSubQueryFilter(field="grade", operator="gt", value=90),),
        limit=10,
    )

    assert rows == [{"row_key": "students:1", "student_id": "1", "name": "Alice"}]


def test_cassandra_executor_runs_with_fake_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _FakePlainTextAuthProvider:
        def __init__(self, *, username: str, password: str) -> None:
            captured["auth"] = (username, password)

    class _FakeSession:
        def execute(self, sql: str, params: list[object]) -> list[types.SimpleNamespace]:
            captured["sql"] = sql
            captured["params"] = params
            return [types.SimpleNamespace(student_id=1, name="Alice")]

        def shutdown(self) -> None:
            return

    class _FakeCluster:
        def __init__(
            self,
            *,
            contact_points: list[str],
            port: int,
            auth_provider: _FakePlainTextAuthProvider | None,
        ) -> None:
            captured["cluster"] = {
                "contact_points": contact_points,
                "port": port,
                "auth_provider": auth_provider,
            }

        def connect(self, keyspace: str) -> _FakeSession:
            captured["keyspace"] = keyspace
            return _FakeSession()

        def shutdown(self) -> None:
            return

    monkeypatch.setitem(
        sys.modules,
        "cassandra.auth",
        types.SimpleNamespace(PlainTextAuthProvider=_FakePlainTextAuthProvider),
    )
    monkeypatch.setitem(
        sys.modules,
        "cassandra.cluster",
        types.SimpleNamespace(Cluster=_FakeCluster),
    )

    source = DatabaseSource(
        name="domain_cassandra",
        driver="cassandra",
        dsn="cassandra://u:p@127.0.0.1:9042/demo_keyspace",
        options={},
    )
    executor = CassandraQueryExecutor(source)
    rows = executor.execute(
        table="students",
        select_fields=("student_id", "name"),
        filters=(LocalSubQueryFilter(field="student_id", operator="eq", value=1),),
        limit=2,
    )

    assert rows == [{"student_id": 1, "name": "Alice"}]
    assert "ALLOW FILTERING" in str(captured["sql"])


def test_hbase_executor_runs_with_fake_driver(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeTable:
        def row(self, key: bytes) -> dict[bytes, bytes]:
            if key == b"students:1":
                return {b"cf:name": b"Alice", b"cf:grade": b"95"}
            return {}

        def scan(self, *, limit: int) -> list[tuple[bytes, dict[bytes, bytes]]]:
            del limit
            return [
                (b"students:1", {b"cf:name": b"Alice", b"cf:grade": b"95"}),
                (b"students:2", {b"cf:name": b"Bob", b"cf:grade": b"88"}),
            ]

    class _FakeConnection:
        def __init__(self, *, host: str, port: int, autoconnect: bool) -> None:
            del host
            del port
            del autoconnect

        def table(self, _name: str) -> _FakeTable:
            return _FakeTable()

        def close(self) -> None:
            return

    monkeypatch.setitem(sys.modules, "happybase", types.SimpleNamespace(Connection=_FakeConnection))

    source = DatabaseSource(
        name="domain_hbase",
        driver="hbase",
        dsn="hbase://127.0.0.1:9090",
        options={},
    )
    executor = HBaseQueryExecutor(source)
    rows = executor.execute(
        table="student_table",
        select_fields=("row_key", "cf:name"),
        filters=(LocalSubQueryFilter(field="cf:grade", operator="gte", value=90),),
        limit=5,
    )

    assert rows == [{"row_key": "students:1", "cf:name": "Alice"}]
