from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.db.unified.non_relational_adapter as non_relational_adapter
from src.db.plugin_registry import DatabaseSource
from src.db.unified.adapter_factory import AdapterFactory
from src.db.unified.non_relational_adapter import (
    CassandraAdapter,
    HBaseAdapter,
    MongoDBAdapter,
    Neo4jAdapter,
    RedisAdapter,
)


class FakeMongoCollection:
    def __init__(self, docs: list[object]) -> None:
        self._docs = docs

    def find(self, query: dict[str, object], *, limit: int) -> list[object]:
        del query
        return self._docs[:limit]


class FakeMongoDatabase:
    def __init__(self, collections: dict[str, FakeMongoCollection]) -> None:
        self._collections = collections

    def list_collection_names(self) -> list[str]:
        return list(self._collections.keys())

    def get_collection(self, name: str) -> FakeMongoCollection:
        return self._collections[name]


class FakeMongoClient:
    def __init__(self, database: FakeMongoDatabase) -> None:
        self._database = database
        self.closed = False

    def get_database(self, name: str) -> FakeMongoDatabase:
        del name
        return self._database

    def close(self) -> None:
        self.closed = True


class FakeNeo4jResult:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def data(self) -> list[dict[str, object]]:
        return self._rows


class FakeNeo4jSession:
    def __init__(
        self,
        node_meta: list[dict[str, object]],
        rel_meta: list[dict[str, object]],
        node_values: dict[tuple[str, str], list[object]],
        rel_values: dict[tuple[str, str], list[object]],
    ) -> None:
        self._node_meta = node_meta
        self._rel_meta = rel_meta
        self._node_values = node_values
        self._rel_values = rel_values
        self.closed = False

    def run(
        self,
        query: str,
        parameters: dict[str, object] | None = None,
    ) -> FakeNeo4jResult:
        if "UNWIND labels(n) AS label" in query:
            return FakeNeo4jResult(self._node_meta)

        if "UNWIND keys(r) AS field" in query:
            return FakeNeo4jResult(self._rel_meta)

        if "WHERE $container IN labels(n)" in query:
            if parameters is None:
                return FakeNeo4jResult([])
            container = str(parameters.get("container", ""))
            field = str(parameters.get("field", ""))
            values = self._node_values.get((container, field), [])
            return FakeNeo4jResult([{"value": value} for value in values])

        if "WHERE type(r) = $container" in query:
            if parameters is None:
                return FakeNeo4jResult([])
            container = str(parameters.get("container", ""))
            field = str(parameters.get("field", ""))
            values = self._rel_values.get((container, field), [])
            return FakeNeo4jResult([{"value": value} for value in values])

        return FakeNeo4jResult([])

    def close(self) -> None:
        self.closed = True


class FakeNeo4jDriver:
    def __init__(self, session: FakeNeo4jSession) -> None:
        self._session = session
        self.closed = False

    def session(self, *, database: str) -> FakeNeo4jSession:
        del database
        return self._session

    def close(self) -> None:
        self.closed = True


class FakeRedisClient:
    def __init__(self) -> None:
        self.closed = False
        self._keys = [b"user:1", b"doc:1", b"plain:1", b"set:1"]
        self._types = {
            b"user:1": b"hash",
            b"doc:1": b"string",
            b"plain:1": b"string",
            b"set:1": b"set",
        }

    def scan_iter(self, *, match: str, count: int) -> list[bytes]:
        del match, count
        return self._keys

    def type(self, key: object) -> object:
        assert isinstance(key, bytes)
        return self._types[key]

    def hgetall(self, key: object) -> dict[bytes, bytes]:
        assert isinstance(key, bytes)
        if key == b"user:1":
            return {b"name": b"Alice", b"email": b"NULL"}
        return {}

    def get(self, key: object) -> object:
        assert isinstance(key, bytes)
        if key == b"doc:1":
            return b'{"profile":{"age":30},"nickname":"Bob"}'
        if key == b"plain:1":
            return b"hello"
        return None

    def lrange(self, key: object, start: int, end: int) -> list[object]:
        del key, start, end
        return []

    def smembers(self, key: object) -> set[object]:
        assert isinstance(key, bytes)
        if key == b"set:1":
            return {b"x", b"y"}
        return set()

    def zrange(self, key: object, start: int, end: int) -> list[object]:
        del key, start, end
        return []

    def close(self) -> None:
        self.closed = True


class FakeCassandraSession:
    def __init__(self) -> None:
        self.executions: list[tuple[str, object | None]] = []

    def execute(self, query: str, parameters: object | None = None) -> list[object]:
        self.executions.append((query, parameters))
        if "system_schema.columns" in query:
            return [
                ("movies", "title", "text"),
                ("movies", "score", "int"),
                ("movies", "empty_col", "text"),
            ]

        match = re.search(r'SELECT\s+"(?P<column>[^"]+)"\s+FROM\s+"[^"]+"\."(?P<table>[^"]+)"', query)
        if match is None:
            return []

        column = match.group("column")
        table = match.group("table")
        if (table, column) == ("movies", "title"):
            return [("Inception",), ("Interstellar",), ("NULL",)]
        if (table, column) == ("movies", "score"):
            return [(9,), (8,), (None,), (7,)]
        if (table, column) == ("movies", "empty_col"):
            return [(None,), ("",), ("NULL",)]
        return []


class FakeCassandraCluster:
    def __init__(self) -> None:
        self.closed = False

    def shutdown(self) -> None:
        self.closed = True


class FakeHBaseTable:
    def __init__(self, rows: list[tuple[object, dict[object, object]]]) -> None:
        self._rows = rows

    def scan(self, *, limit: int) -> list[tuple[object, dict[object, object]]]:
        return self._rows[:limit]


class FakeHBaseConnection:
    def __init__(self) -> None:
        self.closed = False
        self._tables = {
            "movie:films": FakeHBaseTable(
                [
                    (b"rk1", {b"info:title": b"Inception", b"info:year": b"2010"}),
                    (b"rk2", {b"info:title": b"Interstellar", b"info:year": b"2014"}),
                ]
            ),
            "other:tmp": FakeHBaseTable([(b"rk", {b"a:b": b"x"})]),
        }

    def tables(self) -> list[bytes]:
        return [table_name.encode("utf-8") for table_name in self._tables]

    def table(self, name: str) -> FakeHBaseTable:
        return self._tables[name]

    def close(self) -> None:
        self.closed = True


def test_mongodb_adapter_extracts_document_and_nested_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MAX", 20)

    fake_db = FakeMongoDatabase(
        {
            "users": FakeMongoCollection(
                [
                    {"name": "Alice", "profile": {"city": "Beijing", "zip": "NULL"}, "empty": ""},
                    {"name": "Bob", "profile": {"city": "Shanghai"}},
                ]
            )
        }
    )
    fake_client = FakeMongoClient(fake_db)

    def fake_connect(
        dsn: non_relational_adapter.MongoDBDsn,
        source_options: dict[str, str],
    ) -> FakeMongoClient:
        del dsn, source_options
        return fake_client

    monkeypatch.setattr(non_relational_adapter, "_connect_mongodb", fake_connect)

    source = DatabaseSource(
        name="MONGO_SRC",
        driver="mongodb",
        dsn="mongodb://user:pass@127.0.0.1:27017/demo",
        options={},
    )
    units = MongoDBAdapter(source).extract_field_units()
    unit_map = {(unit.container_name, unit.field_path): unit for unit in units}

    assert ("users", "name") in unit_map
    assert unit_map[("users", "name")].field_origin == "document_key"
    assert set(unit_map[("users", "name")].samples) == {"Alice", "Bob"}

    assert ("users", "profile.city") in unit_map
    assert unit_map[("users", "profile.city")].field_origin == "nested_key"
    assert set(unit_map[("users", "profile.city")].samples) == {"Beijing", "Shanghai"}

    assert ("users", "profile.zip") not in unit_map
    assert fake_client.closed


def test_neo4j_adapter_extracts_node_and_relationship_properties(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MAX", 20)

    fake_session = FakeNeo4jSession(
        node_meta=[{"container": "Person", "field_path": "name"}],
        rel_meta=[{"container": "KNOWS", "field_path": "since"}],
        node_values={("Person", "name"): ["Alice", "NULL"]},
        rel_values={("KNOWS", "since"): [2020, None]},
    )
    fake_driver = FakeNeo4jDriver(fake_session)
    captured: dict[str, object] = {}

    def fake_connect(
        dsn: non_relational_adapter.Neo4jDsn,
        source_options: dict[str, str],
    ) -> FakeNeo4jDriver:
        captured["dsn"] = dsn
        captured["source_options"] = source_options
        return fake_driver

    monkeypatch.setattr(non_relational_adapter, "_connect_neo4j", fake_connect)

    source = DatabaseSource(
        name="NEO4J_SRC",
        driver="neo4j",
        dsn="neo4j://neo4j:Neo4j@123456@127.0.0.1:7687/neo4j",
        options={},
    )
    units = Neo4jAdapter(source).extract_field_units()
    unit_map = {(unit.container_name, unit.field_path): unit for unit in units}

    assert unit_map[("Person", "name")].field_origin == "node_property"
    assert set(unit_map[("Person", "name")].samples) == {"Alice"}

    assert unit_map[("KNOWS", "since")].field_origin == "relationship_property"
    assert set(unit_map[("KNOWS", "since")].samples) == {"2020"}

    captured_dsn = captured["dsn"]
    assert isinstance(captured_dsn, non_relational_adapter.Neo4jDsn)
    assert captured_dsn.uri == "neo4j://127.0.0.1:7687/neo4j"
    assert captured_dsn.username == "neo4j"
    assert captured_dsn.password == "Neo4j@123456"
    assert captured["source_options"] == {}

    assert fake_session.closed
    assert fake_driver.closed


def test_redis_adapter_extracts_hash_json_and_object_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MAX", 20)

    fake_client = FakeRedisClient()

    def fake_connect(
        dsn: non_relational_adapter.RedisDsn,
        source_options: dict[str, str],
    ) -> FakeRedisClient:
        del dsn, source_options
        return fake_client

    monkeypatch.setattr(non_relational_adapter, "_connect_redis", fake_connect)

    source = DatabaseSource(
        name="REDIS_SRC",
        driver="redis",
        dsn="redis://127.0.0.1:6379/0",
        options={},
    )
    units = RedisAdapter(source).extract_field_units()
    unit_map = {(unit.container_name, unit.field_path): unit for unit in units}

    assert unit_map[("user:1", "name")].field_origin == "redis_hash_field"
    assert set(unit_map[("user:1", "name")].samples) == {"Alice"}

    assert unit_map[("doc:1", "profile.age")].field_origin == "redis_json_property"
    assert set(unit_map[("doc:1", "profile.age")].samples) == {"30"}

    assert unit_map[("plain:1", "value")].field_origin == "redis_object_property"
    assert set(unit_map[("plain:1", "value")].samples) == {"hello"}

    assert unit_map[("set:1", "value")].field_origin == "redis_object_property"
    assert set(unit_map[("set:1", "value")].samples) == {"x", "y"}

    assert ("user:1", "email") not in unit_map
    assert fake_client.closed


def test_cassandra_adapter_extracts_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MAX", 20)

    fake_cluster = FakeCassandraCluster()
    fake_session = FakeCassandraSession()

    def fake_connect(
        dsn: non_relational_adapter.CassandraDsn,
        source_options: dict[str, str],
    ) -> tuple[FakeCassandraCluster, FakeCassandraSession]:
        del dsn, source_options
        return fake_cluster, fake_session

    monkeypatch.setattr(non_relational_adapter, "_connect_cassandra", fake_connect)

    source = DatabaseSource(
        name="CASSANDRA_SRC",
        driver="cassandra",
        dsn="cassandra://user:pass@127.0.0.1:9042/demo",
        options={},
    )
    units = CassandraAdapter(source).extract_field_units()
    unit_map = {(unit.container_name, unit.field_path): unit for unit in units}

    assert ("movies", "title") in unit_map
    assert unit_map[("movies", "title")].field_origin == "column"
    assert set(unit_map[("movies", "title")].samples) == {"Inception", "Interstellar"}

    assert ("movies", "score") in unit_map
    assert set(unit_map[("movies", "score")].samples) == {"9", "8", "7"}

    assert ("movies", "empty_col") not in unit_map
    assert fake_cluster.closed


def test_hbase_adapter_extracts_family_qualifier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(non_relational_adapter, "DB_SAMPLE_MAX", 20)

    fake_connection = FakeHBaseConnection()

    def fake_connect(
        dsn: non_relational_adapter.HBaseDsn,
        source_options: dict[str, str],
    ) -> FakeHBaseConnection:
        del dsn, source_options
        return fake_connection

    monkeypatch.setattr(non_relational_adapter, "_connect_hbase", fake_connect)

    source = DatabaseSource(
        name="HBASE_SRC",
        driver="hbase",
        dsn="hbase://127.0.0.1:9090/movie",
        options={},
    )
    units = HBaseAdapter(source).extract_field_units()
    unit_map = {(unit.container_name, unit.field_path): unit for unit in units}

    assert ("movie:films", "info:title") in unit_map
    assert unit_map[("movie:films", "info:title")].field_origin == "hbase_family_qualifier"
    assert set(unit_map[("movie:films", "info:title")].samples) == {"Inception", "Interstellar"}

    assert ("other:tmp", "a:b") not in unit_map
    assert fake_connection.closed


def test_factory_routes_non_relational_types_to_expected_adapters() -> None:
    factory = AdapterFactory()

    mongo_adapter = factory.create(
        DatabaseSource(
            name="MONGO_SRC",
            driver="mongodb",
            dsn="mongodb://127.0.0.1:27017/demo",
            options={},
        )
    )
    neo4j_adapter = factory.create(
        DatabaseSource(
            name="NEO4J_SRC",
            driver="neo4j",
            dsn="neo4j://neo4j:secret@127.0.0.1:7687/neo4j",
            options={},
        )
    )
    redis_adapter = factory.create(
        DatabaseSource(
            name="REDIS_SRC",
            driver="redis",
            dsn="redis://127.0.0.1:6379/0",
            options={},
        )
    )
    cassandra_adapter = factory.create(
        DatabaseSource(
            name="CASSANDRA_SRC",
            driver="cassandra",
            dsn="cassandra://127.0.0.1:9042/demo",
            options={},
        )
    )
    hbase_adapter = factory.create(
        DatabaseSource(
            name="HBASE_SRC",
            driver="hbase",
            dsn="hbase://127.0.0.1:9090/movie",
            options={},
        )
    )

    assert isinstance(mongo_adapter, MongoDBAdapter)
    assert isinstance(neo4j_adapter, Neo4jAdapter)
    assert isinstance(redis_adapter, RedisAdapter)
    assert isinstance(cassandra_adapter, CassandraAdapter)
    assert isinstance(hbase_adapter, HBaseAdapter)


def test_non_relational_adapter_rejects_invalid_dsn_shape() -> None:
    with pytest.raises(ValueError, match="mongodb dsn database must not be empty"):
        MongoDBAdapter(
            DatabaseSource(
                name="BAD_MONGO",
                driver="mongodb",
                dsn="mongodb://127.0.0.1:27017",
                options={},
            )
        ).extract_field_units()

    with pytest.raises(ValueError, match="neo4j password must not be empty"):
        Neo4jAdapter(
            DatabaseSource(
                name="BAD_NEO4J",
                driver="neo4j",
                dsn="neo4j://neo4j@127.0.0.1:7687/neo4j",
                options={},
            )
        ).extract_field_units()

    with pytest.raises(ValueError, match="cassandra dsn keyspace must not be empty"):
        CassandraAdapter(
            DatabaseSource(
                name="BAD_CASSANDRA",
                driver="cassandra",
                dsn="cassandra://127.0.0.1:9042",
                options={},
            )
        ).extract_field_units()

    with pytest.raises(ValueError, match="hbase dsn host must not be empty"):
        HBaseAdapter(
            DatabaseSource(
                name="BAD_HBASE",
                driver="hbase",
                dsn="hbase:///movie",
                options={},
            )
        ).extract_field_units()
