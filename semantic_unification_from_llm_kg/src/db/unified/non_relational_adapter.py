from __future__ import annotations

import json
import random
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import unquote, urlparse, urlunparse

from src.configs.config import DB_SAMPLE_MAX, DB_SAMPLE_MIN, DB_SAMPLE_RATIO
from src.db.plugin_registry import DatabaseSource
from src.db.unified.base_adapter import BaseAdapter
from src.db.unified.field_unit import FieldUnit


@dataclass(frozen=True, slots=True)
class MongoDBDsn:
    uri: str
    database: str


@dataclass(frozen=True, slots=True)
class Neo4jDsn:
    uri: str
    username: str
    password: str
    database: str


@dataclass(frozen=True, slots=True)
class RedisDsn:
    uri: str


@dataclass(frozen=True, slots=True)
class CassandraDsn:
    hosts: tuple[str, ...]
    port: int
    keyspace: str
    username: str | None
    password: str | None


@dataclass(frozen=True, slots=True)
class HBaseDsn:
    host: str
    port: int
    namespace: str | None


class MongoCollectionProtocol(Protocol):
    def find(self, query: Mapping[str, object], *, limit: int) -> Iterable[object]: ...


class MongoDatabaseProtocol(Protocol):
    def list_collection_names(self) -> list[str]: ...
    def get_collection(self, name: str) -> MongoCollectionProtocol: ...


class MongoClientProtocol(Protocol):
    def get_database(self, name: str) -> MongoDatabaseProtocol: ...
    def close(self) -> object: ...


class Neo4jResultProtocol(Protocol):
    def data(self) -> list[object]: ...


class Neo4jSessionProtocol(Protocol):
    def run(self, query: str, parameters: Mapping[str, object] | None = None) -> Neo4jResultProtocol: ...
    def close(self) -> object: ...


class Neo4jDriverProtocol(Protocol):
    def session(self, *, database: str) -> Neo4jSessionProtocol: ...
    def close(self) -> object: ...


class RedisClientProtocol(Protocol):
    def scan_iter(self, *, match: str, count: int) -> Iterable[object]: ...
    def type(self, key: object) -> object: ...
    def hgetall(self, key: object) -> Mapping[object, object]: ...
    def get(self, key: object) -> object: ...
    def lrange(self, key: object, start: int, end: int) -> list[object]: ...
    def smembers(self, key: object) -> set[object]: ...
    def zrange(self, key: object, start: int, end: int) -> list[object]: ...
    def close(self) -> object: ...


class CassandraSessionProtocol(Protocol):
    def execute(self, query: str, parameters: object | None = None) -> Iterable[object]: ...


class CassandraClusterProtocol(Protocol):
    def shutdown(self) -> object: ...


class HBaseTableProtocol(Protocol):
    def scan(self, *, limit: int) -> Iterable[tuple[object, Mapping[object, object]]]: ...


class HBaseConnectionProtocol(Protocol):
    def tables(self) -> list[object]: ...
    def table(self, name: str) -> HBaseTableProtocol: ...
    def close(self) -> object: ...


def _parse_positive_int(raw_value: str | None, default_value: int) -> int:
    if raw_value is None:
        return default_value
    normalized = raw_value.strip()
    if not normalized:
        return default_value
    try:
        parsed = int(normalized)
    except ValueError:
        return default_value
    if parsed <= 0:
        return default_value
    return parsed


def _decode_text(raw_value: object) -> str:
    if isinstance(raw_value, bytes):
        return raw_value.decode("utf-8", errors="replace")
    if isinstance(raw_value, str):
        return raw_value
    return str(raw_value)


def _normalize_logical_type(raw_value: object) -> str:
    if isinstance(raw_value, bool):
        return "bool"
    if isinstance(raw_value, int):
        return "int"
    if isinstance(raw_value, float):
        return "float"
    if isinstance(raw_value, str):
        return "string"
    return "unknown"


def _clean_values(raw_values: list[object]) -> list[object]:
    cleaned_values: list[object] = []
    for value in raw_values:
        if value is None:
            continue
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                continue
            if normalized.upper() == "NULL":
                continue
        cleaned_values.append(value)
    return cleaned_values


def _sample_values(raw_values: list[object]) -> tuple[str, ...]:
    cleaned_values = _clean_values(raw_values)
    total = len(cleaned_values)
    if total == 0:
        return ()

    sample_size = max(int(total * DB_SAMPLE_RATIO), DB_SAMPLE_MIN)
    sample_size = min(sample_size, total, DB_SAMPLE_MAX)
    sampled = random.sample(cleaned_values, sample_size)

    output: list[str] = []
    for value in sampled:
        text = str(value).strip()
        if text:
            output.append(text)
    return tuple(output)


def _flatten_nested_fields(field_path: str, value: object, output: list[tuple[str, object]]) -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).strip()
            if not key:
                continue
            next_path = f"{field_path}.{key}" if field_path else key
            _flatten_nested_fields(next_path, child, output)
        return

    if isinstance(value, list):
        output.append((field_path, json.dumps(value, ensure_ascii=False, default=str)))
        return

    output.append((field_path, value))


def _row_value(row: object, index: int, key: str) -> object:
    if isinstance(row, Mapping):
        return row.get(key)
    if hasattr(row, key):
        return getattr(row, key)
    if isinstance(row, tuple) and len(row) > index:
        return row[index]
    return None


def _parse_mongodb_dsn(source: DatabaseSource) -> MongoDBDsn:
    normalized_dsn = source.dsn.strip()
    if not normalized_dsn:
        raise ValueError("source.dsn must not be empty")

    parsed = urlparse(normalized_dsn)
    if parsed.scheme.strip().lower() not in {"mongodb", "mongodb+srv"}:
        raise ValueError("mongodb dsn scheme must be 'mongodb' or 'mongodb+srv'")

    database = parsed.path.lstrip("/").strip() or source.options.get("database", "").strip()
    if not database:
        raise ValueError("mongodb dsn database must not be empty")

    return MongoDBDsn(uri=normalized_dsn, database=database)


def _parse_neo4j_dsn(source: DatabaseSource) -> Neo4jDsn:
    normalized_dsn = source.dsn.strip()
    if not normalized_dsn:
        raise ValueError("source.dsn must not be empty")

    parsed = urlparse(normalized_dsn)
    scheme = parsed.scheme.strip().lower()
    if scheme not in {"neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc"}:
        raise ValueError("neo4j dsn scheme is not supported")

    host = parsed.hostname.strip() if parsed.hostname else ""
    if not host:
        raise ValueError("neo4j dsn host must not be empty")

    username = unquote(parsed.username).strip() if parsed.username else ""
    if not username:
        username = source.options.get("username", "").strip()
    if not username:
        raise ValueError("neo4j username must not be empty")

    password = unquote(parsed.password).strip() if parsed.password else ""
    if not password:
        password = source.options.get("password", "").strip()
    if not password:
        raise ValueError("neo4j password must not be empty")

    database = parsed.path.lstrip("/").strip() or source.options.get("database", "").strip() or "neo4j"

    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("neo4j dsn port is invalid") from exc

    netloc = host if port is None else f"{host}:{port}"
    sanitized_uri = urlunparse(
        (
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )
    return Neo4jDsn(uri=sanitized_uri, username=username, password=password, database=database)


def _parse_redis_dsn(source: DatabaseSource) -> RedisDsn:
    normalized_dsn = source.dsn.strip()
    if not normalized_dsn:
        raise ValueError("source.dsn must not be empty")

    parsed = urlparse(normalized_dsn)
    if parsed.scheme.strip().lower() not in {"redis", "rediss"}:
        raise ValueError("redis dsn scheme must be 'redis' or 'rediss'")

    host = parsed.hostname.strip() if parsed.hostname else ""
    if not host:
        raise ValueError("redis dsn host must not be empty")

    return RedisDsn(uri=normalized_dsn)


def _parse_cassandra_dsn(source: DatabaseSource) -> CassandraDsn:
    normalized_dsn = source.dsn.strip()
    if not normalized_dsn:
        raise ValueError("source.dsn must not be empty")

    parsed = urlparse(normalized_dsn)
    if parsed.scheme.strip().lower() != "cassandra":
        raise ValueError("cassandra dsn scheme must be 'cassandra'")

    netloc = parsed.netloc.strip()
    if not netloc:
        raise ValueError("cassandra dsn host must not be empty")

    auth_part = ""
    hosts_part = netloc
    if "@" in netloc:
        auth_part, hosts_part = netloc.rsplit("@", 1)

    hosts: list[str] = []
    detected_port: int | None = None
    for item in hosts_part.split(","):
        token = item.strip()
        if not token:
            continue
        if ":" in token:
            host_text, port_text = token.rsplit(":", 1)
            host = host_text.strip()
            if host:
                hosts.append(host)
            detected_port = _parse_positive_int(port_text, 9042)
            continue
        hosts.append(token)

    if not hosts:
        raise ValueError("cassandra dsn host list must not be empty")

    keyspace = parsed.path.lstrip("/").strip() or source.options.get("keyspace", "").strip()
    if not keyspace:
        raise ValueError("cassandra dsn keyspace must not be empty")

    username: str | None = None
    password: str | None = None
    if auth_part and ":" in auth_part:
        username_text, password_text = auth_part.split(":", 1)
        username = unquote(username_text).strip() or None
        password = unquote(password_text).strip() or None

    if username is None:
        username = source.options.get("username", "").strip() or None
    if password is None:
        password = source.options.get("password", "").strip() or None

    port = detected_port if detected_port is not None else _parse_positive_int(source.options.get("port"), 9042)
    return CassandraDsn(
        hosts=tuple(hosts),
        port=port,
        keyspace=keyspace,
        username=username,
        password=password,
    )


def _parse_hbase_dsn(source: DatabaseSource) -> HBaseDsn:
    normalized_dsn = source.dsn.strip()
    if not normalized_dsn:
        raise ValueError("source.dsn must not be empty")

    parsed = urlparse(normalized_dsn)
    if parsed.scheme.strip().lower() not in {"hbase", "thrift"}:
        raise ValueError("hbase dsn scheme must be 'hbase' or 'thrift'")

    host = parsed.hostname.strip() if parsed.hostname else ""
    if not host:
        raise ValueError("hbase dsn host must not be empty")

    namespace = parsed.path.lstrip("/").strip() or source.options.get("namespace", "").strip() or None
    port = parsed.port if parsed.port is not None else _parse_positive_int(source.options.get("port"), 9090)
    if port <= 0:
        raise ValueError("hbase dsn port must be positive")

    return HBaseDsn(host=host, port=port, namespace=namespace)


def _connect_mongodb(dsn: MongoDBDsn, source_options: dict[str, str]) -> MongoClientProtocol:
    try:
        from pymongo import MongoClient
    except ImportError as exc:
        raise RuntimeError(
            "pymongo is required for mongodb adapter. Install dependency first, e.g. `uv add pymongo`."
        ) from exc

    connect_timeout_ms = _parse_positive_int(source_options.get("connect_timeout_ms"), 3000)
    socket_timeout_ms = _parse_positive_int(source_options.get("socket_timeout_ms"), 10000)
    return MongoClient(
        dsn.uri,
        serverSelectionTimeoutMS=connect_timeout_ms,
        socketTimeoutMS=socket_timeout_ms,
    )


def _connect_neo4j(dsn: Neo4jDsn, source_options: dict[str, str]) -> Neo4jDriverProtocol:
    del source_options
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise RuntimeError(
            "neo4j is required for neo4j adapter. Install dependency first, e.g. `uv add neo4j`."
        ) from exc

    return GraphDatabase.driver(dsn.uri, auth=(dsn.username, dsn.password))


def _connect_redis(dsn: RedisDsn, source_options: dict[str, str]) -> RedisClientProtocol:
    try:
        import redis
    except ImportError as exc:
        raise RuntimeError(
            "redis is required for redis adapter. Install dependency first, e.g. `uv add redis`."
        ) from exc

    connect_timeout_sec = _parse_positive_int(source_options.get("connect_timeout_sec"), 5)
    read_timeout_sec = _parse_positive_int(source_options.get("read_timeout_sec"), 10)
    return redis.Redis.from_url(
        dsn.uri,
        socket_connect_timeout=connect_timeout_sec,
        socket_timeout=read_timeout_sec,
        decode_responses=False,
    )


def _connect_cassandra(
    dsn: CassandraDsn,
    source_options: dict[str, str],
) -> tuple[CassandraClusterProtocol, CassandraSessionProtocol]:
    requested_connection_class = source_options.get("connection_class", "").strip().lower()
    if not requested_connection_class or requested_connection_class in {"auto", "gevent"}:
        _patch_gevent_socket_if_needed()

    try:
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster
    except ImportError as exc:
        raise RuntimeError(
            "cassandra-driver is required for cassandra adapter. Install dependency first, e.g. `uv add cassandra-driver`."
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            "cassandra-driver initialization failed. "
            "For Python 3.12, set source.options.connection_class=gevent "
            "and ensure gevent is installed."
        ) from exc

    connect_timeout_sec = _parse_positive_int(source_options.get("connect_timeout_sec"), 5)
    cluster_options: dict[str, object] = {"port": dsn.port, "connect_timeout": connect_timeout_sec}
    connection_class = _resolve_cassandra_connection_class(source_options)
    if connection_class is not None:
        cluster_options["connection_class"] = connection_class
    if dsn.username and dsn.password:
        cluster_options["auth_provider"] = PlainTextAuthProvider(username=dsn.username, password=dsn.password)

    try:
        cluster = Cluster(list(dsn.hosts), **cluster_options)
    except Exception as exc:
        if "Unable to load a default connection class" in str(exc):
            raise RuntimeError(
                "cassandra driver has no usable connection class in this runtime. "
                "Set source.options.connection_class to gevent/libev/asyncio, "
                "or install the required cassandra-driver optional components."
            ) from exc
        raise
    session = cluster.connect(dsn.keyspace)
    return cluster, session


def _patch_gevent_socket_if_needed() -> None:
    import socket

    try:
        import gevent.monkey
        import gevent.socket
    except ImportError:
        return

    if socket.socket is gevent.socket.socket:
        return

    gevent.monkey.patch_all(ssl=False)


def _resolve_cassandra_connection_class(source_options: dict[str, str]) -> type[object] | None:
    requested = source_options.get("connection_class", "").strip().lower()
    if not requested or requested == "auto":
        candidates = ("gevent", "libev", "asyncio")
    elif requested in {"gevent", "libev", "asyncio"}:
        candidates = (requested,)
    else:
        raise RuntimeError(
            "source.options.connection_class for cassandra must be one of: "
            "auto, gevent, libev, asyncio"
        )

    for candidate in candidates:
        if candidate == "gevent":
            try:
                from cassandra.io.geventreactor import GeventConnection
            except ImportError:
                continue
            return GeventConnection

        if candidate == "libev":
            try:
                from cassandra.io.libevreactor import LibevConnection
            except ImportError:
                continue
            return LibevConnection

        if candidate == "asyncio":
            try:
                from cassandra.io.asyncioreactor import AsyncioConnection
            except ImportError:
                continue
            return AsyncioConnection

    return None


def _connect_hbase(dsn: HBaseDsn, source_options: dict[str, str]) -> HBaseConnectionProtocol:
    try:
        import happybase
    except ImportError as exc:
        raise RuntimeError(
            "happybase is required for hbase adapter. Install dependency first, e.g. `uv add happybase`."
        ) from exc

    timeout_ms = _parse_positive_int(source_options.get("timeout_ms"), 10000)
    return happybase.Connection(host=dsn.host, port=dsn.port, timeout=timeout_ms)


class MongoDBAdapter(BaseAdapter):
    def extract_field_units(self) -> list[FieldUnit]:
        dsn = _parse_mongodb_dsn(self.source)
        client = _connect_mongodb(dsn, self.source.options)
        try:
            database = client.get_database(dsn.database)
            return self._extract_from_database(database)
        finally:
            client.close()

    def _extract_from_database(self, database: MongoDatabaseProtocol) -> list[FieldUnit]:
        max_documents = _parse_positive_int(self.source.options.get("max_documents"), 2000)

        values_by_field: dict[tuple[str, str], list[object]] = {}
        type_by_field: dict[tuple[str, str], str] = {}
        for collection_name in database.list_collection_names():
            if not collection_name.strip():
                continue

            collection = database.get_collection(collection_name)
            for document in collection.find({}, limit=max_documents):
                if not isinstance(document, Mapping):
                    continue

                flattened: list[tuple[str, object]] = []
                _flatten_nested_fields("", document, flattened)
                for field_path, value in flattened:
                    field_path_text = field_path.strip()
                    if not field_path_text or field_path_text == "_id":
                        continue

                    field_key = (collection_name, field_path_text)
                    if field_key not in values_by_field:
                        values_by_field[field_key] = []
                    values_by_field[field_key].append(value)
                    if field_key not in type_by_field and value is not None:
                        type_by_field[field_key] = _normalize_logical_type(value)

        field_units: list[FieldUnit] = []
        for (collection_name, field_path), values in values_by_field.items():
            samples = _sample_values(values)
            if not samples:
                continue

            field_units.append(
                FieldUnit(
                    source_name=self.source.name,
                    database_type=self.database_type,
                    container_name=collection_name,
                    field_path=field_path,
                    original_field=field_path,
                    field_origin="nested_key" if "." in field_path else "document_key",
                    logical_type=type_by_field.get((collection_name, field_path), "unknown"),
                    samples=samples,
                )
            )
        return field_units


class Neo4jAdapter(BaseAdapter):
    _NODE_FIELDS_QUERY = (
        "MATCH (n) "
        "UNWIND labels(n) AS label "
        "UNWIND keys(n) AS field "
        "RETURN DISTINCT label AS container, field AS field_path"
    )
    _REL_FIELDS_QUERY = (
        "MATCH ()-[r]-() "
        "UNWIND keys(r) AS field "
        "RETURN DISTINCT type(r) AS container, field AS field_path"
    )
    _NODE_VALUES_QUERY = (
        "MATCH (n) "
        "WHERE $container IN labels(n) AND n[$field] IS NOT NULL "
        "RETURN n[$field] AS value LIMIT $limit"
    )
    _REL_VALUES_QUERY = (
        "MATCH ()-[r]-() "
        "WHERE type(r) = $container AND r[$field] IS NOT NULL "
        "RETURN r[$field] AS value LIMIT $limit"
    )

    def extract_field_units(self) -> list[FieldUnit]:
        dsn = _parse_neo4j_dsn(self.source)
        driver = _connect_neo4j(dsn, self.source.options)
        try:
            session = driver.session(database=dsn.database)
            try:
                return self._extract_from_session(session)
            finally:
                session.close()
        finally:
            driver.close()

    def _extract_from_session(self, session: Neo4jSessionProtocol) -> list[FieldUnit]:
        row_limit = _parse_positive_int(self.source.options.get("row_limit"), 3000)
        field_units: list[FieldUnit] = []

        for meta in self._query_rows(session, self._NODE_FIELDS_QUERY, None):
            container = str(meta.get("container", "")).strip()
            field_path = str(meta.get("field_path", "")).strip()
            if not container or not field_path:
                continue

            values_rows = self._query_rows(
                session,
                self._NODE_VALUES_QUERY,
                {"container": container, "field": field_path, "limit": row_limit},
            )
            values = [row.get("value") for row in values_rows]
            samples = _sample_values(values)
            if not samples:
                continue

            logical_type = "unknown"
            for value in values:
                if value is not None:
                    logical_type = _normalize_logical_type(value)
                    break

            field_units.append(
                FieldUnit(
                    source_name=self.source.name,
                    database_type=self.database_type,
                    container_name=container,
                    field_path=field_path,
                    original_field=field_path,
                    field_origin="node_property",
                    logical_type=logical_type,
                    samples=samples,
                )
            )

        for meta in self._query_rows(session, self._REL_FIELDS_QUERY, None):
            container = str(meta.get("container", "")).strip()
            field_path = str(meta.get("field_path", "")).strip()
            if not container or not field_path:
                continue

            values_rows = self._query_rows(
                session,
                self._REL_VALUES_QUERY,
                {"container": container, "field": field_path, "limit": row_limit},
            )
            values = [row.get("value") for row in values_rows]
            samples = _sample_values(values)
            if not samples:
                continue

            logical_type = "unknown"
            for value in values:
                if value is not None:
                    logical_type = _normalize_logical_type(value)
                    break

            field_units.append(
                FieldUnit(
                    source_name=self.source.name,
                    database_type=self.database_type,
                    container_name=container,
                    field_path=field_path,
                    original_field=field_path,
                    field_origin="relationship_property",
                    logical_type=logical_type,
                    samples=samples,
                )
            )

        return field_units

    def _query_rows(
        self,
        session: Neo4jSessionProtocol,
        query: str,
        parameters: Mapping[str, object] | None,
    ) -> list[dict[str, object]]:
        try:
            rows = session.run(query, parameters).data()
        except Exception as exc:
            raise RuntimeError(f"neo4j query failed: {exc}") from exc

        normalized_rows: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            normalized_rows.append({str(key): value for key, value in row.items()})
        return normalized_rows


class RedisAdapter(BaseAdapter):
    def extract_field_units(self) -> list[FieldUnit]:
        dsn = _parse_redis_dsn(self.source)
        client = _connect_redis(dsn, self.source.options)
        try:
            return self._extract_from_client(client)
        finally:
            client.close()

    def _extract_from_client(self, client: RedisClientProtocol) -> list[FieldUnit]:
        pattern = self.source.options.get("key_pattern", "*")
        scan_count = _parse_positive_int(self.source.options.get("scan_count"), 200)
        key_limit = _parse_positive_int(self.source.options.get("key_limit"), 1000)

        field_units: list[FieldUnit] = []
        scanned = 0
        for raw_key in client.scan_iter(match=pattern, count=scan_count):
            if scanned >= key_limit:
                break
            scanned += 1

            key_name = _decode_text(raw_key).strip()
            if not key_name:
                continue

            key_type = _decode_text(client.type(raw_key)).strip().lower()
            if key_type == "hash":
                field_units.extend(self._extract_hash_fields(client, raw_key, key_name))
                continue
            if key_type == "string":
                field_units.extend(self._extract_string_fields(client, raw_key, key_name))
                continue
            if key_type == "list":
                values = [_decode_text(value) for value in client.lrange(raw_key, 0, -1)]
                field_units.extend(self._build_object_units(key_name, "value", values))
                continue
            if key_type == "set":
                values = [_decode_text(value) for value in client.smembers(raw_key)]
                field_units.extend(self._build_object_units(key_name, "value", values))
                continue
            if key_type == "zset":
                values = [_decode_text(value) for value in client.zrange(raw_key, 0, -1)]
                field_units.extend(self._build_object_units(key_name, "value", values))
                continue

        return field_units

    def _extract_hash_fields(
        self,
        client: RedisClientProtocol,
        raw_key: object,
        key_name: str,
    ) -> list[FieldUnit]:
        units: list[FieldUnit] = []
        for raw_field, raw_value in client.hgetall(raw_key).items():
            field_name = _decode_text(raw_field).strip()
            if not field_name:
                continue

            samples = _sample_values([_decode_text(raw_value)])
            if not samples:
                continue

            units.append(
                FieldUnit(
                    source_name=self.source.name,
                    database_type=self.database_type,
                    container_name=key_name,
                    field_path=field_name,
                    original_field=field_name,
                    field_origin="redis_hash_field",
                    logical_type="string",
                    samples=samples,
                )
            )
        return units

    def _extract_string_fields(
        self,
        client: RedisClientProtocol,
        raw_key: object,
        key_name: str,
    ) -> list[FieldUnit]:
        raw_value = client.get(raw_key)
        if raw_value is None:
            return []

        text_value = _decode_text(raw_value)
        try:
            parsed = json.loads(text_value)
        except json.JSONDecodeError:
            return self._build_object_units(key_name, "value", [text_value])

        flattened: list[tuple[str, object]] = []
        if isinstance(parsed, Mapping):
            _flatten_nested_fields("", parsed, flattened)
        else:
            flattened.append(("value", parsed))

        values_by_field: dict[str, list[object]] = {}
        for field_path, value in flattened:
            path = field_path.strip()
            if not path:
                continue
            if path not in values_by_field:
                values_by_field[path] = []
            values_by_field[path].append(value)

        units: list[FieldUnit] = []
        for field_path, values in values_by_field.items():
            samples = _sample_values(values)
            if not samples:
                continue

            logical_type = "unknown"
            for value in values:
                if value is not None:
                    logical_type = _normalize_logical_type(value)
                    break

            units.append(
                FieldUnit(
                    source_name=self.source.name,
                    database_type=self.database_type,
                    container_name=key_name,
                    field_path=field_path,
                    original_field=field_path,
                    field_origin="redis_json_property",
                    logical_type=logical_type,
                    samples=samples,
                )
            )
        return units

    def _build_object_units(self, key_name: str, field_path: str, values: list[object]) -> list[FieldUnit]:
        samples = _sample_values(values)
        if not samples:
            return []

        logical_type = "unknown"
        for value in values:
            if value is not None:
                logical_type = _normalize_logical_type(value)
                break

        return [
            FieldUnit(
                source_name=self.source.name,
                database_type=self.database_type,
                container_name=key_name,
                field_path=field_path,
                original_field=field_path,
                field_origin="redis_object_property",
                logical_type=logical_type,
                samples=samples,
            )
        ]


class CassandraAdapter(BaseAdapter):
    def extract_field_units(self) -> list[FieldUnit]:
        dsn = _parse_cassandra_dsn(self.source)
        cluster, session = _connect_cassandra(dsn, self.source.options)
        try:
            return self._extract_from_session(session, dsn.keyspace)
        finally:
            cluster.shutdown()

    def _extract_from_session(self, session: CassandraSessionProtocol, keyspace: str) -> list[FieldUnit]:
        metadata_query = (
            "SELECT table_name, column_name, type "
            "FROM system_schema.columns "
            "WHERE keyspace_name = %s"
        )
        try:
            metadata_rows = list(session.execute(metadata_query, (keyspace,)))
        except Exception as exc:
            raise RuntimeError(f"discover cassandra fields failed: {exc}") from exc

        row_limit = _parse_positive_int(self.source.options.get("row_limit"), 3000)
        field_units: list[FieldUnit] = []
        for row in metadata_rows:
            table_name = str(_row_value(row, 0, "table_name") or "").strip()
            column_name = str(_row_value(row, 1, "column_name") or "").strip()
            logical_type = str(_row_value(row, 2, "type") or "unknown").strip() or "unknown"
            if not table_name or not column_name:
                continue

            quoted_keyspace = '"' + keyspace.replace('"', '""') + '"'
            quoted_table = '"' + table_name.replace('"', '""') + '"'
            quoted_column = '"' + column_name.replace('"', '""') + '"'
            values_query = f"SELECT {quoted_column} FROM {quoted_keyspace}.{quoted_table} LIMIT {row_limit}"
            try:
                value_rows = list(session.execute(values_query))
            except Exception as exc:
                print(f"read field failed {table_name}.{column_name}: {exc}")
                continue

            values = [_row_value(row_item, 0, column_name) for row_item in value_rows]
            samples = _sample_values(values)
            if not samples:
                continue

            field_units.append(
                FieldUnit(
                    source_name=self.source.name,
                    database_type=self.database_type,
                    container_name=table_name,
                    field_path=column_name,
                    original_field=column_name,
                    field_origin="column",
                    logical_type=logical_type,
                    samples=samples,
                )
            )

        return field_units


class HBaseAdapter(BaseAdapter):
    def extract_field_units(self) -> list[FieldUnit]:
        dsn = _parse_hbase_dsn(self.source)
        connection = _connect_hbase(dsn, self.source.options)
        try:
            return self._extract_from_connection(connection, dsn.namespace)
        finally:
            connection.close()

    def _extract_from_connection(
        self,
        connection: HBaseConnectionProtocol,
        namespace: str | None,
    ) -> list[FieldUnit]:
        row_limit = _parse_positive_int(self.source.options.get("row_limit"), 3000)

        field_units: list[FieldUnit] = []
        for raw_table_name in connection.tables():
            table_name = _decode_text(raw_table_name).strip()
            if not table_name:
                continue

            if namespace and not table_name.startswith(f"{namespace}:"):
                continue

            table = connection.table(table_name)
            values_by_qualifier: dict[str, list[object]] = {}
            for _, columns in table.scan(limit=row_limit):
                for raw_qualifier, raw_value in columns.items():
                    qualifier = _decode_text(raw_qualifier).strip()
                    if not qualifier:
                        continue
                    if qualifier not in values_by_qualifier:
                        values_by_qualifier[qualifier] = []
                    values_by_qualifier[qualifier].append(_decode_text(raw_value))

            for qualifier, values in values_by_qualifier.items():
                samples = _sample_values(values)
                if not samples:
                    continue

                field_units.append(
                    FieldUnit(
                        source_name=self.source.name,
                        database_type=self.database_type,
                        container_name=table_name,
                        field_path=qualifier,
                        original_field=qualifier,
                        field_origin="hbase_family_qualifier",
                        logical_type="string",
                        samples=samples,
                    )
                )

        return field_units
