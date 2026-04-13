from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from pathlib import Path
from urllib.parse import ParseResult, parse_qs, unquote, urlparse

from src.db.plugin_registry import PROJECT_ROOT, DatabaseSource
from src.distributed.contracts import LocalSubQueryFilter


def _quote_ansi_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_backtick_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _quote_table_ref(table: str, quote_identifier: Callable[[str], str]) -> str:
    parts = [item.strip() for item in table.split(".") if item.strip()]
    if not parts:
        raise RuntimeError("table must not be empty")
    if len(parts) == 1:
        return quote_identifier(parts[0])
    if len(parts) == 2:
        return f"{quote_identifier(parts[0])}.{quote_identifier(parts[1])}"
    raise RuntimeError("table must be in form: table or schema.table")


def _resolve_port(parsed: ParseResult, default_port: int, *, context: str) -> int:
    try:
        parsed_port = parsed.port
    except ValueError as exc:
        raise RuntimeError(f"{context} has invalid port") from exc
    port = parsed_port if parsed_port is not None else default_port
    if port <= 0:
        raise RuntimeError(f"{context} port must be positive")
    return port


def _build_where_clause(
    *,
    filters: Sequence[LocalSubQueryFilter],
    quote_identifier: Callable[[str], str],
    placeholder: Callable[[], str],
) -> tuple[str, list[object]]:
    if not filters:
        return "", []

    clauses: list[str] = []
    params: list[object] = []
    for item in filters:
        field_sql = quote_identifier(item.field)
        op = item.operator
        if op == "eq":
            clauses.append(f"{field_sql} = {placeholder()}")
            params.append(item.value)
            continue
        if op == "neq":
            clauses.append(f"{field_sql} <> {placeholder()}")
            params.append(item.value)
            continue
        if op == "gt":
            clauses.append(f"{field_sql} > {placeholder()}")
            params.append(item.value)
            continue
        if op == "gte":
            clauses.append(f"{field_sql} >= {placeholder()}")
            params.append(item.value)
            continue
        if op == "lt":
            clauses.append(f"{field_sql} < {placeholder()}")
            params.append(item.value)
            continue
        if op == "lte":
            clauses.append(f"{field_sql} <= {placeholder()}")
            params.append(item.value)
            continue
        if op == "like":
            clauses.append(f"{field_sql} LIKE {placeholder()}")
            params.append(str(item.value))
            continue
        if op == "in":
            if not isinstance(item.value, tuple):
                raise RuntimeError("filter operator 'in' requires tuple value")
            placeholders = [placeholder() for _ in item.value]
            clause = f"{field_sql} IN ({', '.join(placeholders)})"
            clauses.append(clause)
            params.extend(item.value)
            continue
        raise RuntimeError(f"unsupported filter operator: {op}")

    return " WHERE " + " AND ".join(clauses), params


def _build_named_where_clause(
    *,
    filters: Sequence[LocalSubQueryFilter],
    quote_identifier: Callable[[str], str],
    placeholder: Callable[[str], str],
) -> tuple[str, dict[str, object]]:
    if not filters:
        return "", {}

    clauses: list[str] = []
    params: dict[str, object] = {}
    counter = 0
    for item in filters:
        field_sql = quote_identifier(item.field)
        op = item.operator

        if op == "in":
            if not isinstance(item.value, tuple):
                raise RuntimeError("filter operator 'in' requires tuple value")
            if not item.value:
                raise RuntimeError("filter operator 'in' requires non-empty tuple")
            names: list[str] = []
            for raw_value in item.value:
                param_name = f"p{counter}"
                counter += 1
                params[param_name] = raw_value
                names.append(placeholder(param_name))
            clauses.append(f"{field_sql} IN ({', '.join(names)})")
            continue

        param_name = f"p{counter}"
        counter += 1
        params[param_name] = item.value
        marker = placeholder(param_name)
        if op == "eq":
            clauses.append(f"{field_sql} = {marker}")
            continue
        if op == "neq":
            clauses.append(f"{field_sql} <> {marker}")
            continue
        if op == "gt":
            clauses.append(f"{field_sql} > {marker}")
            continue
        if op == "gte":
            clauses.append(f"{field_sql} >= {marker}")
            continue
        if op == "lt":
            clauses.append(f"{field_sql} < {marker}")
            continue
        if op == "lte":
            clauses.append(f"{field_sql} <= {marker}")
            continue
        if op == "like":
            clauses.append(f"{field_sql} LIKE {marker}")
            params[param_name] = str(item.value)
            continue
        raise RuntimeError(f"unsupported filter operator: {op}")

    return " WHERE " + " AND ".join(clauses), params


def _rows_to_dicts(
    *,
    columns: Sequence[str],
    rows: Sequence[Sequence[object]],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        row_data: dict[str, object] = {}
        for index, column in enumerate(columns):
            value = row[index] if index < len(row) else None
            row_data[column] = _json_safe_value(value)
        output.append(row_data)
    return output


def _json_safe_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    return str(value)


def _to_comparable_scalar(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ""
        lowered = stripped.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        try:
            if "." in stripped:
                return float(stripped)
            return int(stripped)
        except ValueError:
            return stripped
    return str(value)


def _matches_filter_value(row_value: object, item: LocalSubQueryFilter) -> bool:
    current = _to_comparable_scalar(row_value)
    target = item.value
    operator = item.operator

    if operator == "in":
        if not isinstance(target, tuple):
            return False
        allowed = {_to_comparable_scalar(value) for value in target}
        return current in allowed

    wanted = _to_comparable_scalar(target)
    if operator == "eq":
        return current == wanted
    if operator == "neq":
        return current != wanted
    if operator == "gt":
        return isinstance(current, int | float) and isinstance(wanted, int | float) and current > wanted
    if operator == "gte":
        return (
            isinstance(current, int | float)
            and isinstance(wanted, int | float)
            and current >= wanted
        )
    if operator == "lt":
        return isinstance(current, int | float) and isinstance(wanted, int | float) and current < wanted
    if operator == "lte":
        return (
            isinstance(current, int | float)
            and isinstance(wanted, int | float)
            and current <= wanted
        )
    if operator == "like":
        if current is None:
            return False
        return str(wanted).lower() in str(current).lower()
    return False


def _matches_filters(
    *,
    row: dict[str, object],
    filters: Sequence[LocalSubQueryFilter],
) -> bool:
    for item in filters:
        if item.field not in row:
            return False
        if not _matches_filter_value(row[item.field], item):
            return False
    return True


def _project_row(*, row: dict[str, object], select_fields: Sequence[str]) -> dict[str, object]:
    projected: dict[str, object] = {}
    for field in select_fields:
        projected[field] = _json_safe_value(row.get(field))
    return projected


def _decode_binary(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _decode_maybe_json(value: object) -> object:
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    elif isinstance(value, str):
        text = value
    else:
        return _json_safe_value(value)

    stripped = text.strip()
    if not stripped:
        return ""
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            return _json_safe_value(json.loads(stripped))
        except Exception:  # noqa: BLE001
            return stripped
    return stripped


class BaseQueryExecutor(ABC):
    def __init__(self, source: DatabaseSource) -> None:
        self._source = source

    @abstractmethod
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        ...


class SQLiteQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        db_path = Path(self._source.dsn.strip())
        if not db_path.is_absolute():
            db_path = (PROJECT_ROOT / db_path).resolve()

        select_sql = ", ".join(_quote_ansi_identifier(field) for field in select_fields)
        where_sql, params = _build_where_clause(
            filters=filters,
            quote_identifier=_quote_ansi_identifier,
            placeholder=lambda: "?",
        )
        table_sql = _quote_ansi_identifier(table)
        sql = f"SELECT {select_sql} FROM {table_sql}{where_sql} LIMIT {int(limit)}"

        try:
            connection = sqlite3.connect(str(db_path))
        except sqlite3.Error as exc:
            raise RuntimeError(f"sqlite connection failed: {exc}") from exc

        try:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            columns = [str(item[0]) for item in (cursor.description or ())]
            return _rows_to_dicts(columns=columns, rows=rows)
        except sqlite3.Error as exc:
            raise RuntimeError(f"sqlite query failed: {exc}") from exc
        finally:
            connection.close()


class MySQLQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        parsed = urlparse(self._source.dsn.strip())
        scheme = parsed.scheme.strip().lower()
        if scheme not in {"mysql", "tidb"}:
            raise RuntimeError("mysql executor requires mysql:// or tidb:// dsn")

        host = parsed.hostname.strip() if parsed.hostname else ""
        if not host:
            raise RuntimeError("mysql dsn host must not be empty")
        username = unquote(parsed.username).strip() if parsed.username else ""
        if not username:
            raise RuntimeError("mysql dsn username must not be empty")
        password = unquote(parsed.password) if parsed.password else ""
        database = parsed.path.lstrip("/").strip()
        if not database:
            raise RuntimeError("mysql dsn database must not be empty")
        port = _resolve_port(parsed, 3306, context="mysql dsn")

        try:
            import pymysql
        except ImportError as exc:
            raise RuntimeError("pymysql is required for mysql/tidb query execution") from exc

        select_sql = ", ".join(_quote_backtick_identifier(field) for field in select_fields)
        where_sql, params = _build_where_clause(
            filters=filters,
            quote_identifier=_quote_backtick_identifier,
            placeholder=lambda: "%s",
        )
        table_sql = _quote_backtick_identifier(table)
        database_sql = _quote_backtick_identifier(database)
        sql = f"SELECT {select_sql} FROM {database_sql}.{table_sql}{where_sql} LIMIT {int(limit)}"

        connection = pymysql.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database,
            charset="utf8mb4",
        )
        try:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            columns = [str(item[0]) for item in (cursor.description or ())]
            return _rows_to_dicts(columns=columns, rows=rows)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"mysql query failed: {exc}") from exc
        finally:
            connection.close()


class PostgreSQLQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        parsed = urlparse(self._source.dsn.strip())
        scheme = parsed.scheme.strip().lower()
        if scheme not in {"postgresql", "postgres"}:
            raise RuntimeError("postgresql executor requires postgresql:// or postgres:// dsn")

        host = parsed.hostname.strip() if parsed.hostname else ""
        if not host:
            raise RuntimeError("postgresql dsn host must not be empty")
        username = unquote(parsed.username).strip() if parsed.username else ""
        if not username:
            raise RuntimeError("postgresql dsn username must not be empty")
        password = unquote(parsed.password) if parsed.password else ""
        database = parsed.path.lstrip("/").strip()
        if not database:
            raise RuntimeError("postgresql dsn database must not be empty")
        port = _resolve_port(parsed, 5432, context="postgresql dsn")
        schema = self._source.options.get("schema", "").strip() or "public"

        connection: object
        cursor: object
        try:
            import psycopg

            connection = psycopg.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                dbname=database,
            )
            cursor = connection.cursor()
        except ImportError:
            try:
                import psycopg2
            except ImportError as exc:
                raise RuntimeError("psycopg or psycopg2 is required for postgresql query execution") from exc
            connection = psycopg2.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                dbname=database,
            )
            cursor = connection.cursor()

        select_sql = ", ".join(_quote_ansi_identifier(field) for field in select_fields)
        where_sql, params = _build_where_clause(
            filters=filters,
            quote_identifier=_quote_ansi_identifier,
            placeholder=lambda: "%s",
        )
        sql = (
            f"SELECT {select_sql} "
            f"FROM {_quote_ansi_identifier(schema)}.{_quote_ansi_identifier(table)}"
            f"{where_sql} LIMIT {int(limit)}"
        )

        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            columns = [str(item[0]) for item in (cursor.description or ())]
            return _rows_to_dicts(columns=columns, rows=rows)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"postgresql query failed: {exc}") from exc
        finally:
            close_cursor = getattr(cursor, "close", None)
            if callable(close_cursor):
                close_cursor()
            close_connection = getattr(connection, "close", None)
            if callable(close_connection):
                close_connection()


class MongoQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        try:
            from pymongo import MongoClient
        except ImportError as exc:
            raise RuntimeError("pymongo is required for mongodb query execution") from exc

        parsed = urlparse(self._source.dsn.strip())
        database = parsed.path.lstrip("/").strip() or self._source.options.get("database", "").strip()
        if not database:
            raise RuntimeError("mongodb dsn must include database path or options.database")

        query_filter: dict[str, object] = {}
        for item in filters:
            if item.operator == "eq":
                query_filter[item.field] = item.value
            elif item.operator == "neq":
                query_filter[item.field] = {"$ne": item.value}
            elif item.operator == "gt":
                query_filter[item.field] = {"$gt": item.value}
            elif item.operator == "gte":
                query_filter[item.field] = {"$gte": item.value}
            elif item.operator == "lt":
                query_filter[item.field] = {"$lt": item.value}
            elif item.operator == "lte":
                query_filter[item.field] = {"$lte": item.value}
            elif item.operator == "like":
                query_filter[item.field] = {"$regex": str(item.value)}
            elif item.operator == "in":
                values = list(item.value) if isinstance(item.value, tuple) else []
                query_filter[item.field] = {"$in": values}
            else:
                raise RuntimeError(f"unsupported mongodb filter operator: {item.operator}")

        projection = dict.fromkeys(select_fields, 1)
        client = MongoClient(self._source.dsn.strip())
        try:
            collection = client.get_database(database).get_collection(table)
            docs = list(collection.find(query_filter, projection=projection, limit=int(limit)))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"mongodb query failed: {exc}") from exc
        finally:
            client.close()

        rows: list[dict[str, object]] = []
        for doc in docs:
            row: dict[str, object] = {}
            for field in select_fields:
                row[field] = _json_safe_value(doc.get(field))
            rows.append(row)
        return rows


class Neo4jQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        try:
            from neo4j import GraphDatabase
        except ImportError as exc:
            raise RuntimeError("neo4j package is required for neo4j query execution") from exc

        parsed = urlparse(self._source.dsn.strip())
        scheme = parsed.scheme.strip().lower()
        if scheme not in {"neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc"}:
            raise RuntimeError("neo4j dsn scheme is not supported")

        host = parsed.hostname.strip() if parsed.hostname else ""
        if not host:
            raise RuntimeError("neo4j dsn host must not be empty")
        username = unquote(parsed.username).strip() if parsed.username else ""
        if not username:
            username = self._source.options.get("username", "").strip()
        password = unquote(parsed.password).strip() if parsed.password else ""
        if not password:
            password = self._source.options.get("password", "").strip()
        if not username or not password:
            raise RuntimeError("neo4j username/password must not be empty")
        database = parsed.path.lstrip("/").strip() or self._source.options.get("database", "").strip() or "neo4j"

        try:
            port = parsed.port
        except ValueError as exc:
            raise RuntimeError("neo4j dsn has invalid port") from exc
        netloc = host if port is None else f"{host}:{port}"
        uri = f"{parsed.scheme}://{netloc}"

        where_parts: list[str] = []
        params: dict[str, object] = {"limit": int(limit)}
        for index, item in enumerate(filters):
            key = f"p{index}"
            field_ref = f"n.`{item.field}`"
            if item.operator == "eq":
                where_parts.append(f"{field_ref} = ${key}")
                params[key] = item.value
            elif item.operator == "neq":
                where_parts.append(f"{field_ref} <> ${key}")
                params[key] = item.value
            elif item.operator == "gt":
                where_parts.append(f"{field_ref} > ${key}")
                params[key] = item.value
            elif item.operator == "gte":
                where_parts.append(f"{field_ref} >= ${key}")
                params[key] = item.value
            elif item.operator == "lt":
                where_parts.append(f"{field_ref} < ${key}")
                params[key] = item.value
            elif item.operator == "lte":
                where_parts.append(f"{field_ref} <= ${key}")
                params[key] = item.value
            elif item.operator == "like":
                where_parts.append(f"toString({field_ref}) CONTAINS toString(${key})")
                params[key] = item.value
            elif item.operator == "in":
                values = list(item.value) if isinstance(item.value, tuple) else []
                where_parts.append(f"{field_ref} IN ${key}")
                params[key] = values
            else:
                raise RuntimeError(f"unsupported neo4j filter operator: {item.operator}")

        where_clause = ""
        if where_parts:
            where_clause = " WHERE " + " AND ".join(where_parts)

        projection = ", ".join([f"`{field}`: n.`{field}`" for field in select_fields])
        query = (
            f"MATCH (n:`{table}`)"
            f"{where_clause} "
            f"RETURN {{{projection}}} AS row "
            "LIMIT $limit"
        )

        driver = GraphDatabase.driver(uri, auth=(username, password))
        try:
            session = driver.session(database=database)
            try:
                records = session.run(query, parameters=params).data()
            finally:
                session.close()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"neo4j query failed: {exc}") from exc
        finally:
            driver.close()

        rows: list[dict[str, object]] = []
        for record in records:
            row_obj = record.get("row")
            if not isinstance(row_obj, dict):
                continue
            row: dict[str, object] = {}
            for field in select_fields:
                row[field] = _json_safe_value(row_obj.get(field))
            rows.append(row)
        return rows


class ClickHouseQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        try:
            from clickhouse_driver import Client
        except ImportError as exc:
            raise RuntimeError("clickhouse-driver is required for clickhouse query execution") from exc

        parsed = urlparse(self._source.dsn.strip())
        scheme = parsed.scheme.strip().lower()
        if scheme not in {"clickhouse", "ch"}:
            raise RuntimeError("clickhouse executor requires clickhouse:// or ch:// dsn")

        host = parsed.hostname.strip() if parsed.hostname else ""
        if not host:
            raise RuntimeError("clickhouse dsn host must not be empty")
        port = _resolve_port(parsed, 9000, context="clickhouse dsn")

        username = unquote(parsed.username).strip() if parsed.username else ""
        if not username:
            username = self._source.options.get("username", "").strip() or "default"
        password = unquote(parsed.password) if parsed.password else ""
        if not password:
            password = self._source.options.get("password", "")
        database = parsed.path.lstrip("/").strip() or self._source.options.get("database", "").strip()
        if not database:
            database = "default"

        select_sql = ", ".join(_quote_ansi_identifier(field) for field in select_fields)
        where_sql, params = _build_named_where_clause(
            filters=filters,
            quote_identifier=_quote_ansi_identifier,
            placeholder=lambda name: f"%({name})s",
        )
        table_sql = _quote_table_ref(table, _quote_ansi_identifier)
        sql = f"SELECT {select_sql} FROM {table_sql}{where_sql} LIMIT {int(limit)}"

        client = Client(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database,
        )
        try:
            rows = client.execute(sql, params)
            return _rows_to_dicts(columns=select_fields, rows=rows)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"clickhouse query failed: {exc}") from exc


class OracleQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        try:
            import oracledb
        except ImportError as exc:
            raise RuntimeError("oracledb is required for oracle query execution") from exc

        parsed = urlparse(self._source.dsn.strip())
        scheme = parsed.scheme.strip().lower()
        if scheme != "oracle":
            raise RuntimeError("oracle executor requires oracle:// dsn")

        host = parsed.hostname.strip() if parsed.hostname else ""
        if not host:
            raise RuntimeError("oracle dsn host must not be empty")
        port = _resolve_port(parsed, 1521, context="oracle dsn")

        username = unquote(parsed.username).strip() if parsed.username else ""
        if not username:
            username = self._source.options.get("username", "").strip()
        password = unquote(parsed.password).strip() if parsed.password else ""
        if not password:
            password = self._source.options.get("password", "").strip()
        if not username or not password:
            raise RuntimeError("oracle username/password must not be empty")

        service_name = (
            parsed.path.lstrip("/").strip()
            or self._source.options.get("service_name", "").strip()
            or self._source.options.get("sid", "").strip()
        )
        if not service_name:
            raise RuntimeError("oracle dsn must include service name in path or options.service_name")
        dsn = f"{host}:{port}/{service_name}"

        select_sql = ", ".join(_quote_ansi_identifier(field) for field in select_fields)
        where_sql, params = _build_named_where_clause(
            filters=filters,
            quote_identifier=_quote_ansi_identifier,
            placeholder=lambda name: f":{name}",
        )
        table_sql = _quote_table_ref(table, _quote_ansi_identifier)
        sql = (
            f"SELECT {select_sql} FROM {table_sql}{where_sql} "
            f"FETCH FIRST {int(limit)} ROWS ONLY"
        )

        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                return _rows_to_dicts(columns=select_fields, rows=rows)
            finally:
                cursor.close()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"oracle query failed: {exc}") from exc
        finally:
            connection.close()


class RedisQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("redis package is required for redis query execution") from exc

        dsn = self._source.dsn.strip()
        if not dsn:
            raise RuntimeError("redis dsn must not be empty")

        client = redis.Redis.from_url(dsn)
        key_pattern = self._source.options.get("key_pattern", "").strip() or f"{table}:*"

        requested_keys: list[str] = []
        for item in filters:
            if item.field != "row_key":
                continue
            if item.operator == "eq":
                requested_keys.append(str(item.value))
                break
            if item.operator == "in" and isinstance(item.value, tuple):
                requested_keys.extend(str(value) for value in item.value)
                break

        rows: list[dict[str, object]] = []
        raw_key_iter: Sequence[object]
        if requested_keys:
            raw_key_iter = requested_keys
        else:
            raw_key_iter = list(client.scan_iter(match=key_pattern, count=max(64, limit * 2)))

        for raw_key in raw_key_iter:
            key_text = _decode_binary(raw_key)
            if not key_text:
                continue
            try:
                row = self._load_redis_row(client=client, key=key_text)
            except Exception:  # noqa: BLE001
                continue
            if row is None:
                continue
            if not _matches_filters(row=row, filters=filters):
                continue
            rows.append(_project_row(row=row, select_fields=select_fields))
            if len(rows) >= limit:
                break

        return rows

    def _load_redis_row(self, *, client, key: str) -> dict[str, object] | None:
        key_type_obj = client.type(key)
        key_type = _decode_binary(key_type_obj).lower()
        row: dict[str, object] = {"row_key": key}

        if key_type == "hash":
            payload = client.hgetall(key)
            for raw_field, raw_value in payload.items():
                row[_decode_binary(raw_field)] = _decode_maybe_json(raw_value)
            return row

        if key_type == "string":
            row["value"] = _decode_maybe_json(client.get(key))
            return row

        if key_type == "list":
            row["items"] = [_decode_maybe_json(item) for item in client.lrange(key, 0, -1)]
            return row

        if key_type == "set":
            members = [_decode_maybe_json(item) for item in client.smembers(key)]
            row["items"] = sorted(str(item) for item in members)
            return row

        if key_type == "zset":
            members = [_decode_maybe_json(item) for item in client.zrange(key, 0, -1)]
            row["items"] = members
            return row

        return None


class CassandraQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        try:
            from cassandra.auth import PlainTextAuthProvider
            from cassandra.cluster import Cluster
        except ImportError as exc:
            raise RuntimeError("cassandra-driver is required for cassandra query execution") from exc

        parsed = urlparse(self._source.dsn.strip())
        scheme = parsed.scheme.strip().lower()
        if scheme not in {"cassandra", "scylla"}:
            raise RuntimeError("cassandra executor requires cassandra:// or scylla:// dsn")

        host = parsed.hostname.strip() if parsed.hostname else ""
        if not host:
            raise RuntimeError("cassandra dsn host must not be empty")
        port = _resolve_port(parsed, 9042, context="cassandra dsn")
        keyspace = parsed.path.lstrip("/").strip() or self._source.options.get("keyspace", "").strip()
        if not keyspace:
            raise RuntimeError("cassandra dsn must include keyspace path or options.keyspace")

        username = unquote(parsed.username).strip() if parsed.username else ""
        if not username:
            username = self._source.options.get("username", "").strip()
        password = unquote(parsed.password).strip() if parsed.password else ""
        if not password:
            password = self._source.options.get("password", "").strip()

        query_params = parse_qs(parsed.query)
        if "port" in query_params and query_params["port"]:
            try:
                port = int(query_params["port"][0])
            except ValueError as exc:
                raise RuntimeError("cassandra dsn query port must be an integer") from exc

        auth_provider = None
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
        cluster = Cluster(contact_points=[host], port=port, auth_provider=auth_provider)
        session = cluster.connect(keyspace)

        select_sql = ", ".join(_quote_ansi_identifier(field) for field in select_fields)
        where_sql, params = _build_where_clause(
            filters=filters,
            quote_identifier=_quote_ansi_identifier,
            placeholder=lambda: "%s",
        )
        table_sql = _quote_table_ref(table, _quote_ansi_identifier)
        sql = (
            f"SELECT {select_sql} FROM {table_sql}"
            f"{where_sql} LIMIT {int(limit)} ALLOW FILTERING"
        )

        try:
            records = session.execute(sql, params)
            rows: list[dict[str, object]] = []
            for record in records:
                row: dict[str, object] = {}
                for field in select_fields:
                    row[field] = _json_safe_value(getattr(record, field, None))
                rows.append(row)
            return rows
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"cassandra query failed: {exc}") from exc
        finally:
            session.shutdown()
            cluster.shutdown()


class HBaseQueryExecutor(BaseQueryExecutor):
    def execute(
        self,
        *,
        table: str,
        select_fields: tuple[str, ...],
        filters: tuple[LocalSubQueryFilter, ...],
        limit: int,
    ) -> list[dict[str, object]]:
        try:
            import happybase
        except ImportError as exc:
            raise RuntimeError("happybase is required for hbase query execution") from exc

        parsed = urlparse(self._source.dsn.strip())
        scheme = parsed.scheme.strip().lower()
        if scheme not in {"hbase", "thrift"}:
            raise RuntimeError("hbase executor requires hbase:// or thrift:// dsn")

        host = parsed.hostname.strip() if parsed.hostname else ""
        if not host:
            raise RuntimeError("hbase dsn host must not be empty")
        port = _resolve_port(parsed, 9090, context="hbase dsn")

        connection = happybase.Connection(host=host, port=port, autoconnect=True)
        htable = connection.table(table)

        requested_keys: list[str] = []
        for item in filters:
            if item.field != "row_key":
                continue
            if item.operator == "eq":
                requested_keys.append(str(item.value))
                break
            if item.operator == "in" and isinstance(item.value, tuple):
                requested_keys.extend(str(value) for value in item.value)
                break

        rows: list[dict[str, object]] = []
        try:
            if requested_keys:
                for key_text in requested_keys:
                    raw_data = htable.row(key_text.encode("utf-8"))
                    if not raw_data:
                        continue
                    row = self._decode_hbase_row(row_key=key_text, raw_data=raw_data)
                    if not _matches_filters(row=row, filters=filters):
                        continue
                    rows.append(_project_row(row=row, select_fields=select_fields))
                    if len(rows) >= limit:
                        break
            else:
                for raw_key, raw_data in htable.scan(limit=max(limit * 5, 100)):
                    row = self._decode_hbase_row(row_key=_decode_binary(raw_key), raw_data=raw_data)
                    if not _matches_filters(row=row, filters=filters):
                        continue
                    rows.append(_project_row(row=row, select_fields=select_fields))
                    if len(rows) >= limit:
                        break
            return rows
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"hbase query failed: {exc}") from exc
        finally:
            connection.close()

    def _decode_hbase_row(self, *, row_key: str, raw_data: dict[object, object]) -> dict[str, object]:
        row: dict[str, object] = {"row_key": row_key}
        for raw_column, raw_value in raw_data.items():
            row[_decode_binary(raw_column)] = _decode_maybe_json(raw_value)
        return row


def create_query_executor(source: DatabaseSource) -> BaseQueryExecutor:
    driver = source.driver.strip().lower()
    if driver == "sqlite":
        return SQLiteQueryExecutor(source)
    if driver in {"mysql", "tidb"}:
        return MySQLQueryExecutor(source)
    if driver in {"postgresql", "postgres"}:
        return PostgreSQLQueryExecutor(source)
    if driver == "mongodb":
        return MongoQueryExecutor(source)
    if driver == "neo4j":
        return Neo4jQueryExecutor(source)
    if driver in {"clickhouse", "ch"}:
        return ClickHouseQueryExecutor(source)
    if driver == "oracle":
        return OracleQueryExecutor(source)
    if driver == "redis":
        return RedisQueryExecutor(source)
    if driver in {"cassandra", "scylla"}:
        return CassandraQueryExecutor(source)
    if driver in {"hbase", "thrift"}:
        return HBaseQueryExecutor(source)
    raise RuntimeError(f"query executor is not implemented for driver: {source.driver}")
