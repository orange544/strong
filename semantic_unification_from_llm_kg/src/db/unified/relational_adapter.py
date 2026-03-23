from __future__ import annotations

import random
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import ParseResult, parse_qs, unquote, urlparse

from src.configs.config import DB_SAMPLE_MAX, DB_SAMPLE_MIN, DB_SAMPLE_RATIO
from src.db.plugin_registry import DatabaseSource, SQLiteDatabasePlugin
from src.db.unified.base_adapter import BaseAdapter
from src.db.unified.field_unit import DatabaseType, FieldUnit


@dataclass(frozen=True, slots=True)
class RelationalColumn:
    table_name: str
    column_name: str
    logical_type: str


@dataclass(frozen=True, slots=True)
class MySQLLikeDsn:
    driver: str
    host: str
    port: int
    username: str
    password: str
    database: str
    charset: str


@dataclass(frozen=True, slots=True)
class PostgreSQLDsn:
    host: str
    port: int
    username: str
    password: str
    database: str
    schema: str
    sslmode: str | None


@dataclass(frozen=True, slots=True)
class OracleDsn:
    host: str
    port: int
    username: str
    password: str
    service_name: str
    owner: str


@dataclass(frozen=True, slots=True)
class ClickHouseDsn:
    host: str
    port: int
    username: str
    password: str
    database: str
    secure: bool


class DBAPICursor(Protocol):
    def execute(self, query: str, params: object | None = None) -> object: ...
    def fetchall(self) -> list[tuple[object, ...]]: ...


class DBAPIConnection(Protocol):
    def cursor(self) -> DBAPICursor: ...
    def close(self) -> object: ...


def _quote_ansi_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _quote_backtick_identifier(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _quote_sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _clean_column_values(raw_values: list[object]) -> list[object]:
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


def _calculate_sample_size(total: int) -> int:
    if total <= 0:
        return 0
    sampled_size = max(int(total * DB_SAMPLE_RATIO), DB_SAMPLE_MIN)
    sampled_size = min(sampled_size, total, DB_SAMPLE_MAX)
    return sampled_size


def _random_sample_values(raw_values: list[object]) -> tuple[str, ...]:
    cleaned_values = _clean_column_values(raw_values)
    total = len(cleaned_values)
    if total == 0:
        return ()

    sampled_size = _calculate_sample_size(total)
    sampled_values = random.sample(cleaned_values, sampled_size)

    normalized_samples: list[str] = []
    for value in sampled_values:
        sample_text = str(value).strip()
        if sample_text:
            normalized_samples.append(sample_text)
    return tuple(normalized_samples)


def _normalize_logical_type(raw_value: object) -> str:
    if not isinstance(raw_value, str):
        return "unknown"
    logical_type = raw_value.strip()
    return logical_type if logical_type else "unknown"


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


def _parse_bool(raw_value: str | None, default_value: bool) -> bool:
    if raw_value is None:
        return default_value
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default_value


def _first_query_param(params: dict[str, list[str]], key: str) -> str | None:
    values = params.get(key, [])
    if not values:
        return None
    value = values[0].strip()
    return value if value else None


def _resolve_port(parsed: ParseResult, default_port: int, driver_name: str) -> int:
    try:
        port_candidate = parsed.port
    except ValueError as exc:
        raise ValueError(f"{driver_name} dsn port is invalid") from exc

    port = port_candidate if port_candidate is not None else default_port
    if port <= 0:
        raise ValueError(f"{driver_name} dsn port must be positive")
    return port


def _parse_mysql_like_dsn(source: DatabaseSource) -> MySQLLikeDsn:
    normalized_dsn = source.dsn.strip()
    if not normalized_dsn:
        raise ValueError("source.dsn must not be empty")

    parsed = urlparse(normalized_dsn)
    scheme = parsed.scheme.strip().lower()

    normalized_driver = source.driver.strip().lower()
    if normalized_driver == "mysql":
        allowed_schemes = {"mysql"}
    elif normalized_driver == "tidb":
        allowed_schemes = {"tidb", "mysql"}
    else:
        raise ValueError(f"unsupported mysql-like driver: {source.driver}")

    if scheme not in allowed_schemes:
        allowed_scheme_text = ", ".join(sorted(allowed_schemes))
        raise ValueError(
            f"dsn scheme '{scheme}' does not match driver '{normalized_driver}', "
            f"allowed schemes: {allowed_scheme_text}"
        )

    host = parsed.hostname.strip() if parsed.hostname else ""
    if not host:
        raise ValueError("mysql/tidb dsn host must not be empty")

    username = unquote(parsed.username).strip() if parsed.username else ""
    if not username:
        raise ValueError("mysql/tidb dsn username must not be empty")

    password = unquote(parsed.password) if parsed.password else ""
    port = _resolve_port(parsed, 3306, "mysql/tidb")

    database = parsed.path.lstrip("/").strip()
    if not database:
        raise ValueError("mysql/tidb dsn database name must not be empty")

    query_params = parse_qs(parsed.query, keep_blank_values=True)
    charset = _first_query_param(query_params, "charset") or "utf8mb4"

    return MySQLLikeDsn(
        driver=normalized_driver,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        charset=charset,
    )


def _parse_postgresql_dsn(source: DatabaseSource) -> PostgreSQLDsn:
    normalized_dsn = source.dsn.strip()
    if not normalized_dsn:
        raise ValueError("source.dsn must not be empty")

    parsed = urlparse(normalized_dsn)
    scheme = parsed.scheme.strip().lower()
    if scheme not in {"postgresql", "postgres"}:
        raise ValueError("postgresql dsn scheme must be 'postgresql' or 'postgres'")

    host = parsed.hostname.strip() if parsed.hostname else ""
    if not host:
        raise ValueError("postgresql dsn host must not be empty")

    username = unquote(parsed.username).strip() if parsed.username else ""
    if not username:
        raise ValueError("postgresql dsn username must not be empty")

    password = unquote(parsed.password) if parsed.password else ""
    port = _resolve_port(parsed, 5432, "postgresql")

    database = parsed.path.lstrip("/").strip()
    if not database:
        raise ValueError("postgresql dsn database name must not be empty")

    query_params = parse_qs(parsed.query, keep_blank_values=True)
    schema = source.options.get("schema", "").strip()
    if not schema:
        schema = _first_query_param(query_params, "schema") or "public"
    sslmode = source.options.get("sslmode", "").strip()
    if not sslmode:
        sslmode = _first_query_param(query_params, "sslmode")

    return PostgreSQLDsn(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        schema=schema,
        sslmode=sslmode,
    )


def _parse_oracle_dsn(source: DatabaseSource) -> OracleDsn:
    normalized_dsn = source.dsn.strip()
    if not normalized_dsn:
        raise ValueError("source.dsn must not be empty")

    parsed = urlparse(normalized_dsn)
    scheme = parsed.scheme.strip().lower()
    if scheme not in {"oracle", "oracledb"}:
        raise ValueError("oracle dsn scheme must be 'oracle' or 'oracledb'")

    host = parsed.hostname.strip() if parsed.hostname else ""
    if not host:
        raise ValueError("oracle dsn host must not be empty")

    username = unquote(parsed.username).strip() if parsed.username else ""
    if not username:
        raise ValueError("oracle dsn username must not be empty")

    password = unquote(parsed.password) if parsed.password else ""
    port = _resolve_port(parsed, 1521, "oracle")

    query_params = parse_qs(parsed.query, keep_blank_values=True)
    service_name = parsed.path.lstrip("/").strip()
    if not service_name:
        service_name = _first_query_param(query_params, "service_name") or ""
    if not service_name:
        raise ValueError("oracle dsn service name must not be empty")

    owner = source.options.get("owner", "").strip()
    if not owner:
        owner = _first_query_param(query_params, "owner") or username
    owner = owner.upper()

    return OracleDsn(
        host=host,
        port=port,
        username=username,
        password=password,
        service_name=service_name,
        owner=owner,
    )


def _parse_clickhouse_dsn(source: DatabaseSource) -> ClickHouseDsn:
    normalized_dsn = source.dsn.strip()
    if not normalized_dsn:
        raise ValueError("source.dsn must not be empty")

    parsed = urlparse(normalized_dsn)
    scheme = parsed.scheme.strip().lower()
    if scheme not in {"clickhouse", "clickhouses", "http", "https"}:
        raise ValueError(
            "clickhouse dsn scheme must be one of: clickhouse, clickhouses, http, https"
        )

    host = parsed.hostname.strip() if parsed.hostname else ""
    if not host:
        raise ValueError("clickhouse dsn host must not be empty")

    username = unquote(parsed.username).strip() if parsed.username else "default"
    password = unquote(parsed.password) if parsed.password else ""

    query_params = parse_qs(parsed.query, keep_blank_values=True)
    secure_default = scheme in {"clickhouses", "https"}
    secure = _parse_bool(source.options.get("secure"), secure_default)

    default_port = 9440 if secure else 9000
    port = _resolve_port(parsed, default_port, "clickhouse")

    database = parsed.path.lstrip("/").strip()
    if not database:
        database = source.options.get("database", "").strip()
    if not database:
        database = _first_query_param(query_params, "database") or "default"

    return ClickHouseDsn(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        secure=secure,
    )


def _connect_mysql_like(dsn: MySQLLikeDsn, source_options: dict[str, str]) -> DBAPIConnection:
    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError(
            "pymysql is required for mysql/tidb adapters. "
            "Install dependency first, e.g. `uv add pymysql`."
        ) from exc

    connect_timeout = _parse_positive_int(source_options.get("connect_timeout"), 5)
    read_timeout = _parse_positive_int(source_options.get("read_timeout"), 10)
    write_timeout = _parse_positive_int(source_options.get("write_timeout"), 10)

    return pymysql.connect(
        host=dsn.host,
        port=dsn.port,
        user=dsn.username,
        password=dsn.password,
        database=dsn.database,
        charset=dsn.charset,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        write_timeout=write_timeout,
    )


def _connect_postgresql(dsn: PostgreSQLDsn, source_options: dict[str, str]) -> DBAPIConnection:
    connect_timeout = _parse_positive_int(source_options.get("connect_timeout"), 5)

    try:
        import psycopg

        return psycopg.connect(
            host=dsn.host,
            port=dsn.port,
            user=dsn.username,
            password=dsn.password,
            dbname=dsn.database,
            connect_timeout=connect_timeout,
            sslmode=dsn.sslmode,
        )
    except ImportError:
        try:
            import psycopg2
        except ImportError as exc:
            raise RuntimeError(
                "psycopg (or psycopg2) is required for postgresql adapter. "
                "Install dependency first, e.g. `uv add psycopg[binary]`."
            ) from exc

        return psycopg2.connect(
            host=dsn.host,
            port=dsn.port,
            user=dsn.username,
            password=dsn.password,
            dbname=dsn.database,
            connect_timeout=connect_timeout,
            sslmode=dsn.sslmode,
        )


def _connect_oracle(dsn: OracleDsn, source_options: dict[str, str]) -> DBAPIConnection:
    try:
        import oracledb
    except ImportError as exc:
        raise RuntimeError(
            "oracledb is required for oracle adapter. "
            "Install dependency first, e.g. `uv add oracledb`."
        ) from exc

    connect_timeout = _parse_positive_int(source_options.get("connect_timeout"), 5)
    connection_kwargs: dict[str, object] = {
        "user": dsn.username,
        "password": dsn.password,
        "dsn": f"{dsn.host}:{dsn.port}/{dsn.service_name}",
        "tcp_connect_timeout": connect_timeout,
    }
    return oracledb.connect(**connection_kwargs)


def _connect_clickhouse(dsn: ClickHouseDsn, source_options: dict[str, str]) -> DBAPIConnection:
    try:
        from clickhouse_driver import dbapi
    except ImportError as exc:
        raise RuntimeError(
            "clickhouse-driver is required for clickhouse adapter. "
            "Install dependency first, e.g. `uv add clickhouse-driver`."
        ) from exc

    connect_timeout = _parse_positive_int(source_options.get("connect_timeout"), 5)
    read_timeout = _parse_positive_int(source_options.get("read_timeout"), 10)

    return dbapi.connect(
        host=dsn.host,
        port=dsn.port,
        user=dsn.username,
        password=dsn.password,
        database=dsn.database,
        secure=dsn.secure,
        connect_timeout=connect_timeout,
        send_receive_timeout=read_timeout,
    )


def _extract_single_column_values(rows: list[tuple[object, ...]]) -> list[object]:
    values: list[object] = []
    for row in rows:
        if not row:
            continue
        values.append(row[0])
    return values


def _build_field_units(
    *,
    source_name: str,
    database_type: DatabaseType,
    columns: list[RelationalColumn],
    read_values: Callable[[RelationalColumn], list[object]],
) -> list[FieldUnit]:
    field_units: list[FieldUnit] = []
    for column in columns:
        values = read_values(column)
        samples = _random_sample_values(values)
        if not samples:
            continue

        field_units.append(
            FieldUnit(
                source_name=source_name,
                database_type=database_type,
                container_name=column.table_name,
                field_path=column.column_name,
                original_field=column.column_name,
                field_origin="column",
                logical_type=column.logical_type,
                samples=samples,
            )
        )
    return field_units


class SQLiteRelationalAdapter(BaseAdapter):
    def extract_field_units(self) -> list[FieldUnit]:
        plugin = SQLiteDatabasePlugin()
        agent = plugin.create_agent(self.source)
        try:
            columns = self._discover_relational_columns(agent.conn)
            return _build_field_units(
                source_name=self.source.name,
                database_type=self.database_type,
                columns=columns,
                read_values=lambda column: self._read_column_values(agent.conn, column),
            )
        finally:
            agent.close()

    def _discover_relational_columns(self, connection: sqlite3.Connection) -> list[RelationalColumn]:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_rows = cursor.fetchall()

        columns: list[RelationalColumn] = []
        for row in table_rows:
            if not row:
                continue

            table_name_raw = row[0]
            if not isinstance(table_name_raw, str):
                continue

            table_name = table_name_raw.strip()
            if not table_name or table_name.startswith("sqlite_"):
                continue

            quoted_table_name = _quote_ansi_identifier(table_name)
            cursor.execute(f"PRAGMA table_info({quoted_table_name})")
            for col_row in cursor.fetchall():
                if len(col_row) < 3:
                    continue

                column_name_raw = col_row[1]
                logical_type_raw = col_row[2]
                if not isinstance(column_name_raw, str):
                    continue

                column_name = column_name_raw.strip()
                if not column_name:
                    continue

                columns.append(
                    RelationalColumn(
                        table_name=table_name,
                        column_name=column_name,
                        logical_type=_normalize_logical_type(logical_type_raw),
                    )
                )

        return columns

    def _read_column_values(self, connection: sqlite3.Connection, column: RelationalColumn) -> list[object]:
        quoted_table_name = _quote_ansi_identifier(column.table_name)
        quoted_column_name = _quote_ansi_identifier(column.column_name)
        query = f"SELECT {quoted_column_name} FROM {quoted_table_name}"

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except sqlite3.Error as exc:
            print(f"read field failed {column.table_name}.{column.column_name}: {exc}")
            return []

        return _extract_single_column_values(rows)


class MySQLTiDBRelationalAdapter(BaseAdapter):
    def extract_field_units(self) -> list[FieldUnit]:
        dsn = _parse_mysql_like_dsn(self.source)
        connection = _connect_mysql_like(dsn, self.source.options)
        try:
            columns = self._discover_relational_columns(connection, dsn.database)
            return _build_field_units(
                source_name=self.source.name,
                database_type=self.database_type,
                columns=columns,
                read_values=lambda column: self._read_column_values(connection, dsn.database, column),
            )
        finally:
            connection.close()

    def _discover_relational_columns(
        self,
        connection: DBAPIConnection,
        database_name: str,
    ) -> list[RelationalColumn]:
        query = (
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s "
            "ORDER BY TABLE_NAME, ORDINAL_POSITION"
        )

        cursor = connection.cursor()
        try:
            cursor.execute(query, (database_name,))
            rows = cursor.fetchall()
        except Exception as exc:
            raise RuntimeError(f"discover fields failed in schema '{database_name}': {exc}") from exc

        columns: list[RelationalColumn] = []
        for row in rows:
            if len(row) < 3:
                continue

            table_name_raw = row[0]
            column_name_raw = row[1]
            logical_type_raw = row[2]
            if not isinstance(table_name_raw, str) or not isinstance(column_name_raw, str):
                continue

            table_name = table_name_raw.strip()
            column_name = column_name_raw.strip()
            if not table_name or not column_name:
                continue

            columns.append(
                RelationalColumn(
                    table_name=table_name,
                    column_name=column_name,
                    logical_type=_normalize_logical_type(logical_type_raw),
                )
            )

        return columns

    def _read_column_values(
        self,
        connection: DBAPIConnection,
        database_name: str,
        column: RelationalColumn,
    ) -> list[object]:
        quoted_database = _quote_backtick_identifier(database_name)
        quoted_table = _quote_backtick_identifier(column.table_name)
        quoted_column = _quote_backtick_identifier(column.column_name)
        query = f"SELECT {quoted_column} FROM {quoted_database}.{quoted_table}"

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except Exception as exc:
            print(f"read field failed {column.table_name}.{column.column_name}: {exc}")
            return []

        return _extract_single_column_values(rows)


class PostgreSQLRelationalAdapter(BaseAdapter):
    def extract_field_units(self) -> list[FieldUnit]:
        dsn = _parse_postgresql_dsn(self.source)
        connection = _connect_postgresql(dsn, self.source.options)
        try:
            columns = self._discover_relational_columns(connection, dsn.schema)
            return _build_field_units(
                source_name=self.source.name,
                database_type=self.database_type,
                columns=columns,
                read_values=lambda column: self._read_column_values(connection, dsn.schema, column),
            )
        finally:
            connection.close()

    def _discover_relational_columns(
        self,
        connection: DBAPIConnection,
        schema_name: str,
    ) -> list[RelationalColumn]:
        query = (
            "SELECT table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = %s "
            "ORDER BY table_name, ordinal_position"
        )

        cursor = connection.cursor()
        try:
            cursor.execute(query, (schema_name,))
            rows = cursor.fetchall()
        except Exception as exc:
            raise RuntimeError(f"discover fields failed in schema '{schema_name}': {exc}") from exc

        columns: list[RelationalColumn] = []
        for row in rows:
            if len(row) < 3:
                continue

            table_name_raw = row[0]
            column_name_raw = row[1]
            logical_type_raw = row[2]
            if not isinstance(table_name_raw, str) or not isinstance(column_name_raw, str):
                continue

            table_name = table_name_raw.strip()
            column_name = column_name_raw.strip()
            if not table_name or not column_name:
                continue

            columns.append(
                RelationalColumn(
                    table_name=table_name,
                    column_name=column_name,
                    logical_type=_normalize_logical_type(logical_type_raw),
                )
            )

        return columns

    def _read_column_values(
        self,
        connection: DBAPIConnection,
        schema_name: str,
        column: RelationalColumn,
    ) -> list[object]:
        quoted_schema = _quote_ansi_identifier(schema_name)
        quoted_table = _quote_ansi_identifier(column.table_name)
        quoted_column = _quote_ansi_identifier(column.column_name)
        query = f"SELECT {quoted_column} FROM {quoted_schema}.{quoted_table}"

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except Exception as exc:
            print(f"read field failed {column.table_name}.{column.column_name}: {exc}")
            return []

        return _extract_single_column_values(rows)


class OracleRelationalAdapter(BaseAdapter):
    def extract_field_units(self) -> list[FieldUnit]:
        dsn = _parse_oracle_dsn(self.source)
        connection = _connect_oracle(dsn, self.source.options)
        try:
            columns = self._discover_relational_columns(connection, dsn.owner)
            return _build_field_units(
                source_name=self.source.name,
                database_type=self.database_type,
                columns=columns,
                read_values=lambda column: self._read_column_values(connection, dsn.owner, column),
            )
        finally:
            connection.close()

    def _discover_relational_columns(
        self,
        connection: DBAPIConnection,
        owner: str,
    ) -> list[RelationalColumn]:
        query = (
            "SELECT table_name, column_name, data_type "
            "FROM all_tab_columns "
            "WHERE owner = :owner "
            "ORDER BY table_name, column_id"
        )

        cursor = connection.cursor()
        try:
            cursor.execute(query, {"owner": owner})
            rows = cursor.fetchall()
        except Exception as exc:
            raise RuntimeError(f"discover fields failed in owner '{owner}': {exc}") from exc

        columns: list[RelationalColumn] = []
        for row in rows:
            if len(row) < 3:
                continue

            table_name_raw = row[0]
            column_name_raw = row[1]
            logical_type_raw = row[2]
            if not isinstance(table_name_raw, str) or not isinstance(column_name_raw, str):
                continue

            table_name = table_name_raw.strip()
            column_name = column_name_raw.strip()
            if not table_name or not column_name:
                continue

            columns.append(
                RelationalColumn(
                    table_name=table_name,
                    column_name=column_name,
                    logical_type=_normalize_logical_type(logical_type_raw),
                )
            )

        return columns

    def _read_column_values(
        self,
        connection: DBAPIConnection,
        owner: str,
        column: RelationalColumn,
    ) -> list[object]:
        quoted_owner = _quote_ansi_identifier(owner)
        quoted_table = _quote_ansi_identifier(column.table_name)
        quoted_column = _quote_ansi_identifier(column.column_name)
        query = f"SELECT {quoted_column} FROM {quoted_owner}.{quoted_table}"

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except Exception as exc:
            print(f"read field failed {column.table_name}.{column.column_name}: {exc}")
            return []

        return _extract_single_column_values(rows)


class ClickHouseRelationalAdapter(BaseAdapter):
    def extract_field_units(self) -> list[FieldUnit]:
        dsn = _parse_clickhouse_dsn(self.source)
        connection = _connect_clickhouse(dsn, self.source.options)
        try:
            columns = self._discover_relational_columns(connection, dsn.database)
            return _build_field_units(
                source_name=self.source.name,
                database_type=self.database_type,
                columns=columns,
                read_values=lambda column: self._read_column_values(connection, dsn.database, column),
            )
        finally:
            connection.close()

    def _discover_relational_columns(
        self,
        connection: DBAPIConnection,
        database_name: str,
    ) -> list[RelationalColumn]:
        database_literal = _quote_sql_literal(database_name)
        query = (
            "SELECT table, name, type "
            "FROM system.columns "
            f"WHERE database = {database_literal} "
            "ORDER BY table, position"
        )

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except Exception as exc:
            raise RuntimeError(f"discover fields failed in database '{database_name}': {exc}") from exc

        columns: list[RelationalColumn] = []
        for row in rows:
            if len(row) < 3:
                continue

            table_name_raw = row[0]
            column_name_raw = row[1]
            logical_type_raw = row[2]
            if not isinstance(table_name_raw, str) or not isinstance(column_name_raw, str):
                continue

            table_name = table_name_raw.strip()
            column_name = column_name_raw.strip()
            if not table_name or not column_name:
                continue

            columns.append(
                RelationalColumn(
                    table_name=table_name,
                    column_name=column_name,
                    logical_type=_normalize_logical_type(logical_type_raw),
                )
            )

        return columns

    def _read_column_values(
        self,
        connection: DBAPIConnection,
        database_name: str,
        column: RelationalColumn,
    ) -> list[object]:
        quoted_database = _quote_backtick_identifier(database_name)
        quoted_table = _quote_backtick_identifier(column.table_name)
        quoted_column = _quote_backtick_identifier(column.column_name)
        query = f"SELECT {quoted_column} FROM {quoted_database}.{quoted_table}"

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except Exception as exc:
            print(f"read field failed {column.table_name}.{column.column_name}: {exc}")
            return []

        return _extract_single_column_values(rows)


class PendingRelationalAdapter(BaseAdapter):
    # TODO(next phase): reserved for future relational engines if needed.
    def extract_field_units(self) -> list[FieldUnit]:
        raise RuntimeError(
            f"relational adapter for '{self.database_type}' is TODO in current phase "
            "(supported now: sqlite/mysql/tidb/postgresql/oracle/clickhouse)"
        )
