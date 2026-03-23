from __future__ import annotations

import importlib.util
import socket
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from src.db.plugin_registry import DatabaseSource
from src.db.unified.adapter_factory import AdapterFactory
from src.db.unified.field_unit import DatabaseType, normalize_database_type

PROJECT_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class DriverDependency:
    module_name: str
    install_hint: str


_DRIVER_DEPENDENCIES: dict[DatabaseType, DriverDependency | None] = {
    "sqlite": None,
    "mysql": DriverDependency(module_name="pymysql", install_hint="uv add pymysql"),
    "tidb": DriverDependency(module_name="pymysql", install_hint="uv add pymysql"),
    "postgresql": DriverDependency(
        module_name="psycopg",
        install_hint="uv add 'psycopg[binary]'",
    ),
    "oracle": DriverDependency(module_name="oracledb", install_hint="uv add oracledb"),
    "clickhouse": DriverDependency(
        module_name="clickhouse_driver",
        install_hint="uv add clickhouse-driver",
    ),
    "mongodb": DriverDependency(module_name="pymongo", install_hint="uv add pymongo"),
    "neo4j": DriverDependency(module_name="neo4j", install_hint="uv add neo4j"),
    "redis": DriverDependency(module_name="redis", install_hint="uv add redis"),
    "cassandra": DriverDependency(
        module_name="cassandra",
        install_hint="uv add cassandra-driver",
    ),
    "hbase": DriverDependency(module_name="happybase", install_hint="uv add happybase"),
}


def run_preflight_checks(
    sources: dict[str, DatabaseSource],
    *,
    check_sqlite_path: bool,
    check_tcp: bool,
    tcp_timeout_sec: float,
) -> None:
    validate_driver_support(sources)
    validate_runtime_dependencies(sources)
    if check_sqlite_path:
        validate_sqlite_paths(sources)
    if check_tcp:
        validate_tcp_connectivity(sources, tcp_timeout_sec=tcp_timeout_sec)


def validate_driver_support(sources: dict[str, DatabaseSource]) -> None:
    supported = ", ".join(AdapterFactory().supported_database_types())
    unsupported: list[str] = []
    for source_name, source in sources.items():
        try:
            normalize_database_type(source.driver)
        except ValueError:
            unsupported.append(f"{source_name}({source.driver})")
    if unsupported:
        raise RuntimeError(
            "Unsupported database drivers in DB_SOURCES_JSON: "
            f"{', '.join(unsupported)}. Supported drivers: {supported}"
        )


def validate_runtime_dependencies(sources: dict[str, DatabaseSource]) -> None:
    missing: list[str] = []
    for source_name, source in sources.items():
        database_type = normalize_database_type(source.driver)
        dependency = _DRIVER_DEPENDENCIES[database_type]
        if dependency is None:
            continue
        if _module_exists(dependency.module_name):
            continue
        missing.append(
            f"{source_name}({database_type}): missing '{dependency.module_name}', run `{dependency.install_hint}`"
        )

    if missing:
        raise RuntimeError("Missing database adapter dependencies:\n" + "\n".join(missing))


def validate_sqlite_paths(sources: dict[str, DatabaseSource]) -> None:
    missing_files: list[str] = []
    for source_name, source in sources.items():
        database_type = normalize_database_type(source.driver)
        if database_type != "sqlite":
            continue

        raw_dsn = source.dsn.strip()
        if not raw_dsn:
            missing_files.append(f"{source_name}: sqlite dsn is empty")
            continue

        sqlite_path = Path(raw_dsn)
        if not sqlite_path.is_absolute():
            sqlite_path = (PROJECT_ROOT / sqlite_path).resolve()
        if sqlite_path.is_file():
            continue
        missing_files.append(f"{source_name}: sqlite file not found -> {sqlite_path}")

    if missing_files:
        raise RuntimeError("SQLite source preflight failed:\n" + "\n".join(missing_files))


def validate_tcp_connectivity(
    sources: dict[str, DatabaseSource],
    *,
    tcp_timeout_sec: float,
) -> None:
    if tcp_timeout_sec <= 0:
        raise RuntimeError("tcp_timeout_sec must be positive")

    errors: list[str] = []
    for source_name, source in sources.items():
        database_type = normalize_database_type(source.driver)
        endpoint = _resolve_endpoint(database_type, source.dsn.strip())
        if endpoint is None:
            continue

        host, port = endpoint
        try:
            with socket.create_connection((host, port), timeout=tcp_timeout_sec):
                pass
        except OSError as exc:
            errors.append(
                f"{source_name}({database_type}) cannot connect to {host}:{port} -> {exc}"
            )

    if errors:
        raise RuntimeError("Database connectivity preflight failed:\n" + "\n".join(errors))


def _module_exists(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def _resolve_endpoint(
    database_type: DatabaseType,
    dsn: str,
) -> tuple[str, int] | None:
    if not dsn:
        return None

    if database_type == "sqlite":
        return None

    if database_type == "cassandra":
        parsed = urlparse(dsn)
        netloc = parsed.netloc
        host_part = netloc.rsplit("@", 1)[-1]
        first = host_part.split(",", 1)[0].strip()
        if not first:
            return None
        if ":" in first:
            host, port_raw = first.rsplit(":", 1)
            return host.strip(), _to_positive_port(port_raw, 9042)
        return first, 9042

    parsed = urlparse(dsn)
    host = parsed.hostname.strip() if parsed.hostname else ""
    if not host:
        return None

    if parsed.port is not None:
        return host, parsed.port

    default_port = _default_port(database_type, scheme=parsed.scheme.strip().lower())
    return host, default_port


def _default_port(database_type: DatabaseType, *, scheme: str) -> int:
    if database_type in {"mysql", "tidb"}:
        return 3306
    if database_type == "postgresql":
        return 5432
    if database_type == "oracle":
        return 1521
    if database_type == "clickhouse":
        return 9440 if scheme in {"clickhouses", "https"} else 9000
    if database_type == "mongodb":
        return 27017
    if database_type == "neo4j":
        return 7687
    if database_type == "redis":
        return 6379
    if database_type == "hbase":
        return 9090
    if database_type == "cassandra":
        return 9042
    return 0


def _to_positive_port(raw: str, default_port: int) -> int:
    try:
        value = int(raw)
    except ValueError:
        return default_port
    if value <= 0:
        return default_port
    return value
