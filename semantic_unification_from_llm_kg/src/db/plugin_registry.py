from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.db.database_agent import DatabaseAgent


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class DatabaseSource:
    name: str
    driver: str
    dsn: str
    options: dict[str, str] = field(default_factory=dict)


class DatabasePlugin:
    driver: str

    def create_agent(self, source: DatabaseSource) -> DatabaseAgent:
        raise NotImplementedError


class SQLiteDatabasePlugin(DatabasePlugin):
    driver = "sqlite"

    def create_agent(self, source: DatabaseSource) -> DatabaseAgent:
        from src.db.database_agent import DatabaseAgent

        if source.driver.strip().lower() != self.driver:
            raise ValueError(f"source driver mismatch: expected '{self.driver}', got '{source.driver}'")

        dsn = source.dsn.strip()
        if not dsn:
            raise ValueError("sqlite source dsn must not be empty")

        db_path = Path(dsn)
        if not db_path.is_absolute():
            db_path = (PROJECT_ROOT / db_path).resolve()
        return DatabaseAgent(str(db_path))


class DatabasePluginRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, DatabasePlugin] = {}
        self.register(SQLiteDatabasePlugin())

    def register(self, plugin: DatabasePlugin, *, replace: bool = False) -> None:
        key = plugin.driver.strip().lower()
        if not key:
            raise ValueError("plugin driver must not be empty")

        if key in self._plugins and not replace:
            raise ValueError(f"plugin '{key}' already registered")
        self._plugins[key] = plugin

    def get(self, driver: str) -> DatabasePlugin:
        key = driver.strip().lower()
        if key not in self._plugins:
            raise KeyError(f"unsupported database driver: {driver}")
        return self._plugins[key]

    def create_agent(self, source: DatabaseSource) -> DatabaseAgent:
        plugin = self.get(source.driver)
        return plugin.create_agent(source)

    def supported_drivers(self) -> tuple[str, ...]:
        return tuple(sorted(self._plugins.keys()))


def _normalize_options(raw_options: Any) -> dict[str, str]:
    if not isinstance(raw_options, Mapping):
        return {}
    return {str(k): str(v) for k, v in raw_options.items() if str(k).strip()}


def _to_source_item(name: str, payload: Any) -> DatabaseSource:
    source_name = str(name).strip()
    if not source_name:
        raise ValueError("source name must not be empty")

    if isinstance(payload, str):
        dsn = payload.strip()
        if not dsn:
            raise ValueError(f"source '{source_name}' has empty dsn")
        return DatabaseSource(
            name=source_name,
            driver="sqlite",
            dsn=dsn,
            options={},
        )

    if isinstance(payload, Mapping):
        driver = str(payload.get("driver", "sqlite")).strip() or "sqlite"
        dsn = str(payload.get("dsn", "")).strip()
        if not dsn:
            raise ValueError(f"source '{source_name}' has empty dsn")
        options = _normalize_options(payload.get("options", {}))
        return DatabaseSource(
            name=source_name,
            driver=driver,
            dsn=dsn,
            options=options,
        )

    raise ValueError(
        f"source '{source_name}' payload must be a string or an object, got {type(payload).__name__}"
    )


def _legacy_paths_to_sources(legacy_db_paths: Mapping[str, str]) -> dict[str, DatabaseSource]:
    sources: dict[str, DatabaseSource] = {}
    for name, path in legacy_db_paths.items():
        source_name = str(name).strip()
        dsn = str(path).strip()
        if not source_name or not dsn:
            continue
        sources[source_name] = DatabaseSource(
            name=source_name,
            driver="sqlite",
            dsn=dsn,
            options={},
        )
    return sources


def load_db_sources_from_env(*, legacy_db_paths: Mapping[str, str]) -> dict[str, DatabaseSource]:
    raw_json = os.getenv("DB_SOURCES_JSON", "").strip()
    if not raw_json:
        return _legacy_paths_to_sources(legacy_db_paths)

    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError(f"DB_SOURCES_JSON is not valid JSON: {exc}") from exc

    if not isinstance(parsed, Mapping):
        raise ValueError("DB_SOURCES_JSON must be a JSON object")

    sources: dict[str, DatabaseSource] = {}
    for name, payload in parsed.items():
        source = _to_source_item(str(name), payload)
        sources[source.name] = source

    if sources:
        return sources
    return _legacy_paths_to_sources(legacy_db_paths)
