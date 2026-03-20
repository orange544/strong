from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.db.plugin_registry as plugin_registry


def test_load_db_sources_falls_back_to_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DB_SOURCES_JSON", raising=False)
    sources = plugin_registry.load_db_sources_from_env(
        legacy_db_paths={"IMDB": "data/dbs/IMDB.db"}
    )

    assert list(sources.keys()) == ["IMDB"]
    source = sources["IMDB"]
    assert source.driver == "sqlite"
    assert source.dsn == "data/dbs/IMDB.db"
    assert source.options == {}


def test_load_db_sources_supports_string_and_object_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "IMDB": "data/dbs/IMDB.db",
        "PG_MOVIES": {
            "driver": "postgres",
            "dsn": "postgresql://user:pwd@127.0.0.1:5432/movies",
            "options": {"sslmode": "disable", "pool_size": 5},
        },
    }
    monkeypatch.setenv("DB_SOURCES_JSON", json.dumps(payload))

    sources = plugin_registry.load_db_sources_from_env(legacy_db_paths={})

    assert sources["IMDB"].driver == "sqlite"
    assert sources["PG_MOVIES"].driver == "postgres"
    assert sources["PG_MOVIES"].options == {
        "sslmode": "disable",
        "pool_size": "5",
    }


def test_load_db_sources_rejects_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_SOURCES_JSON", "{bad-json")
    with pytest.raises(ValueError, match="DB_SOURCES_JSON is not valid JSON"):
        plugin_registry.load_db_sources_from_env(legacy_db_paths={})


def test_load_db_sources_rejects_invalid_payload_type(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_SOURCES_JSON", json.dumps({"IMDB": 123}))
    with pytest.raises(ValueError, match="payload must be a string or an object"):
        plugin_registry.load_db_sources_from_env(legacy_db_paths={})


def test_registry_default_sqlite_plugin_creates_agent_for_absolute_path(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "plugin_registry.db"
    conn = sqlite3.connect(str(db_path))
    conn.close()

    source = plugin_registry.DatabaseSource(
        name="TEST",
        driver="sqlite",
        dsn=str(db_path),
        options={},
    )
    registry = plugin_registry.DatabasePluginRegistry()
    agent = registry.create_agent(source)
    try:
        assert agent.db_path == str(db_path)
    finally:
        agent.close()


def test_registry_registers_custom_plugin_and_checks_duplicates() -> None:
    class FakePlugin:
        driver = "fake"

        def create_agent(self, source: plugin_registry.DatabaseSource) -> Any:
            raise NotImplementedError(source.name)

    registry = plugin_registry.DatabasePluginRegistry()
    registry.register(cast(plugin_registry.DatabasePlugin, FakePlugin()))

    assert "fake" in registry.supported_drivers()
    assert registry.get("fake").driver == "fake"

    with pytest.raises(ValueError, match="already registered"):
        registry.register(cast(plugin_registry.DatabasePlugin, FakePlugin()))


def test_registry_rejects_empty_driver_and_unsupported_lookup() -> None:
    class EmptyDriverPlugin:
        driver = "   "

        def create_agent(self, source: plugin_registry.DatabaseSource) -> Any:
            raise NotImplementedError(source.name)

    registry = plugin_registry.DatabasePluginRegistry()
    with pytest.raises(ValueError, match="driver must not be empty"):
        registry.register(cast(plugin_registry.DatabasePlugin, EmptyDriverPlugin()))

    with pytest.raises(KeyError, match="unsupported database driver"):
        registry.get("not_exists")


def test_sqlite_plugin_rejects_driver_mismatch_and_empty_dsn() -> None:
    plugin = plugin_registry.SQLiteDatabasePlugin()

    with pytest.raises(ValueError, match="source driver mismatch"):
        plugin.create_agent(
            plugin_registry.DatabaseSource(
                name="PG",
                driver="postgres",
                dsn="db.sqlite",
                options={},
            )
        )

    with pytest.raises(ValueError, match="dsn must not be empty"):
        plugin.create_agent(
            plugin_registry.DatabaseSource(
                name="EMPTY",
                driver="sqlite",
                dsn="   ",
                options={},
            )
        )


def test_sqlite_plugin_resolves_relative_path_from_project_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(plugin_registry, "PROJECT_ROOT", tmp_path)
    plugin = plugin_registry.SQLiteDatabasePlugin()
    source = plugin_registry.DatabaseSource(
        name="REL",
        driver="sqlite",
        dsn="data/relative_plugin.db",
        options={},
    )
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)

    agent = plugin.create_agent(source)
    try:
        expected_path = (tmp_path / "data/relative_plugin.db").resolve()
        assert agent.db_path == str(expected_path)
    finally:
        agent.close()


def test_to_source_item_rejects_empty_name_and_empty_dsn_for_mapping() -> None:
    with pytest.raises(ValueError, match="source name must not be empty"):
        plugin_registry._to_source_item(" ", {"driver": "sqlite", "dsn": "db.sqlite"})  # noqa: SLF001

    with pytest.raises(ValueError, match="source 'IMDB' has empty dsn"):
        plugin_registry._to_source_item("IMDB", "   ")  # noqa: SLF001

    with pytest.raises(ValueError, match="source 'IMDB' has empty dsn"):
        plugin_registry._to_source_item("IMDB", {"driver": "sqlite", "dsn": "  "})  # noqa: SLF001


def test_normalize_options_and_legacy_paths_branches() -> None:
    assert plugin_registry._normalize_options(1) == {}  # noqa: SLF001
    assert plugin_registry._normalize_options({" ": "x", "sslmode": "disable"}) == {  # noqa: SLF001
        "sslmode": "disable"
    }

    legacy = plugin_registry._legacy_paths_to_sources(  # noqa: SLF001
        {
            "IMDB": "data/dbs/IMDB.db",
            " ": "data/dbs/skip.db",
            "SKIP_EMPTY_DSN": " ",
        }
    )
    assert list(legacy.keys()) == ["IMDB"]


def test_load_db_sources_rejects_non_object_json_and_falls_back_on_empty_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DB_SOURCES_JSON", json.dumps(["not", "an", "object"]))
    with pytest.raises(ValueError, match="must be a JSON object"):
        plugin_registry.load_db_sources_from_env(legacy_db_paths={})

    monkeypatch.setenv("DB_SOURCES_JSON", "{}")
    sources = plugin_registry.load_db_sources_from_env(
        legacy_db_paths={"IMDB": "data/dbs/IMDB.db"}
    )
    assert list(sources.keys()) == ["IMDB"]
