from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

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
    registry.register(FakePlugin())

    assert "fake" in registry.supported_drivers()
    assert registry.get("fake").driver == "fake"

    with pytest.raises(ValueError, match="already registered"):
        registry.register(FakePlugin())


def test_registry_rejects_empty_driver_and_unsupported_lookup() -> None:
    class EmptyDriverPlugin:
        driver = "   "

        def create_agent(self, source: plugin_registry.DatabaseSource) -> Any:
            raise NotImplementedError(source.name)

    registry = plugin_registry.DatabasePluginRegistry()
    with pytest.raises(ValueError, match="driver must not be empty"):
        registry.register(EmptyDriverPlugin())

    with pytest.raises(KeyError, match="unsupported database driver"):
        registry.get("not_exists")
