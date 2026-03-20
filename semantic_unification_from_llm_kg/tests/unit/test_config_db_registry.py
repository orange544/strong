from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.configs.config as config
import src.db.database_agent as database_agent
import src.storage.registry as registry


def _clear_db_path_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ.keys()):
        if key.startswith("DB_PATH_") or key == "DB_PATHS_JSON":
            monkeypatch.delenv(key, raising=False)


def _create_test_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE movies (
                id INTEGER PRIMARY KEY,
                title TEXT,
                raw_note TEXT
            )
            """
        )
        cursor.executemany(
            "INSERT INTO movies(id, title, raw_note) VALUES (?, ?, ?)",
            [
                (1, "Inception", "A"),
                (2, "Interstellar", "B"),
                (3, "Memento", None),
                (4, "Tenet", ""),
                (5, "Dunkirk", "NULL"),
                (6, "Prestige", "  "),
                (7, "Batman Begins", "null"),
                (8, "Insomnia", "C"),
            ],
        )
        cursor.execute(
            """
            CREATE TABLE empty_values (
                note TEXT
            )
            """
        )
        cursor.executemany(
            "INSERT INTO empty_values(note) VALUES (?)",
            [
                (None,),
                ("",),
                ("NULL",),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_config_number_parsers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UT_INT", "41")
    monkeypatch.setenv("UT_INT_BAD", "invalid")
    monkeypatch.setenv("UT_FLOAT", "3.5")
    monkeypatch.setenv("UT_FLOAT_BAD", "oops")
    monkeypatch.setenv("UT_OPTIONAL_ZERO", "0")
    monkeypatch.setenv("UT_OPTIONAL_NEG", "-0.1")
    monkeypatch.setenv("UT_OPTIONAL_POS", "0.01")

    assert config._as_int("UT_INT", 1) == 41
    assert config._as_int("UT_INT_BAD", 7) == 7
    assert config._as_float("UT_FLOAT", 0.0) == 3.5
    assert config._as_float("UT_FLOAT_BAD", 1.25) == 1.25
    assert config._as_optional_float("UT_OPTIONAL_ZERO", 5.0) is None
    assert config._as_optional_float("UT_OPTIONAL_NEG", 5.0) is None
    assert config._as_optional_float("UT_OPTIONAL_POS", 5.0) == 0.01


def test_load_db_paths_prefers_valid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_db_path_env(monkeypatch)
    monkeypatch.setenv(
        "DB_PATHS_JSON",
        json.dumps(
            {
                "IMDB": "data/dbs/IMDB.db",
                "": "data/dbs/ignored.db",
                "TMDB": "  ",
                "ANIME": "data/dbs/anime.db",
            }
        ),
    )
    monkeypatch.setenv("DB_PATH_SHOULD_NOT_USE", "data/dbs/prefixed.db")

    loaded = config._load_db_paths()

    assert loaded == {
        "IMDB": "data/dbs/IMDB.db",
        "ANIME": "data/dbs/anime.db",
    }


def test_load_db_paths_fallback_to_prefixed_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_db_path_env(monkeypatch)
    monkeypatch.setenv("DB_PATHS_JSON", "{bad-json")
    monkeypatch.setenv("DB_PATH_IMDB", "data/dbs/IMDB.db")
    monkeypatch.setenv("DB_PATH_TMDB", "data/dbs/TMDB.db")
    monkeypatch.setenv("DB_PATH_EMPTY", "   ")

    loaded = config._load_db_paths()

    assert loaded == {
        "IMDB": "data/dbs/IMDB.db",
        "TMDB": "data/dbs/TMDB.db",
    }


def test_load_db_paths_uses_last_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_db_path_env(monkeypatch)

    loaded = config._load_db_paths()

    assert loaded == {
        "IMDB": "data/dbs/DBDB.db",
        "TMDB": "data/dbs/TMDB.db",
    }


def test_quote_identifier_escapes_double_quotes() -> None:
    assert database_agent._quote_identifier('movie"name') == '"movie""name"'


def test_sample_field_filters_noise_and_limits_sample_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "sample_filter.db"
    _create_test_db(db_path)

    monkeypatch.setattr(database_agent, "DB_SAMPLE_RATIO", 0.5)
    monkeypatch.setattr(database_agent, "DB_SAMPLE_MIN", 2)
    monkeypatch.setattr(database_agent, "DB_SAMPLE_MAX", 3)

    agent = database_agent.DatabaseAgent(str(db_path))
    try:
        result = agent.sample_field("movies", "raw_note")
    finally:
        agent.close()

    assert result["table"] == "movies"
    assert result["field"] == "raw_note"
    assert result["type"] == "VARCHAR"
    samples = result["samples"]
    assert isinstance(samples, list)
    assert len(samples) == 2
    assert set(samples).issubset({"A", "B", "C"})


def test_sample_field_missing_field_returns_empty_samples(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_field.db"
    _create_test_db(db_path)

    agent = database_agent.DatabaseAgent(str(db_path))
    try:
        result = agent.sample_field("not_exists_table", "not_exists")
    finally:
        agent.close()

    assert result["samples"] == []


def test_get_all_fields_excludes_empty_columns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "all_fields.db"
    _create_test_db(db_path)

    monkeypatch.setattr(database_agent, "DB_SAMPLE_RATIO", 1.0)
    monkeypatch.setattr(database_agent, "DB_SAMPLE_MIN", 1)
    monkeypatch.setattr(database_agent, "DB_SAMPLE_MAX", 10)

    agent = database_agent.DatabaseAgent(str(db_path))
    try:
        fields = database_agent.get_all_fields(agent)
    finally:
        agent.close()

    keys = {(item["table"], item["field"]) for item in fields}
    assert ("movies", "title") in keys
    assert ("movies", "raw_note") in keys
    assert ("empty_values", "note") not in keys


def test_generate_db_data_returns_tables_and_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "db_data.db"
    _create_test_db(db_path)

    agent = database_agent.DatabaseAgent(str(db_path))
    try:
        data = database_agent.generate_db_data({"TESTDB": agent})
    finally:
        agent.close()

    assert "TESTDB" in data
    assert "movies" in data["TESTDB"]
    assert data["TESTDB"]["movies"] == ["id", "title", "raw_note"]
    assert data["TESTDB"]["empty_values"] == ["note"]


def test_registry_load_missing_file_returns_empty_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "registry_missing.json"
    monkeypatch.setattr(registry, "REGISTRY_PATH", str(registry_path))

    loaded = registry.load_registry()
    latest = registry.get_latest_run()

    assert loaded == {"runs": []}
    assert latest is None


def test_registry_append_and_latest_roundtrip_with_utf8(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "registry.json"
    monkeypatch.setattr(registry, "REGISTRY_PATH", str(registry_path))

    first = {"id": 1, "msg": "首次运行"}
    second = {"id": 2, "msg": "第二次运行"}

    registry.append_run_record(first)
    registry.append_run_record(second)

    loaded = registry.load_registry()
    latest = registry.get_latest_run()
    content = registry_path.read_text(encoding="utf-8")

    assert isinstance(loaded.get("runs"), list)
    assert loaded["runs"] == [first, second]
    assert latest == second
    assert "首次运行" in content
    assert "第二次运行" in content


def test_registry_load_existing_non_object_payload_returns_empty_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "registry_non_object.json"
    registry_path.write_text(json.dumps(["bad", "shape"]), encoding="utf-8")
    monkeypatch.setattr(registry, "REGISTRY_PATH", str(registry_path))

    loaded = registry.load_registry()
    assert loaded == {"runs": []}


def test_registry_append_resets_invalid_runs_and_latest_non_object(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "registry_invalid_runs.json"
    monkeypatch.setattr(registry, "REGISTRY_PATH", str(registry_path))

    registry_path.write_text(json.dumps({"runs": {"bad": "shape"}}), encoding="utf-8")
    registry.append_run_record({"id": 10})
    loaded = registry.load_registry()
    assert loaded["runs"] == [{"id": 10}]

    registry_path.write_text(json.dumps({"runs": [123]}), encoding="utf-8")
    latest = registry.get_latest_run()
    assert latest is None
