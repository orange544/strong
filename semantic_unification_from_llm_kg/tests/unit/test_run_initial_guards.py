from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.pipeline.run_initial as run_initial
from src.db.plugin_registry import DatabaseSource
from src.db.unified.field_unit import FieldUnit


class _FakeDatabaseAgent:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeRegistry:
    def create_agent(self, source: DatabaseSource) -> _FakeDatabaseAgent:
        return _FakeDatabaseAgent(source.dsn)

    def supported_drivers(self) -> tuple[str, ...]:
        return ("sqlite",)


class _FakeIPFSWithNoneDomainUnified:
    def __init__(self) -> None:
        self._counter = 0
        self._storage: dict[str, Any] = {}

    def add_json(self, obj: Any) -> object:
        self._counter += 1
        if self._counter == 3:
            # domain_unified_cid for the only domain
            return None
        cid = f"cid_{self._counter}"
        self._storage[cid] = obj
        return cid

    def cat_json(self, cid: object) -> Any:
        if not isinstance(cid, str):
            raise RuntimeError("cid must be a string")
        return self._storage[cid]


def _create_sqlite_db(path: Path) -> str:
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE movie (id INTEGER PRIMARY KEY, title TEXT)")
    conn.execute("INSERT INTO movie (title) VALUES (?)", ("A",))
    conn.commit()
    conn.close()
    return str(path)


def _make_save_json(tmp_output: Path) -> Any:
    def _save_json(data: Any, filename: str) -> str:
        path = tmp_output / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)

    return _save_json


def test_run_initial_load_runtime_db_sources_rejects_invalid_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_initial,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: cast(Any, []),
    )
    with pytest.raises(RuntimeError, match="must return a source map"):
        run_initial._load_runtime_db_sources()

    monkeypatch.setattr(
        run_initial,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {
            "  ": DatabaseSource(name="X", driver="sqlite", dsn="x.db", options={})
        },
    )
    with pytest.raises(RuntimeError, match="name must be a non-empty string"):
        run_initial._load_runtime_db_sources()

    monkeypatch.setattr(
        run_initial,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {"ONE": cast(Any, object())},
    )
    with pytest.raises(RuntimeError, match="invalid source object"):
        run_initial._load_runtime_db_sources()


def test_run_initial_load_runtime_db_sources_accepts_valid_map(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = DatabaseSource(name="IMDB", driver="sqlite", dsn="data/dbs/IMDB.db", options={})
    monkeypatch.setattr(
        run_initial,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {"IMDB": source},
    )

    loaded = run_initial._load_runtime_db_sources()
    assert list(loaded.keys()) == ["IMDB"]
    assert loaded["IMDB"] is source


def test_run_initial_discover_and_collect_candidate_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_dir = tmp_path / "dbs"
    db_dir.mkdir(parents=True, exist_ok=True)
    _create_sqlite_db(db_dir / "a.db")
    _create_sqlite_db(db_dir / "b.sqlite")
    (db_dir / "c.txt").write_text("x", encoding="utf-8")
    (db_dir / "d.db").mkdir(parents=True, exist_ok=True)

    discovered = run_initial._discover_sqlite_sources_from_folder(str(db_dir))
    assert set(discovered.keys()) == {"a", "b"}

    monkeypatch.setattr(
        run_initial,
        "_load_runtime_db_sources",
        lambda: {
            "b": DatabaseSource(name="b", driver="sqlite", dsn="override.db", options={}),
            "cfg": DatabaseSource(name="cfg", driver="sqlite", dsn="cfg.db", options={}),
        },
    )
    candidates = run_initial._collect_candidate_sources(str(db_dir))
    assert set(candidates.keys()) == {"a", "b", "cfg"}
    assert candidates["b"].dsn == "override.db"


def test_run_initial_discover_returns_empty_when_folder_missing(tmp_path: Path) -> None:
    missing = tmp_path / "missing-folder"
    assert run_initial._discover_sqlite_sources_from_folder(str(missing)) == {}


def test_run_initial_normalize_source_for_agent_handles_stat_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakePath:
        def __init__(self, value: str):
            self.value = value

        def is_file(self) -> bool:
            return True

        def stat(self) -> Any:
            raise OSError("stat failed")

        def __str__(self) -> str:
            return self.value

    monkeypatch.setattr(run_initial, "_resolve_sqlite_dsn", lambda _dsn: "dummy.db")
    monkeypatch.setattr(run_initial, "Path", _FakePath)

    with pytest.raises(RuntimeError, match="Failed to inspect sqlite file"):
        run_initial._normalize_source_for_agent(
            DatabaseSource(name="ONE", driver="sqlite", dsn="x.db", options={})
        )


def test_run_initial_create_db_agents_rejects_empty_dsn_and_unsupported_driver() -> None:
    class _UnsupportedRegistry:
        def create_agent(self, source: DatabaseSource) -> _FakeDatabaseAgent:
            raise KeyError(source.driver)

        def supported_drivers(self) -> tuple[str, ...]:
            return ("sqlite",)

    with pytest.raises(RuntimeError, match="empty DSN"):
        run_initial._create_db_agents(
            {"EMPTY": DatabaseSource(name="EMPTY", driver="sqlite", dsn="   ", options={})},
            cast(Any, _UnsupportedRegistry()),
        )

    with pytest.raises(RuntimeError, match="Unsupported database driver 'postgres'"):
        run_initial._create_db_agents(
            {
                "PG": DatabaseSource(
                    name="PG",
                    driver="postgres",
                    dsn="postgresql://127.0.0.1:5432/app",
                    options={},
                )
            },
            cast(Any, _UnsupportedRegistry()),
        )


def test_run_initial_run_all_raises_when_no_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(run_initial, "_auto_db_folder", lambda: "data/dbs")
    monkeypatch.setattr(run_initial, "_collect_candidate_sources", lambda _db_folder: {})

    with pytest.raises(RuntimeError, match="No database sources configured"):
        run_initial.run_all()


def test_run_initial_run_all_handles_max_fields_and_none_domain_unified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = _FakeIPFSWithNoneDomainUnified()
    captured_records: list[dict[str, Any]] = []
    captured_max_fields: dict[str, int] = {}
    db_file = Path(_create_sqlite_db(tmp_path / "one.db"))

    monkeypatch.setattr(
        run_initial,
        "_collect_candidate_sources",
        lambda _db_folder: {
            "ONE": DatabaseSource(name="ONE", driver="sqlite", dsn=str(db_file), options={})
        },
    )
    monkeypatch.setattr(
        run_initial,
        "extract_field_units_by_source",
        lambda _sources, *, max_fields_per_domain=0: (
            captured_max_fields.__setitem__("value", max_fields_per_domain),
            {
                "ONE": [
                    FieldUnit(
                        source_name="ONE",
                        database_type="sqlite",
                        container_name="movie",
                        field_path="id",
                        original_field="id",
                        field_origin="column",
                        logical_type="INTEGER",
                        samples=("1",),
                    )
                ]
            },
        )[1],
    )
    monkeypatch.setattr(run_initial, "IPFSClient", lambda: cast(Any, fake_ipfs))
    monkeypatch.setattr(run_initial, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(
        run_initial,
        "PIPELINE_CONFIG",
        {
            "llm_desc_max_workers": 1,
            "llm_desc_domain_timeout_sec": 30,
            "run_max_fields_per_domain": 1,
        },
    )
    monkeypatch.setattr(run_initial, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(run_initial, "LLM_UNIFY_CONFIG", {"api_key": "", "base_url": "", "model_name": "unify"})
    monkeypatch.setattr(
        run_initial,
        "_persist_run_record",
        lambda record, _timestamp: captured_records.append(record),
    )

    class _FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            return {
                "table": sample["table"],
                "field": sample["field"],
                "description": "ok",
            }

    class _FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return field_desc_list

        def unify_across_domains(self, domain_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                {
                    "canonical_name": "movie_id",
                    "fields": [str(domain_items[0]["fields"][0])],
                    "description": "id",
                }
            ]

    class _FakeKnowledgeGraphAgent:
        def generate_cypher(
            self,
            run_record: dict[str, Any],
            db_data: dict[str, dict[str, list[str]]],
            domain_field_desc_map: dict[str, list[dict[str, Any]]],
            domain_unified_map: dict[str, list[dict[str, Any]]],
            unified_fields: list[dict[str, Any]],
        ) -> list[str]:
            assert run_record["domains"][0]["sampled_field_count"] == 1
            assert db_data["ONE"]["movie"] == ["id"]
            assert len(domain_field_desc_map["ONE"]) == 1
            # domain_unified_cid is None, should fallback to empty list branch.
            assert domain_unified_map["ONE"] == []
            assert len(unified_fields) == 1
            return ["MERGE (:Smoke {name:'ok'});"]

    monkeypatch.setattr(run_initial, "FieldDescriptionAgent", _FakeFieldDescriptionAgent)
    monkeypatch.setattr(run_initial, "FieldSemanticAgent", _FakeFieldSemanticAgent)
    monkeypatch.setattr(run_initial, "KnowledgeGraphAgent", _FakeKnowledgeGraphAgent)

    run_initial.run_all()

    assert len(captured_records) == 1
    assert captured_records[0]["status"] == "completed"
    assert captured_max_fields["value"] == 1


def test_run_initial_run_all_warns_when_persist_fails_on_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_file = Path(_create_sqlite_db(tmp_path / "one_success.db"))
    fake_ipfs = _FakeIPFSWithNoneDomainUnified()

    monkeypatch.setattr(
        run_initial,
        "_collect_candidate_sources",
        lambda _db_folder: {
            "ONE": DatabaseSource(name="ONE", driver="sqlite", dsn=str(db_file), options={})
        },
    )
    monkeypatch.setattr(
        run_initial,
        "extract_field_units_by_source",
        lambda _sources, *, max_fields_per_domain=0: {
            "ONE": [
                FieldUnit(
                    source_name="ONE",
                    database_type="sqlite",
                    container_name="movie",
                    field_path="id",
                    original_field="id",
                    field_origin="column",
                    logical_type="INTEGER",
                    samples=("1",),
                )
            ]
        },
    )
    monkeypatch.setattr(run_initial, "IPFSClient", lambda: cast(Any, fake_ipfs))
    monkeypatch.setattr(run_initial, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(
        run_initial,
        "PIPELINE_CONFIG",
        {
            "llm_desc_max_workers": 1,
            "llm_desc_domain_timeout_sec": 30,
            "run_max_fields_per_domain": 0,
        },
    )
    monkeypatch.setattr(run_initial, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(run_initial, "LLM_UNIFY_CONFIG", {"api_key": "", "base_url": "", "model_name": "unify"})
    monkeypatch.setattr(run_initial, "_persist_run_record", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("persist down")))

    class _FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            return {"table": sample["table"], "field": sample["field"], "description": "ok"}

    class _FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return field_desc_list

        def unify_across_domains(self, domain_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [{"canonical_name": "x", "fields": [str(domain_items[0]["fields"][0])], "description": "x"}]

    class _FakeKnowledgeGraphAgent:
        def generate_cypher(
            self,
            run_record: dict[str, Any],
            db_data: dict[str, dict[str, list[str]]],
            domain_field_desc_map: dict[str, list[dict[str, Any]]],
            domain_unified_map: dict[str, list[dict[str, Any]]],
            unified_fields: list[dict[str, Any]],
        ) -> list[str]:
            del run_record, db_data, domain_field_desc_map, domain_unified_map, unified_fields
            return ["MERGE (:Smoke {name:'ok'});"]

    monkeypatch.setattr(run_initial, "FieldDescriptionAgent", _FakeFieldDescriptionAgent)
    monkeypatch.setattr(run_initial, "FieldSemanticAgent", _FakeFieldSemanticAgent)
    monkeypatch.setattr(run_initial, "KnowledgeGraphAgent", _FakeKnowledgeGraphAgent)

    run_initial.run_all()
    output = capsys.readouterr().out
    assert "[warn] failed to persist run record: persist down" in output


def test_run_initial_run_all_warns_when_persist_fails_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_file = Path(_create_sqlite_db(tmp_path / "one_failure.db"))

    monkeypatch.setattr(
        run_initial,
        "_collect_candidate_sources",
        lambda _db_folder: {
            "ONE": DatabaseSource(name="ONE", driver="sqlite", dsn=str(db_file), options={})
        },
    )
    monkeypatch.setattr(
        run_initial,
        "extract_field_units_by_source",
        lambda _sources, *, max_fields_per_domain=0: {
            "ONE": [
                FieldUnit(
                    source_name="ONE",
                    database_type="sqlite",
                    container_name="movie",
                    field_path="id",
                    original_field="id",
                    field_origin="column",
                    logical_type="INTEGER",
                    samples=("1",),
                )
            ]
        },
    )
    monkeypatch.setattr(run_initial, "IPFSClient", lambda: cast(Any, _FakeIPFSWithNoneDomainUnified()))
    monkeypatch.setattr(run_initial, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(
        run_initial,
        "PIPELINE_CONFIG",
        {
            "llm_desc_max_workers": 1,
            "llm_desc_domain_timeout_sec": 30,
            "run_max_fields_per_domain": 0,
        },
    )
    monkeypatch.setattr(run_initial, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(run_initial, "LLM_UNIFY_CONFIG", {"api_key": "", "base_url": "", "model_name": "unify"})
    monkeypatch.setattr(run_initial, "_persist_run_record", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("persist down")))

    class _FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            return {"table": sample["table"], "field": sample["field"], "description": "ok"}

    class _FailingFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            del api_key, base_url, model_name

        def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            del field_desc_list
            return []

        def unify_across_domains(self, domain_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            del domain_items
            raise RuntimeError("semantic failed")

    monkeypatch.setattr(run_initial, "FieldDescriptionAgent", _FakeFieldDescriptionAgent)
    monkeypatch.setattr(run_initial, "FieldSemanticAgent", _FailingFieldSemanticAgent)

    with pytest.raises(RuntimeError, match="semantic failed"):
        run_initial.run_all()
    output = capsys.readouterr().out
    assert "[warn] failed to persist failed run record: persist down" in output
