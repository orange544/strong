from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.pipeline.run_auto as run_auto
from src.db.plugin_registry import DatabaseSource


def test_auto_previous_unified_fields_cid_handles_none_and_invalid_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"previous_unified_fields_cid": None})
    assert run_auto._auto_previous_unified_fields_cid() == ""

    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"previous_unified_fields_cid": 123})
    with pytest.raises(RuntimeError, match="must be a string"):
        run_auto._auto_previous_unified_fields_cid()


def test_auto_poll_interval_sec_parses_and_validates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"poll_interval_sec": 0})
    assert run_auto._auto_poll_interval_sec() == 1

    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"poll_interval_sec": "2"})
    assert run_auto._auto_poll_interval_sec() == 2

    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"poll_interval_sec": "bad"})
    with pytest.raises(RuntimeError, match="must be an integer"):
        run_auto._auto_poll_interval_sec()


def test_load_runtime_db_sources_rejects_invalid_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_auto,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: cast(Any, []),
    )
    with pytest.raises(RuntimeError, match="must return a source map"):
        run_auto._load_runtime_db_sources()

    monkeypatch.setattr(
        run_auto,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {"  ": DatabaseSource(name="A", driver="sqlite", dsn="a.db", options={})},
    )
    with pytest.raises(RuntimeError, match="name must be a non-empty string"):
        run_auto._load_runtime_db_sources()

    monkeypatch.setattr(
        run_auto,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {"A": cast(Any, object())},
    )
    with pytest.raises(RuntimeError, match="invalid source object"):
        run_auto._load_runtime_db_sources()


def test_run_llm_pipeline_rejects_non_string_cid_from_ipfs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeIPFS:
        def cat_json(self, _cid: str) -> object:
            return [{"table": "movie", "field": "title", "samples": ["A"]}]

        def add_json(self, _obj: object) -> object:
            return 123

    class FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, item: dict[str, Any]) -> dict[str, str]:
            return {
                "table": str(item["table"]),
                "field": str(item["field"]),
                "description": "ok",
            }

    monkeypatch.setattr(run_auto, "FieldDescriptionAgent", FakeFieldDescriptionAgent)
    monkeypatch.setattr(run_auto, "save_json", lambda _data, _name: "ignored.json")

    with pytest.raises(RuntimeError, match="CID string"):
        run_auto.run_llm_pipeline(cast(Any, FakeIPFS()), "samples-cid")


def test_run_auto_internal_wrappers_reject_invalid_return_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.service.sample.run_sampling", lambda *_args, **_kwargs: 123)
    with pytest.raises(RuntimeError, match="CID string"):
        run_auto._run_sampling({}, cast(Any, object()), "20260320_000000")

    monkeypatch.setattr(
        "src.service.semantic_service.unify_fields_with_existing",
        lambda **_kwargs: 123,
    )
    with pytest.raises(RuntimeError, match="CID string"):
        run_auto._unify_fields_with_existing(
            field_descriptions=[],
            existing_unified_fields_cid="cid",
            ipfs=cast(Any, object()),
            llm_config={},
        )

    monkeypatch.setattr("src.service.kg_service.run_kg_full", lambda *_args, **_kwargs: "bad")
    with pytest.raises(RuntimeError, match="must return"):
        run_auto._run_kg_full(cast(Any, object()), "cid", {})


def test_auto_poll_interval_sec_rejects_non_int_non_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"poll_interval_sec": None})
    with pytest.raises(RuntimeError, match="must be an integer"):
        run_auto._auto_poll_interval_sec()


def test_load_runtime_db_sources_returns_valid_map(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = DatabaseSource(name="IMDB", driver="sqlite", dsn="data/dbs/IMDB.db", options={})
    monkeypatch.setattr(
        run_auto,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {"IMDB": source},
    )

    loaded = run_auto._load_runtime_db_sources()
    assert list(loaded.keys()) == ["IMDB"]
    assert loaded["IMDB"] is source


def test_discover_sqlite_sources_handles_missing_folder_and_filters_entries(
    tmp_path: Path,
) -> None:
    missing = run_auto._discover_sqlite_sources_from_folder(str(tmp_path / "missing"))
    assert missing == {}

    db_folder = tmp_path / "discover"
    db_folder.mkdir(parents=True, exist_ok=True)
    (db_folder / "a.db").write_text("x", encoding="utf-8")
    (db_folder / "b.sqlite3").write_text("x", encoding="utf-8")
    (db_folder / "note.txt").write_text("x", encoding="utf-8")
    (db_folder / "nested").mkdir()

    discovered = run_auto._discover_sqlite_sources_from_folder(str(db_folder))
    assert set(discovered.keys()) == {"a", "b"}


def test_source_signature_and_wrapper_success_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_file = tmp_path / "sig.db"
    db_file.write_text("x", encoding="utf-8")
    source = DatabaseSource(name="SIG", driver="sqlite", dsn=str(db_file), options={})

    signature = run_auto._source_signature(source)
    assert signature.startswith("SIG|sqlite|")
    assert str(db_file.resolve()) in signature

    monkeypatch.setattr("src.service.sample.run_sampling", lambda *_args, **_kwargs: "samples-cid")
    assert run_auto._run_sampling({}, cast(Any, object()), "20260320_030303") == "samples-cid"

    monkeypatch.setattr(
        "src.service.semantic_service.unify_fields_with_existing",
        lambda **_kwargs: "uf-cid",
    )
    assert (
        run_auto._unify_fields_with_existing(
            field_descriptions=[],
            existing_unified_fields_cid="cid",
            ipfs=cast(Any, object()),
            llm_config={},
        )
        == "uf-cid"
    )

    monkeypatch.setattr(
        "src.service.kg_service.run_kg_full",
        lambda *_args, **_kwargs: ("cypher.json", ["CREATE (:N)"]),
    )
    cypher_file, cypher_list = run_auto._run_kg_full(cast(Any, object()), "cid", {})
    assert cypher_file == "cypher.json"
    assert cypher_list == ["CREATE (:N)"]


def test_source_signature_handles_non_sqlite_and_empty_dsn() -> None:
    pg_sig = run_auto._source_signature(
        DatabaseSource(name="PG", driver="postgres", dsn="postgres://db", options={})
    )
    assert pg_sig == "PG|postgres|postgres://db"

    sqlite_empty_sig = run_auto._source_signature(
        DatabaseSource(name="SQL", driver="sqlite", dsn="   ", options={})
    )
    assert sqlite_empty_sig == "SQL|sqlite|"


def test_run_kg_full_wrapper_validates_tuple_item_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.service.kg_service.run_kg_full", lambda *_args, **_kwargs: (1, []))
    with pytest.raises(RuntimeError, match="cypher_file must be a string"):
        run_auto._run_kg_full(cast(Any, object()), "cid", {})

    monkeypatch.setattr(
        "src.service.kg_service.run_kg_full",
        lambda *_args, **_kwargs: ("cypher.json", "bad"),
    )
    with pytest.raises(RuntimeError, match="cypher_list must be a list"):
        run_auto._run_kg_full(cast(Any, object()), "cid", {})


def test_monitor_handles_initial_source_discovery_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call_state = {"count": 0}

    def fake_collect(_db_folder: str) -> dict[str, DatabaseSource]:
        call_state["count"] += 1
        if call_state["count"] == 1:
            raise ValueError("bad config")
        return {}

    def fake_sleep(_sec: float) -> None:
        raise StopIteration("stop")

    monkeypatch.setattr(run_auto, "_collect_candidate_sources", fake_collect)
    monkeypatch.setattr(run_auto, "_new_registry", lambda: object())
    monkeypatch.setattr("src.pipeline.run_auto.time.sleep", fake_sleep)
    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"poll_interval_sec": 0})

    with pytest.raises(StopIteration, match="stop"):
        run_auto.monitor_and_process_new_database(
            ipfs=cast(Any, object()),
            db_folder="data/dbs",
            previous_unified_fields_cid="uf-cid",
        )
    assert call_state["count"] >= 2


def test_run_llm_pipeline_accepts_explicit_llm_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeIPFS:
        def cat_json(self, _cid: str) -> object:
            return [{"table": "movie", "field": "title", "samples": ["A"]}]

        def add_json(self, _obj: object) -> object:
            return "desc-cid"

    captured: dict[str, str] = {}

    class FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            captured["api_key"] = api_key
            captured["base_url"] = base_url
            captured["model_name"] = model_name

        def generate_description(self, item: dict[str, Any]) -> dict[str, str]:
            return {
                "table": str(item["table"]),
                "field": str(item["field"]),
                "description": "ok",
            }

    monkeypatch.setattr(run_auto, "FieldDescriptionAgent", FakeFieldDescriptionAgent)
    monkeypatch.setattr(run_auto, "save_json", lambda _data, _name: "ignored.json")

    cid = run_auto.run_llm_pipeline(
        cast(Any, FakeIPFS()),
        "samples-cid",
        timestamp="20260320_040404",
        llm_config={
            "api_key": "k",
            "base_url": "http://127.0.0.1:1234/v1",
            "model_name": "m",
        },
    )
    assert cid == "desc-cid"
    assert captured == {
        "api_key": "k",
        "base_url": "http://127.0.0.1:1234/v1",
        "model_name": "m",
    }


def test_run_llm_pipeline_uses_generated_timestamp_when_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeNow:
        def strftime(self, _pattern: str) -> str:
            return "20260320_050505"

    class _FakeDateTime:
        @staticmethod
        def now() -> _FakeNow:
            return _FakeNow()

    class _FakeIPFS:
        def cat_json(self, _cid: str) -> object:
            return [{"table": "movie", "field": "id", "samples": [1]}]

        def add_json(self, _obj: object) -> object:
            return "desc-cid"

    class _FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, item: dict[str, Any]) -> dict[str, str]:
            return {
                "table": str(item["table"]),
                "field": str(item["field"]),
                "description": "ok",
            }

    captured: dict[str, str] = {}
    monkeypatch.setattr(run_auto, "datetime", _FakeDateTime)
    monkeypatch.setattr(run_auto, "FieldDescriptionAgent", _FakeFieldDescriptionAgent)
    monkeypatch.setattr(
        run_auto,
        "save_json",
        lambda _data, filename: captured.update({"filename": filename}) or "ignored.json",
    )

    cid = run_auto.run_llm_pipeline(
        cast(Any, _FakeIPFS()),
        "samples-cid",
        timestamp=None,
        llm_config=None,
    )
    assert cid == "desc-cid"
    assert captured["filename"] == "field_descriptions_20260320_050505.json"


def test_run_llm_pipeline_rejects_invalid_timestamp_and_llm_config_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeIPFS:
        def cat_json(self, _cid: str) -> object:
            return [{"table": "movie", "field": "id", "samples": [1]}]

        def add_json(self, _obj: object) -> object:
            return "desc-cid"

    class _FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, item: dict[str, Any]) -> dict[str, str]:
            return {
                "table": str(item["table"]),
                "field": str(item["field"]),
                "description": "ok",
            }

    monkeypatch.setattr(run_auto, "FieldDescriptionAgent", _FakeFieldDescriptionAgent)
    monkeypatch.setattr(run_auto, "save_json", lambda _data, _name: "ignored.json")

    with pytest.raises(RuntimeError, match="timestamp token contains unsafe characters"):
        run_auto.run_llm_pipeline(
            cast(Any, _FakeIPFS()),
            "samples-cid",
            timestamp="../bad",
            llm_config={},
        )

    with pytest.raises(RuntimeError, match="llm_config must be a mapping when provided"):
        run_auto.run_llm_pipeline(
            cast(Any, _FakeIPFS()),
            "samples-cid",
            timestamp="20260320_060606",
            llm_config=cast(Any, "bad"),
        )

    with pytest.raises(RuntimeError, match="llm_config\\['api_key'\\] must be a string"):
        run_auto.run_llm_pipeline(
            cast(Any, _FakeIPFS()),
            "samples-cid",
            timestamp="20260320_060606",
            llm_config=cast(Any, {"api_key": 1}),
        )


def test_run_auto_helper_coercions_cover_remaining_guards() -> None:
    with pytest.raises(RuntimeError, match="timestamp must be a string"):
        run_auto._coerce_timestamp_token(1)

    with pytest.raises(RuntimeError, match="timestamp must be a non-empty string"):
        run_auto._coerce_timestamp_token("   ")

    normalized = run_auto._coerce_llm_config(
        {"api_key": None, "base_url": None, "model_name": None}
    )
    assert normalized == {"api_key": "", "base_url": "", "model_name": ""}
