from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.service.semantic_service as semantic_service


class _FakeIPFS:
    def __init__(self, payload: object, *, add_result: object = "cid-updated") -> None:
        self.payload = payload
        self.add_result = add_result
        self.uploaded: list[object] = []

    def cat_json(self, _cid: str) -> object:
        return self.payload

    def add_json(self, obj: object) -> object:
        self.uploaded.append(obj)
        return self.add_result


@pytest.mark.parametrize(
    ("payload", "pattern"),
    [
        ("bad", "field_descriptions must be a list"),
        ([1], "field_descriptions item at index 0 must be an object"),
        ([{"field": "id"}], "missing non-empty table"),
        ([{"table": "movie"}], "missing non-empty field"),
    ],
)
def test_coerce_field_descriptions_rejects_invalid_payload(payload: object, pattern: str) -> None:
    with pytest.raises(RuntimeError, match=pattern):
        semantic_service._coerce_field_descriptions(payload)


@pytest.mark.parametrize(
    ("payload", "pattern"),
    [
        ("bad", "uf must be a list"),
        ([1], "uf item at index 0 must be an object"),
        ([{"fields": [], "description": ""}], "missing non-empty canonical_name"),
        ([{"canonical_name": "id", "fields": [], "description": 1}], "non-string description"),
        ([{"canonical_name": "id", "fields": "bad", "description": ""}], "non-list fields"),
        ([{"canonical_name": "id", "fields": [1], "description": ""}], "non-string field"),
    ],
)
def test_coerce_unified_fields_rejects_invalid_payload(payload: object, pattern: str) -> None:
    with pytest.raises(RuntimeError, match=pattern):
        semantic_service._coerce_unified_fields(payload, context="uf")


def test_coerce_unified_fields_deduplicates_duplicate_fields() -> None:
    rows = semantic_service._coerce_unified_fields(
        [
            {
                "canonical_name": "movie_id",
                "fields": ["DB.movie.id", "DB.movie.id", "DB.movie.code"],
                "description": "movie id",
            }
        ],
        context="uf",
    )
    assert rows[0]["fields"] == ["DB.movie.id", "DB.movie.code"]


def test_unify_new_fields_handles_empty_and_missing_methods() -> None:
    class _NoUnifyAgent:
        pass

    agent = cast(Any, _NoUnifyAgent())
    assert semantic_service._unify_new_fields(agent, []) == []

    with pytest.raises(RuntimeError, match="must provide 'unify_fields' or 'unify_within_domain'"):
        semantic_service._unify_new_fields(
            agent,
            [{"table": "movie", "field": "id", "description": "identifier"}],
        )


def test_coerce_llm_config_rejects_non_mapping_and_accepts_none_values() -> None:
    with pytest.raises(RuntimeError, match="llm_config must be a mapping"):
        semantic_service._coerce_llm_config("bad")

    normalized = semantic_service._coerce_llm_config(
        {"api_key": None, "base_url": None, "model_name": None}
    )
    assert normalized == {"api_key": "", "base_url": "", "model_name": ""}


def test_merge_unified_fields_skips_duplicate_canonical_names() -> None:
    merged = semantic_service.merge_unified_fields(
        existing=[
            {
                "canonical_name": "movie_id",
                "fields": ["DB.movie.id", "DB.movie.id"],
                "description": "existing id",
            }
        ],
        new=[
            {
                "canonical_name": "movie_id",
                "fields": ["NEW.movie.id"],
                "description": "duplicate should be skipped",
            },
            {
                "canonical_name": "movie_title",
                "fields": ["NEW.movie.title"],
                "description": "title",
            },
        ],
    )

    assert [item["canonical_name"] for item in merged] == ["movie_id", "movie_title"]
    assert merged[0]["fields"] == ["DB.movie.id"]


def test_unify_fields_with_existing_validates_cid_timestamp_and_llm_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str) -> None:
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def unify_within_domain(self, _field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return []

    monkeypatch.setattr(semantic_service, "FieldSemanticAgent", _FakeFieldSemanticAgent)
    monkeypatch.setattr(semantic_service, "save_json", lambda _data, _name: "ignored.json")

    valid_field_descriptions = [{"table": "movie", "field": "id", "description": "identifier"}]

    with pytest.raises(RuntimeError, match="existing_unified_fields_cid must be a non-empty string"):
        semantic_service.unify_fields_with_existing(
            field_descriptions=valid_field_descriptions,
            existing_unified_fields_cid=" ",
            ipfs=cast(Any, _FakeIPFS(payload=[])),
            llm_config={},
            timestamp="20260320_000001",
        )

    with pytest.raises(RuntimeError, match="timestamp token contains unsafe characters"):
        semantic_service.unify_fields_with_existing(
            field_descriptions=valid_field_descriptions,
            existing_unified_fields_cid="existing-cid",
            ipfs=cast(Any, _FakeIPFS(payload=[])),
            llm_config={},
            timestamp="../20260320",
        )

    with pytest.raises(RuntimeError, match="llm_config\\['api_key'\\] must be a string"):
        semantic_service.unify_fields_with_existing(
            field_descriptions=valid_field_descriptions,
            existing_unified_fields_cid="existing-cid",
            ipfs=cast(Any, _FakeIPFS(payload=[])),
            llm_config=cast(Any, {"api_key": 1}),
            timestamp="20260320_000001",
        )


def test_unify_fields_with_existing_uses_generated_timestamp_and_rejects_bad_ipfs_cid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeNow:
        def strftime(self, _pattern: str) -> str:
            return "20260320_020202"

    class _FakeDateTime:
        @staticmethod
        def now() -> _FakeNow:
            return _FakeNow()

    class _FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str) -> None:
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def unify_within_domain(self, _field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return []

    captured: dict[str, str] = {}

    def _fake_save_json(_data: object, filename: str) -> str:
        captured["filename"] = filename
        return filename

    monkeypatch.setattr(semantic_service, "datetime", _FakeDateTime)
    monkeypatch.setattr(semantic_service, "FieldSemanticAgent", _FakeFieldSemanticAgent)
    monkeypatch.setattr(semantic_service, "save_json", _fake_save_json)

    bad_ipfs = _FakeIPFS(payload=[], add_result=123)
    with pytest.raises(RuntimeError, match="ipfs.add_json return value must be a string"):
        semantic_service.unify_fields_with_existing(
            field_descriptions=[{"table": "movie", "field": "id", "description": "identifier"}],
            existing_unified_fields_cid="existing-cid",
            ipfs=cast(Any, bad_ipfs),
            llm_config={},
            timestamp=None,
        )

    assert captured["filename"] == "unified_fields_20260320_020202.json"
