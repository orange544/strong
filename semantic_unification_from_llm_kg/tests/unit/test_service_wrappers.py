from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.service.kg_service as kg_service
import src.service.sample as sample_service


class _FakeIPFS:
    def __init__(self, payload: object):
        self.payload = payload
        self.uploaded: list[object] = []

    def cat_json(self, _cid: str) -> object:
        return self.payload

    def add_json(self, obj: object) -> str:
        self.uploaded.append(obj)
        return "samples-cid"


def test_run_sampling_merges_db_name_and_uploads(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_all_fields(_agent: object) -> list[dict[str, Any]]:
        return [
            {"table": "movie", "field": "id", "samples": [1]},
            {"table": "movie", "field": "title", "samples": ["A"], "db_name": "KEEP"},
        ]

    monkeypatch.setattr(sample_service, "get_all_fields", fake_get_all_fields)
    monkeypatch.setattr(
        sample_service,
        "save_json",
        lambda data, filename: captured.update({"data": data, "filename": filename}) or "samples.json",
    )

    ipfs = _FakeIPFS(payload=[])
    cid = sample_service.run_sampling(
        db_agents={"DB": cast(Any, object())},
        ipfs=cast(Any, ipfs),
        timestamp="20260320_000001",
    )

    assert cid == "samples-cid"
    assert captured["filename"] == "samples_20260320_000001.json"
    data = cast(list[dict[str, Any]], captured["data"])
    assert data[0]["db_name"] == "DB"
    assert data[1]["db_name"] == "KEEP"
    assert ipfs.uploaded[0] == data


def test_service_sample_uses_generated_timestamp_and_rejects_non_string_cid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeNow:
        def strftime(self, _pattern: str) -> str:
            return "20260320_010101"

    class _FakeDateTime:
        @staticmethod
        def now() -> _FakeNow:
            return _FakeNow()

    class _BadCIDIPFS:
        def add_json(self, _obj: object) -> object:
            return 1

    captured: dict[str, object] = {}
    monkeypatch.setattr(sample_service, "datetime", _FakeDateTime)
    monkeypatch.setattr(
        sample_service,
        "get_all_fields",
        lambda _agent: [{"table": "movie", "field": "id", "samples": [1]}],
    )
    monkeypatch.setattr(
        sample_service,
        "save_json",
        lambda _data, filename: captured.update({"filename": filename}) or "samples.json",
    )

    with pytest.raises(RuntimeError, match="CID string"):
        sample_service.run_sampling(
            db_agents={"DB": cast(Any, object())},
            ipfs=cast(Any, _BadCIDIPFS()),
            timestamp=None,
        )
    assert captured["filename"] == "samples_20260320_010101.json"


def test_run_kg_full_rejects_invalid_unified_payload_shape() -> None:
    ipfs_bad_list = _FakeIPFS(payload={"bad": "object"})
    with pytest.raises(RuntimeError, match="must be a list"):
        kg_service.run_kg_full(cast(Any, ipfs_bad_list), "cid", {})

    ipfs_bad_item = _FakeIPFS(payload=[1])
    with pytest.raises(RuntimeError, match="must be an object"):
        kg_service.run_kg_full(cast(Any, ipfs_bad_item), "cid", {})


def test_run_kg_full_builds_minimal_run_record_for_kg_only_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeKGAgent:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def generate_cypher(
            self,
            run_record: dict[str, Any],
            db_data: dict[str, dict[str, list[str]]],
            domain_field_desc_map: dict[str, list[dict[str, Any]]],
            domain_unified_map: dict[str, list[dict[str, Any]]],
            unified_fields: list[dict[str, Any]],
        ) -> list[str]:
            self.calls.append(
                {
                    "run_record": run_record,
                    "db_data": db_data,
                    "domain_field_desc_map": domain_field_desc_map,
                    "domain_unified_map": domain_unified_map,
                    "unified_fields": unified_fields,
                }
            )
            return ["MERGE (:Smoke {name:'ok'})"]

    fake_agent = FakeKGAgent()
    monkeypatch.setattr(kg_service, "generate_db_data", lambda _agents: {"DB": {"movie": ["id"]}})
    monkeypatch.setattr(kg_service, "KnowledgeGraphAgent", lambda: fake_agent)
    monkeypatch.setattr(kg_service, "save_json", lambda _data, _name: "cypher_20260320_000002.json")

    ipfs = _FakeIPFS(payload=[{"canonical_name": "movie_id", "fields": ["DB.movie.id"], "description": "id"}])
    cypher_file, cypher_list = kg_service.run_kg_full(
        ipfs=cast(Any, ipfs),
        unified_fields_cid="uf-cid",
        db_agents={},
        timestamp="20260320_000002",
    )

    assert cypher_file == "cypher_20260320_000002.json"
    assert cypher_list == ["MERGE (:Smoke {name:'ok'})"]
    assert len(fake_agent.calls) == 1
    call = fake_agent.calls[0]
    assert call["run_record"]["mode"] == "kg_only_from_existing_unified_fields"
    assert call["domain_field_desc_map"] == {"DB": []}
    assert call["domain_unified_map"] == {"DB": []}
