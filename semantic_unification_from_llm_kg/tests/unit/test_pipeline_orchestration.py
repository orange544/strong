from __future__ import annotations

import json
import sqlite3
import subprocess as std_subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.kg.kg_agent as kg_agent
import src.pipeline.orchestration_common as orchestration_common
import src.pipeline.run as pipeline_run
import src.pipeline.run_auto as run_auto
import src.pipeline.run_domain_share as domain_share
import src.pipeline.run_initial as run_initial
import src.pipeline.run_sampling as run_sampling
import src.service.semantic_service as semantic_service
from src.db.database_agent import DatabaseAgent
from src.db.plugin_registry import DatabasePluginRegistry, DatabaseSource


class FakeDatabaseAgent:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeIPFS:
    def __init__(self) -> None:
        self._counter = 0
        self._storage: dict[str, Any] = {}

    def _new_cid(self) -> str:
        cid = f"cid_{self._counter}"
        self._counter += 1
        return cid

    def add_file(self, filepath: str) -> str:
        cid = self._new_cid()
        payload = json.loads(Path(filepath).read_text(encoding="utf-8"))
        self._storage[cid] = payload
        return cid

    def add_json(self, obj: Any) -> str:
        cid = self._new_cid()
        self._storage[cid] = obj
        return cid

    def cat_json(self, cid: str) -> Any:
        return self._storage[cid]


def _make_save_json(tmp_output: Path) -> Callable[[Any, str], str]:
    def _save_json(data: Any, filename: str) -> str:
        path = tmp_output / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)

    return _save_json


def _make_sources(db_paths: dict[str, str]) -> dict[str, DatabaseSource]:
    return {
        name: DatabaseSource(name=name, driver="sqlite", dsn=path, options={})
        for name, path in db_paths.items()
    }


def _create_sqlite_db(path: Path) -> str:
    conn = sqlite3.connect(str(path))
    conn.close()
    return str(path)


class FakeRegistry:
    def create_agent(self, source: DatabaseSource) -> FakeDatabaseAgent:
        return FakeDatabaseAgent(source.dsn)

    def supported_drivers(self) -> tuple[str, ...]:
        return ("sqlite",)


def test_generate_descriptions_parallel_handles_agent_failure() -> None:
    class Agent:
        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            if sample["field"] == "bad":
                raise RuntimeError("boom")
            return {
                "table": sample["table"],
                "field": sample["field"],
                "description": "ok",
            }

    samples = [
        {"table": "movie", "field": "good", "samples": [1]},
        {"table": "movie", "field": "bad", "samples": [2]},
    ]
    result = orchestration_common.generate_descriptions_parallel(
        fd_agent=Agent(),
        samples=samples,
        max_workers=2,
        domain_timeout_sec=5,
    )

    assert len(result) == 2
    by_field = {item["field"]: item for item in result}
    assert by_field["good"]["description"] == "ok"
    assert "generation_failed" in by_field["bad"]["description"]


def test_generate_descriptions_parallel_handles_timeout() -> None:
    class SlowAgent:
        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            time.sleep(0.05)
            return {
                "table": sample["table"],
                "field": sample["field"],
                "description": "slow",
            }

    result = orchestration_common.generate_descriptions_parallel(
        fd_agent=SlowAgent(),
        samples=[{"table": "movie", "field": "title", "samples": ["a"]}],
        max_workers=1,
        domain_timeout_sec=0,
    )

    assert len(result) == 1
    assert "generation_failed" in result[0]["description"]


def test_run_all_uses_full_orchestration_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    captured_records: list[dict[str, Any]] = []

    monkeypatch.setattr(
        pipeline_run,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: _make_sources({"ONE": "db1", "TWO": "db2"}),
    )
    monkeypatch.setattr(pipeline_run, "_new_registry", lambda: FakeRegistry())
    monkeypatch.setattr(
        pipeline_run,
        "PIPELINE_CONFIG",
        {
            "llm_desc_max_workers": 1,
            "llm_desc_domain_timeout_sec": 30,
            "run_max_fields_per_domain": 0,
        },
    )
    monkeypatch.setattr(
        pipeline_run,
        "DOMAIN_SHARE_DEFAULTS",
        {
            "ipfs_chain_bin": str(tmp_path / "ipfs-chain.exe"),
            "go_norn_root": "",
            "receiver": "receiver",
            "rpc_addr": "127.0.0.1:45558",
            "ipfs_api": "http://127.0.0.1:5001",
            "timeout_sec": 6,
        },
    )
    monkeypatch.setattr(pipeline_run, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(pipeline_run, "LLM_UNIFY_CONFIG", {"api_key": "", "base_url": "", "model_name": "unify"})

    monkeypatch.setattr(pipeline_run, "_ensure_ipfs_chain_binary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline_run, "_put_file_on_chain", lambda **_kwargs: ("", "txhash"))
    monkeypatch.setattr(pipeline_run, "IPFSClient", lambda: fake_ipfs)
    monkeypatch.setattr(pipeline_run, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(pipeline_run, "append_run_record", lambda record: captured_records.append(record))

    def fake_get_all_fields(agent: FakeDatabaseAgent) -> list[dict[str, Any]]:
        if agent.db_path == "db1":
            return [
                {"table": "movie", "field": "name", "samples": ["A"]},
            ]
        return [
            {"table": "movie", "field": "id", "samples": [1]},
            {"table": "credits", "field": "movie_id", "samples": [1]},
        ]

    monkeypatch.setattr(pipeline_run, "get_all_fields", fake_get_all_fields)
    monkeypatch.setattr(
        pipeline_run,
        "generate_db_data",
        lambda _agents: {
            "ONE": {"movie": ["name"]},
            "TWO": {"movie": ["id"], "credits": ["movie_id"]},
        },
    )

    class FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            return {
                "table": sample["table"],
                "field": sample["field"],
                "description": f"desc:{sample['field']}",
            }

    class FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                {
                    "canonical_name": "joined",
                    "fields": [f"TWO.{item['table']}.{item['field']}" for item in field_desc_list[:2]],
                    "description": "joined description",
                }
            ]

        def unify_across_domains(self, domain_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            first = domain_items[0]
            return [
                {
                    "canonical_name": "global_name",
                    "fields": first["fields"],
                    "description": "global description",
                }
            ]

    class FakeKnowledgeGraphAgent:
        def generate_cypher(
            self,
            run_record: dict[str, Any],
            db_data: dict[str, dict[str, list[str]]],
            domain_field_desc_map: dict[str, list[dict[str, Any]]],
            domain_unified_map: dict[str, list[dict[str, Any]]],
            unified_fields: list[dict[str, Any]],
        ) -> list[str]:
            assert run_record["mode"] == "full_pipeline_with_chain"
            assert "ONE" in db_data and "TWO" in db_data
            assert "ONE" in domain_field_desc_map and "TWO" in domain_field_desc_map
            assert "ONE" in domain_unified_map and "TWO" in domain_unified_map
            assert len(unified_fields) == 1
            return ["MERGE (:Smoke {name:'ok'});"]

    monkeypatch.setattr(pipeline_run, "FieldDescriptionAgent", FakeFieldDescriptionAgent)
    monkeypatch.setattr(pipeline_run, "FieldSemanticAgent", FakeFieldSemanticAgent)
    monkeypatch.setattr(pipeline_run, "KnowledgeGraphAgent", FakeKnowledgeGraphAgent)

    pipeline_run.run_all()

    assert len(captured_records) == 1
    record = captured_records[0]
    assert record["mode"] == "full_pipeline_with_chain"
    assert len(record["domains"]) == 2
    assert record["unified_field_count"] == 1
    assert record["cypher_count"] == 1
    assert record["domains"][0]["db_name"] == "ONE"
    assert record["domains"][1]["db_name"] == "TWO"


def test_run_load_runtime_db_sources_validates_source_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pipeline_run,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {"ONE": object()},
    )
    with pytest.raises(RuntimeError, match="invalid source object"):
        pipeline_run._load_runtime_db_sources()


def test_run_domain_share_happy_path_with_mock_llm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    captured_records: list[dict[str, Any]] = []
    imdb_db = _create_sqlite_db(tmp_path / "imdb.db")
    tmdb_db = _create_sqlite_db(tmp_path / "tmdb.db")

    monkeypatch.setattr(
        domain_share,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: _make_sources({"IMDB": imdb_db, "TMDB": tmdb_db}),
    )
    monkeypatch.setattr(domain_share, "DatabasePluginRegistry", lambda: FakeRegistry())
    monkeypatch.setattr(domain_share, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(
        domain_share,
        "_sample_fields_for_domain",
        lambda _agent, _max_fields: [{"table": "movie", "field": "name", "samples": ["A"]}],
    )
    monkeypatch.setattr(domain_share, "_make_ipfs_client", lambda _api_url: fake_ipfs)
    monkeypatch.setattr(domain_share, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(domain_share, "append_run_record", lambda record: captured_records.append(record))

    cfg = domain_share.DomainShareConfig(
        ipfs_chain_bin=tmp_path / "ipfs-chain.exe",
        go_norn_root=None,
        receiver="receiver",
        rpc_addr="127.0.0.1:45558",
        ipfs_api="http://127.0.0.1:5001",
        timeout_sec=6,
        strict=False,
        skip_chain=True,
        selected_domains=["IMDB"],
        max_fields_per_domain=5,
        mock_llm=True,
    )
    manifest = domain_share.run_domain_share(cfg)

    assert manifest["mode"] == "domain_sample_description_to_ipfs_and_chain"
    assert len(manifest["domains"]) == 1
    entry = manifest["domains"][0]
    assert entry["domain"] == "IMDB"
    assert entry["status"] == "completed"
    assert "samples_cid" in entry
    assert "field_descriptions_cid" in entry
    assert len(captured_records) == 1


def test_run_domain_share_non_strict_collects_domain_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    good_db = _create_sqlite_db(tmp_path / "good.db")
    bad_db = _create_sqlite_db(tmp_path / "bad.db")

    monkeypatch.setattr(
        domain_share,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: _make_sources({"GOOD": good_db, "BAD": bad_db}),
    )
    monkeypatch.setattr(domain_share, "DatabasePluginRegistry", lambda: FakeRegistry())
    monkeypatch.setattr(domain_share, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(domain_share, "_make_ipfs_client", lambda _api_url: fake_ipfs)
    monkeypatch.setattr(domain_share, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(domain_share, "append_run_record", lambda _record: None)

    def fake_sample_fields(agent: FakeDatabaseAgent, _max_fields: int) -> list[dict[str, Any]]:
        if str(agent.db_path).endswith("bad.db"):
            raise RuntimeError("sampling failed")
        return [{"table": "movie", "field": "name", "samples": ["X"]}]

    monkeypatch.setattr(domain_share, "_sample_fields_for_domain", fake_sample_fields)

    cfg = domain_share.DomainShareConfig(
        ipfs_chain_bin=tmp_path / "ipfs-chain.exe",
        go_norn_root=None,
        receiver="receiver",
        rpc_addr="127.0.0.1:45558",
        ipfs_api="http://127.0.0.1:5001",
        timeout_sec=6,
        strict=False,
        skip_chain=True,
        selected_domains=[],
        max_fields_per_domain=5,
        mock_llm=True,
    )
    manifest = domain_share.run_domain_share(cfg)

    statuses = {item["domain"]: item["status"] for item in manifest["domains"]}
    assert statuses["GOOD"] == "completed"
    assert statuses["BAD"] == "failed"


def test_run_domain_share_records_failed_description_without_failing_domain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    imdb_db = _create_sqlite_db(tmp_path / "imdb.db")

    monkeypatch.setattr(
        domain_share,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: _make_sources({"IMDB": imdb_db}),
    )
    monkeypatch.setattr(domain_share, "DatabasePluginRegistry", lambda: FakeRegistry())
    monkeypatch.setattr(domain_share, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(
        domain_share,
        "_sample_fields_for_domain",
        lambda _agent, _max_fields: [
            {"table": "movie", "field": "ok", "samples": ["A"]},
            {"table": "movie", "field": "bad", "samples": ["B"]},
        ],
    )
    monkeypatch.setattr(domain_share, "_make_ipfs_client", lambda _api_url: fake_ipfs)
    monkeypatch.setattr(domain_share, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(domain_share, "append_run_record", lambda _record: None)
    monkeypatch.setattr(
        domain_share,
        "PIPELINE_CONFIG",
        {"llm_desc_max_workers": 1, "llm_desc_domain_timeout_sec": 60},
    )

    class FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, sample: dict[str, Any]) -> dict[str, str]:
            if sample["field"] == "bad":
                raise RuntimeError("desc failed")
            return {
                "table": sample["table"],
                "field": sample["field"],
                "description": "ok-desc",
            }

    monkeypatch.setattr(domain_share, "FieldDescriptionAgent", FakeFieldDescriptionAgent)

    cfg = domain_share.DomainShareConfig(
        ipfs_chain_bin=tmp_path / "ipfs-chain.exe",
        go_norn_root=None,
        receiver="receiver",
        rpc_addr="127.0.0.1:45558",
        ipfs_api="http://127.0.0.1:5001",
        timeout_sec=6,
        strict=False,
        skip_chain=True,
        selected_domains=["IMDB"],
        max_fields_per_domain=5,
        mock_llm=False,
    )
    manifest = domain_share.run_domain_share(cfg)

    assert len(manifest["domains"]) == 1
    entry = manifest["domains"][0]
    assert entry["status"] == "completed"
    assert entry["description_failed_count"] == 1


def test_run_domain_share_strict_raises_domain_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    bad_db = _create_sqlite_db(tmp_path / "bad.db")

    monkeypatch.setattr(
        domain_share,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: _make_sources({"BAD": bad_db}),
    )
    monkeypatch.setattr(domain_share, "DatabasePluginRegistry", lambda: FakeRegistry())
    monkeypatch.setattr(domain_share, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(domain_share, "_make_ipfs_client", lambda _api_url: fake_ipfs)
    monkeypatch.setattr(domain_share, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(domain_share, "append_run_record", lambda _record: None)
    monkeypatch.setattr(
        domain_share,
        "_sample_fields_for_domain",
        lambda _agent, _max_fields: (_ for _ in ()).throw(RuntimeError("sampling failed")),
    )

    cfg = domain_share.DomainShareConfig(
        ipfs_chain_bin=tmp_path / "ipfs-chain.exe",
        go_norn_root=None,
        receiver="receiver",
        rpc_addr="127.0.0.1:45558",
        ipfs_api="http://127.0.0.1:5001",
        timeout_sec=6,
        strict=True,
        skip_chain=True,
        selected_domains=[],
        max_fields_per_domain=5,
        mock_llm=True,
    )

    with pytest.raises(RuntimeError, match="sampling failed"):
        domain_share.run_domain_share(cfg)


def test_run_domain_share_strict_persists_manifest_before_raise(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    bad_db = _create_sqlite_db(tmp_path / "bad_strict.db")
    captured_records: list[dict[str, Any]] = []

    monkeypatch.setattr(
        domain_share,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: _make_sources({"BAD": bad_db}),
    )
    monkeypatch.setattr(domain_share, "DatabasePluginRegistry", lambda: FakeRegistry())
    monkeypatch.setattr(domain_share, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(domain_share, "_make_ipfs_client", lambda _api_url: fake_ipfs)
    monkeypatch.setattr(domain_share, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(domain_share, "append_run_record", lambda record: captured_records.append(record))
    monkeypatch.setattr(
        domain_share,
        "_sample_fields_for_domain",
        lambda _agent, _max_fields: (_ for _ in ()).throw(RuntimeError("sampling failed")),
    )

    cfg = domain_share.DomainShareConfig(
        ipfs_chain_bin=tmp_path / "ipfs-chain.exe",
        go_norn_root=None,
        receiver="receiver",
        rpc_addr="127.0.0.1:45558",
        ipfs_api="http://127.0.0.1:5001",
        timeout_sec=6,
        strict=True,
        skip_chain=True,
        selected_domains=[],
        max_fields_per_domain=5,
        mock_llm=True,
    )

    with pytest.raises(RuntimeError, match="sampling failed"):
        domain_share.run_domain_share(cfg)

    assert len(captured_records) == 1
    manifest = captured_records[0]
    assert len(manifest["domains"]) == 1
    entry = manifest["domains"][0]
    assert entry["domain"] == "BAD"
    assert entry["status"] == "failed"
    assert "sampling failed" in entry["error"]

    manifest_files = list((tmp_path / "outputs").glob("domain_share_manifest_*.json"))
    assert len(manifest_files) == 1


def test_run_domain_share_non_strict_marks_chain_failure_with_consistent_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    good_db = _create_sqlite_db(tmp_path / "good_chain.db")
    bad_db = _create_sqlite_db(tmp_path / "bad_chain.db")

    monkeypatch.setattr(
        domain_share,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: _make_sources({"GOOD": good_db, "BAD": bad_db}),
    )
    monkeypatch.setattr(domain_share, "DatabasePluginRegistry", lambda: FakeRegistry())
    monkeypatch.setattr(domain_share, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(
        domain_share,
        "_sample_fields_for_domain",
        lambda _agent, _max_fields: [{"table": "movie", "field": "name", "samples": ["X"]}],
    )
    monkeypatch.setattr(domain_share, "_make_ipfs_client", lambda _api_url: fake_ipfs)
    monkeypatch.setattr(domain_share, "_ensure_ipfs_chain_binary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(domain_share, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(domain_share, "append_run_record", lambda _record: None)

    def fake_put_file_on_chain(**kwargs: Any) -> tuple[str, str]:
        key = str(kwargs["key"])
        if key.startswith("REGISTER_SAMPLE:BAD_"):
            raise RuntimeError("sample chain failed")
        cid = f"chain-{key}"
        payload = json.loads(Path(str(kwargs["file_path"])).read_text(encoding="utf-8"))
        fake_ipfs._storage[cid] = payload
        return cid, "tx-ok"

    monkeypatch.setattr(domain_share, "_put_file_on_chain", fake_put_file_on_chain)

    cfg = domain_share.DomainShareConfig(
        ipfs_chain_bin=tmp_path / "ipfs-chain.exe",
        go_norn_root=None,
        receiver="receiver",
        rpc_addr="127.0.0.1:45558",
        ipfs_api="http://127.0.0.1:5001",
        timeout_sec=6,
        strict=False,
        skip_chain=False,
        selected_domains=[],
        max_fields_per_domain=5,
        mock_llm=True,
    )
    manifest = domain_share.run_domain_share(cfg)

    by_domain = {item["domain"]: item for item in manifest["domains"]}
    good_entry = by_domain["GOOD"]
    bad_entry = by_domain["BAD"]

    assert good_entry["status"] == "completed"
    assert str(good_entry["sample_chain_cid"]).startswith("chain-REGISTER_SAMPLE:GOOD_")
    assert good_entry["sample_tx_hash"] == "tx-ok"
    assert str(good_entry["description_chain_cid"]).startswith("chain-REGISTER_DESCRIPTION:GOOD_")
    assert good_entry["description_tx_hash"] == "tx-ok"
    assert good_entry["failed_stage"] == ""
    assert good_entry["error_context"] == {}

    assert bad_entry["status"] == "failed"
    assert bad_entry["sample_chain_key"].startswith("REGISTER_SAMPLE:BAD_")
    assert bad_entry["sample_chain_cid"] == ""
    assert bad_entry["sample_tx_hash"] == ""
    assert bad_entry["description_chain_key"] == ""
    assert bad_entry["description_chain_cid"] == ""
    assert bad_entry["description_tx_hash"] == ""
    assert bad_entry["failed_stage"] == "sample_chain"
    assert bad_entry["error_context"]["stage"] == "sample_chain"
    assert str(bad_entry["error_context"]["chain_key"]).startswith("REGISTER_SAMPLE:BAD_")
    assert str(bad_entry["error_context"]["file_path"]).endswith(".json")
    assert bad_entry["error_context"]["timeout_sec"] == 6
    assert "sample chain registration failed" in bad_entry["error"]


def test_run_domain_share_strict_raises_on_description_chain_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    imdb_db = _create_sqlite_db(tmp_path / "imdb_chain.db")
    chain_calls = {"count": 0}
    captured_records: list[dict[str, Any]] = []

    monkeypatch.setattr(
        domain_share,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: _make_sources({"IMDB": imdb_db}),
    )
    monkeypatch.setattr(domain_share, "DatabasePluginRegistry", lambda: FakeRegistry())
    monkeypatch.setattr(domain_share, "LLM_DESC_CONFIG", {"api_key": "", "base_url": "", "model_name": "desc"})
    monkeypatch.setattr(
        domain_share,
        "_sample_fields_for_domain",
        lambda _agent, _max_fields: [{"table": "movie", "field": "name", "samples": ["X"]}],
    )
    monkeypatch.setattr(domain_share, "_make_ipfs_client", lambda _api_url: fake_ipfs)
    monkeypatch.setattr(domain_share, "_ensure_ipfs_chain_binary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(domain_share, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(domain_share, "append_run_record", lambda record: captured_records.append(record))

    def fake_put_file_on_chain(**_kwargs: Any) -> tuple[str, str]:
        chain_calls["count"] += 1
        if chain_calls["count"] == 1:
            file_path = Path(str(_kwargs["file_path"]))
            fake_ipfs._storage["sample-chain-cid"] = json.loads(file_path.read_text(encoding="utf-8"))
            return "sample-chain-cid", "sample-tx"
        raise RuntimeError("description chain failed")

    monkeypatch.setattr(domain_share, "_put_file_on_chain", fake_put_file_on_chain)

    cfg = domain_share.DomainShareConfig(
        ipfs_chain_bin=tmp_path / "ipfs-chain.exe",
        go_norn_root=None,
        receiver="receiver",
        rpc_addr="127.0.0.1:45558",
        ipfs_api="http://127.0.0.1:5001",
        timeout_sec=6,
        strict=True,
        skip_chain=False,
        selected_domains=["IMDB"],
        max_fields_per_domain=5,
        mock_llm=True,
    )

    with pytest.raises(RuntimeError, match="description chain registration failed"):
        domain_share.run_domain_share(cfg)
    assert chain_calls["count"] == 2
    assert len(captured_records) == 1
    entry = captured_records[0]["domains"][0]
    assert entry["domain"] == "IMDB"
    assert entry["status"] == "failed"
    assert entry["failed_stage"] == "description_chain"
    assert entry["error_context"]["stage"] == "description_chain"
    assert str(entry["error_context"]["chain_key"]).startswith("REGISTER_DESCRIPTION:IMDB_")
    assert str(entry["error_context"]["file_path"]).endswith(".json")


def test_run_domain_share_create_agent_for_source_validates_sqlite_dsn(
    tmp_path: Path,
) -> None:
    registry = DatabasePluginRegistry()

    with pytest.raises(RuntimeError, match="empty DSN"):
        domain_share._create_agent_for_source(
            registry,
            DatabaseSource(name="EMPTY", driver="sqlite", dsn="   ", options={}),
        )

    missing_file = tmp_path / "missing.db"
    assert missing_file.exists() is False
    with pytest.raises(RuntimeError, match="missing sqlite file"):
        domain_share._create_agent_for_source(
            registry,
            DatabaseSource(name="MISSING", driver="sqlite", dsn=str(missing_file), options={}),
        )
    assert missing_file.exists() is False


def test_run_domain_share_sample_fields_falls_back_when_agent_is_not_sqlite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NoConnAgent:
        pass

    monkeypatch.setattr(
        domain_share,
        "get_all_fields",
        lambda _agent: [
            {"table": "movie", "field": "id", "samples": [1]},
            {"table": "movie", "field": "title", "samples": ["A"]},
        ],
    )

    samples = domain_share._sample_fields_for_domain(cast(Any, NoConnAgent()), max_fields=1)
    assert len(samples) == 1
    assert samples[0]["field"] == "id"


def test_run_domain_share_sample_fields_falls_back_when_sqlite_introspection_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenConn:
        def cursor(self) -> object:
            raise RuntimeError("boom")

    class BrokenAgent:
        def __init__(self) -> None:
            self.conn = BrokenConn()

        def sample_field(self, table: str, field: str) -> dict[str, Any]:
            return {"table": table, "field": field, "samples": [1]}

    monkeypatch.setattr(
        domain_share,
        "get_all_fields",
        lambda _agent: [
            {"table": "movie", "field": "id", "samples": [1]},
            {"table": "movie", "field": "title", "samples": ["A"]},
        ],
    )

    samples = domain_share._sample_fields_for_domain(cast(Any, BrokenAgent()), max_fields=1)
    assert len(samples) == 1
    assert samples[0]["field"] == "id"


def test_run_sampling_rejects_unsupported_database_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_sampling,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {
            "PG": DatabaseSource(
                name="PG",
                driver="postgres",
                dsn="postgresql://user:pwd@127.0.0.1:5432/movies",
                options={},
            )
        },
    )

    with pytest.raises(RuntimeError, match="Unsupported database driver 'postgres'"):
        run_sampling.run_sampling_only(upload_to_ipfs=False, timestamp="20260319_000000")


def test_run_auto_monitor_processes_new_db_once_and_closes_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ipfs = FakeIPFS()
    created_agents: list[FakeDatabaseAgent] = []
    run_sampling_calls: list[dict[str, FakeDatabaseAgent]] = []

    source_a = DatabaseSource(name="A", driver="sqlite", dsn="a.db", options={})
    source_b = DatabaseSource(name="B", driver="sqlite", dsn="b.db", options={})
    source_states = [
        {"A": source_a},
        {"A": source_a, "B": source_b},
        {"A": source_a, "B": source_b},
    ]
    source_idx = {"value": 0}

    def fake_collect_sources(_db_folder: str) -> dict[str, DatabaseSource]:
        idx = source_idx["value"]
        source_idx["value"] += 1
        if idx >= len(source_states):
            return source_states[-1]
        return source_states[idx]

    def fake_create_agent(
        _registry: Any,
        source: DatabaseSource,
    ) -> dict[str, FakeDatabaseAgent]:
        agent = FakeDatabaseAgent("db-path")
        created_agents.append(agent)
        return {source.name: agent}

    def fake_run_sampling(
        db_agents: dict[str, FakeDatabaseAgent],
        _ipfs: Any,
        _timestamp: str,
    ) -> str:
        run_sampling_calls.append(db_agents)
        return "samples-cid"

    sleep_calls = {"count": 0}

    def fake_sleep(_seconds: int) -> None:
        sleep_calls["count"] += 1
        if sleep_calls["count"] >= 2:
            raise RuntimeError("stop loop")

    monkeypatch.setattr(run_auto, "_collect_candidate_sources", fake_collect_sources)
    monkeypatch.setattr(run_auto, "_new_registry", lambda: object())
    monkeypatch.setattr(run_auto, "_create_agent_for_source", fake_create_agent)
    monkeypatch.setattr(run_auto, "_run_sampling", fake_run_sampling)
    monkeypatch.setattr(run_auto, "run_llm_pipeline", lambda *_args, **_kwargs: "desc-cid")
    monkeypatch.setattr(run_auto, "_unify_fields_with_existing", lambda **_kwargs: "uf-cid")
    monkeypatch.setattr(run_auto, "_run_kg_full", lambda *_args, **_kwargs: ("cypher.json", []))
    monkeypatch.setattr("src.pipeline.run_auto.time.sleep", fake_sleep)
    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"poll_interval_sec": 0})
    monkeypatch.setattr(ipfs, "cat_json", lambda _cid: [])

    with pytest.raises(RuntimeError, match="stop loop"):
        run_auto.monitor_and_process_new_database(cast(Any, ipfs), "data/dbs", "old-uf-cid")

    assert len(run_sampling_calls) == 1
    assert len(created_agents) == 1
    assert created_agents[0].closed is True


def test_run_auto_monitor_retries_failed_new_source_until_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ipfs = FakeIPFS()
    created_agents: list[FakeDatabaseAgent] = []
    run_sampling_attempts = {"count": 0}
    unify_calls = {"count": 0}

    source_a = DatabaseSource(name="A", driver="sqlite", dsn="a.db", options={})
    source_b = DatabaseSource(name="B", driver="sqlite", dsn="b.db", options={})
    source_states = [
        {"A": source_a},
        {"A": source_a, "B": source_b},
        {"A": source_a, "B": source_b},
        {"A": source_a, "B": source_b},
    ]
    source_idx = {"value": 0}

    def fake_collect_sources(_db_folder: str) -> dict[str, DatabaseSource]:
        idx = source_idx["value"]
        source_idx["value"] += 1
        if idx >= len(source_states):
            return source_states[-1]
        return source_states[idx]

    def fake_create_agent(
        _registry: Any,
        source: DatabaseSource,
    ) -> dict[str, FakeDatabaseAgent]:
        agent = FakeDatabaseAgent(source.dsn)
        created_agents.append(agent)
        return {source.name: agent}

    def fake_run_sampling(
        _db_agents: dict[str, FakeDatabaseAgent],
        _ipfs: Any,
        _timestamp: str,
    ) -> str:
        run_sampling_attempts["count"] += 1
        if run_sampling_attempts["count"] == 1:
            raise RuntimeError("transient sampling error")
        return "samples-cid"

    sleep_calls = {"count": 0}

    def fake_sleep(_seconds: int) -> None:
        sleep_calls["count"] += 1
        if sleep_calls["count"] >= 3:
            raise RuntimeError("stop loop")

    monkeypatch.setattr(run_auto, "_collect_candidate_sources", fake_collect_sources)
    monkeypatch.setattr(run_auto, "_new_registry", lambda: object())
    monkeypatch.setattr(run_auto, "_create_agent_for_source", fake_create_agent)
    monkeypatch.setattr(run_auto, "_run_sampling", fake_run_sampling)
    monkeypatch.setattr(run_auto, "run_llm_pipeline", lambda *_args, **_kwargs: "desc-cid")

    def fake_unify_fields_with_existing(**_kwargs: Any) -> str:
        unify_calls["count"] += 1
        return "uf-cid"

    monkeypatch.setattr(run_auto, "_unify_fields_with_existing", fake_unify_fields_with_existing)
    monkeypatch.setattr(run_auto, "_run_kg_full", lambda *_args, **_kwargs: ("cypher.json", []))
    monkeypatch.setattr("src.pipeline.run_auto.time.sleep", fake_sleep)
    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"poll_interval_sec": 0})
    monkeypatch.setattr(ipfs, "cat_json", lambda _cid: [])

    with pytest.raises(RuntimeError, match="stop loop"):
        run_auto.monitor_and_process_new_database(cast(Any, ipfs), "data/dbs", "old-uf-cid")

    assert run_sampling_attempts["count"] == 2
    assert unify_calls["count"] == 1
    assert len(created_agents) == 2
    assert all(agent.closed for agent in created_agents)


def test_run_auto_monitor_ignores_sqlite_dsn_format_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ipfs = FakeIPFS()
    repo_root = Path(__file__).resolve().parents[2]
    db_dir = repo_root / "outputs"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_dir / f"test_run_auto_signature_{time.time_ns()}.db"

    try:
        conn = sqlite3.connect(str(db_file))
        conn.close()

        rel_dsn = str(db_file.relative_to(repo_root))
        abs_dsn = str(db_file.resolve())

        source_rel = DatabaseSource(name="APP", driver="sqlite", dsn=rel_dsn, options={})
        source_abs = DatabaseSource(name="APP", driver="sqlite", dsn=abs_dsn, options={})
        source_states = [
            {"APP": source_rel},
            {"APP": source_abs},
            {"APP": source_abs},
        ]
        source_idx = {"value": 0}

        def fake_collect_sources(_db_folder: str) -> dict[str, DatabaseSource]:
            idx = source_idx["value"]
            source_idx["value"] += 1
            if idx >= len(source_states):
                return source_states[-1]
            return source_states[idx]

        create_agent_calls: list[DatabaseSource] = []
        run_sampling_calls: list[dict[str, FakeDatabaseAgent]] = []

        def fake_create_agent(
            _registry: Any,
            source: DatabaseSource,
        ) -> dict[str, FakeDatabaseAgent]:
            create_agent_calls.append(source)
            return {source.name: FakeDatabaseAgent(source.dsn)}

        def fake_run_sampling(
            db_agents: dict[str, FakeDatabaseAgent],
            _ipfs: Any,
            _timestamp: str,
        ) -> str:
            run_sampling_calls.append(db_agents)
            return "samples-cid"

        sleep_calls = {"count": 0}

        def fake_sleep(_seconds: int) -> None:
            sleep_calls["count"] += 1
            if sleep_calls["count"] >= 2:
                raise RuntimeError("stop loop")

        monkeypatch.setattr(run_auto, "_collect_candidate_sources", fake_collect_sources)
        monkeypatch.setattr(run_auto, "_new_registry", lambda: object())
        monkeypatch.setattr(run_auto, "_create_agent_for_source", fake_create_agent)
        monkeypatch.setattr(run_auto, "_run_sampling", fake_run_sampling)
        monkeypatch.setattr(run_auto, "run_llm_pipeline", lambda *_args, **_kwargs: "desc-cid")
        monkeypatch.setattr(run_auto, "_unify_fields_with_existing", lambda **_kwargs: "uf-cid")
        monkeypatch.setattr(run_auto, "_run_kg_full", lambda *_args, **_kwargs: ("cypher.json", []))
        monkeypatch.setattr("src.pipeline.run_auto.time.sleep", fake_sleep)
        monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"poll_interval_sec": 0})
        monkeypatch.setattr(ipfs, "cat_json", lambda _cid: [])

        with pytest.raises(RuntimeError, match="stop loop"):
            run_auto.monitor_and_process_new_database(cast(Any, ipfs), "data/dbs", "old-uf-cid")

        assert create_agent_calls == []
        assert run_sampling_calls == []
    finally:
        if db_file.exists():
            db_file.unlink()


@pytest.mark.parametrize(
    "samples_payload",
    [
        [{"table": "movie", "field": "title", "samples": ["A"]}],
        {"samples": [{"table": "movie", "field": "title", "samples": ["A"]}]},
    ],
)
def test_run_auto_llm_pipeline_generates_and_uploads_descriptions(
    monkeypatch: pytest.MonkeyPatch,
    samples_payload: Any,
) -> None:
    fake_ipfs = FakeIPFS()
    fake_ipfs._storage["samples-cid"] = samples_payload

    class FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, item: dict[str, Any]) -> dict[str, str]:
            return {
                "table": str(item["table"]),
                "field": str(item["field"]),
                "description": "mock-desc",
            }

    monkeypatch.setattr(run_auto, "FieldDescriptionAgent", FakeFieldDescriptionAgent)
    monkeypatch.setattr(run_auto, "save_json", lambda _data, _name: "ignored.json")

    cid = run_auto.run_llm_pipeline(cast(Any, fake_ipfs), "samples-cid", timestamp="20260319_000000")

    assert cid.startswith("cid_")
    payload = fake_ipfs.cat_json(cid)
    assert isinstance(payload, list)
    assert payload[0]["description"] == "mock-desc"


@pytest.mark.parametrize(
    ("samples_payload", "error_pattern"),
    [
        ({"samples": "not-a-list"}, "sample payload from IPFS must be a list or an artifact with 'samples'"),
        ([123], "sample item at index 0 must be an object"),
        ({"samples": [{"table": "movie", "field": "title"}, 1]}, "sample item at index 1 must be an object"),
    ],
)
def test_run_auto_llm_pipeline_rejects_invalid_sample_payload(
    samples_payload: Any,
    error_pattern: str,
) -> None:
    fake_ipfs = FakeIPFS()
    fake_ipfs._storage["samples-cid"] = samples_payload

    with pytest.raises(RuntimeError, match=error_pattern):
        run_auto.run_llm_pipeline(cast(Any, fake_ipfs), "samples-cid", timestamp="20260319_000000")


def test_semantic_service_falls_back_to_unify_within_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    existing_cid = fake_ipfs.add_json(
        [
            {
                "canonical_name": "title",
                "fields": ["OLD.movie.title"],
                "description": "movie title",
            }
        ]
    )
    call_state = {"legacy": 0, "within": 0}

    class FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            call_state["within"] += 1
            assert len(field_desc_list) == 1
            return [
                {
                    "canonical_name": "release_year",
                    "fields": ["NEW.movie.year"],
                    "description": "release year",
                }
            ]

    monkeypatch.setattr(semantic_service, "FieldSemanticAgent", FakeFieldSemanticAgent)
    monkeypatch.setattr(semantic_service, "save_json", lambda _data, _name: "ignored.json")

    updated_cid = semantic_service.unify_fields_with_existing(
        field_descriptions=[{"table": "movie", "field": "year", "description": "release year"}],
        existing_unified_fields_cid=existing_cid,
        ipfs=cast(Any, fake_ipfs),
        llm_config={},
        timestamp="20260319_000000",
    )

    merged = fake_ipfs.cat_json(updated_cid)
    assert call_state["legacy"] == 0
    assert call_state["within"] == 1
    assert [item["canonical_name"] for item in merged] == ["title", "release_year"]


def test_semantic_service_prefers_legacy_unify_fields_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    existing_cid = fake_ipfs.add_json([])
    call_state = {"legacy": 0, "within": 0}

    class FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def unify_fields(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            call_state["legacy"] += 1
            assert len(field_desc_list) == 1
            return [
                {
                    "canonical_name": "legacy_only",
                    "fields": ["NEW.movie.id"],
                    "description": "legacy path",
                }
            ]

        def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            call_state["within"] += 1
            return [
                {
                    "canonical_name": "within_only",
                    "fields": ["NEW.movie.id"],
                    "description": "within path",
                }
            ]

    monkeypatch.setattr(semantic_service, "FieldSemanticAgent", FakeFieldSemanticAgent)
    monkeypatch.setattr(semantic_service, "save_json", lambda _data, _name: "ignored.json")

    updated_cid = semantic_service.unify_fields_with_existing(
        field_descriptions=[{"table": "movie", "field": "id", "description": "identifier"}],
        existing_unified_fields_cid=existing_cid,
        ipfs=cast(Any, fake_ipfs),
        llm_config={},
        timestamp="20260319_000000",
    )

    merged = fake_ipfs.cat_json(updated_cid)
    assert call_state["legacy"] == 1
    assert call_state["within"] == 0
    assert [item["canonical_name"] for item in merged] == ["legacy_only"]


def test_semantic_service_rejects_invalid_existing_unified_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    fake_ipfs._storage["bad-existing-cid"] = {"unexpected": "object"}

    class FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return field_desc_list

    monkeypatch.setattr(semantic_service, "FieldSemanticAgent", FakeFieldSemanticAgent)
    monkeypatch.setattr(semantic_service, "save_json", lambda _data, _name: "ignored.json")

    with pytest.raises(RuntimeError, match="existing_unified_fields must be a list"):
        semantic_service.unify_fields_with_existing(
            field_descriptions=[{"table": "movie", "field": "id", "description": "identifier"}],
            existing_unified_fields_cid="bad-existing-cid",
            ipfs=cast(Any, fake_ipfs),
            llm_config={},
            timestamp="20260319_000000",
        )


def test_run_auto_create_agent_for_new_db_handles_missing_and_success(
    tmp_path: Path,
) -> None:
    registry = run_auto._new_registry()

    with pytest.raises(RuntimeError, match="New database file not found"):
        run_auto._create_agent_for_new_db(registry, str(tmp_path), "missing.db")

    db_file = tmp_path / "new.db"
    conn = sqlite3.connect(str(db_file))
    conn.close()

    db_agents = run_auto._create_agent_for_new_db(registry, str(tmp_path), "new.db")
    assert list(db_agents.keys()) == ["new.db"]
    for agent in db_agents.values():
        agent.close()


def test_run_auto_collect_candidate_sources_prefers_config_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auto_file = tmp_path / "AUTO.db"
    auto_file.write_text("x", encoding="utf-8")

    monkeypatch.setattr(
        run_auto,
        "_load_runtime_db_sources",
        lambda: {
            "AUTO": DatabaseSource(name="AUTO", driver="sqlite", dsn="configured.db", options={}),
            "CFG": DatabaseSource(name="CFG", driver="sqlite", dsn="cfg.db", options={}),
        },
    )

    candidates = run_auto._collect_candidate_sources(str(tmp_path))

    assert set(candidates.keys()) == {"AUTO", "CFG"}
    assert candidates["AUTO"].dsn == "configured.db"
    assert candidates["CFG"].dsn == "cfg.db"


def test_run_auto_create_agent_for_source_validates_dsn_and_driver(
    tmp_path: Path,
) -> None:
    registry = run_auto._new_registry()

    with pytest.raises(RuntimeError, match="empty DSN"):
        run_auto._create_agent_for_source(
            registry,
            DatabaseSource(name="EMPTY", driver="sqlite", dsn="   ", options={}),
        )

    with pytest.raises(RuntimeError, match="Unsupported database driver 'postgres'"):
        run_auto._create_agent_for_source(
            registry,
            DatabaseSource(name="PG", driver="postgres", dsn="postgresql://127.0.0.1/db", options={}),
        )

    with pytest.raises(RuntimeError, match="New database file not found"):
        run_auto._create_agent_for_source(
            registry,
            DatabaseSource(name="MISSING", driver="sqlite", dsn=str(tmp_path / "missing.db"), options={}),
        )

    repo_root = Path(__file__).resolve().parents[2]
    db_dir = repo_root / "outputs"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_dir / f"test_run_auto_relative_{time.time_ns()}.db"
    try:
        conn = sqlite3.connect(str(db_file))
        conn.close()

        relative_dsn = str(db_file.relative_to(repo_root))
        db_agents = run_auto._create_agent_for_source(
            registry,
            DatabaseSource(name="REL", driver="sqlite", dsn=relative_dsn, options={}),
        )
        assert list(db_agents.keys()) == ["REL"]
        for agent in db_agents.values():
            assert Path(agent.db_path).resolve() == db_file.resolve()
            agent.close()
    finally:
        if db_file.exists():
            db_file.unlink()


def test_run_initial_create_db_agents_validates_sqlite_dsn_file_checks() -> None:
    class CapturingRegistry:
        def __init__(self) -> None:
            self.sources: list[DatabaseSource] = []

        def create_agent(self, source: DatabaseSource) -> FakeDatabaseAgent:
            self.sources.append(source)
            return FakeDatabaseAgent(source.dsn)

        def supported_drivers(self) -> tuple[str, ...]:
            return ("sqlite", "postgres")

    registry = CapturingRegistry()
    missing_source = DatabaseSource(name="MISSING", driver="sqlite", dsn="outputs/missing_init.db", options={})
    with pytest.raises(RuntimeError, match="missing sqlite file"):
        run_initial._create_db_agents({"MISSING": missing_source}, cast(Any, registry))

    repo_root = Path(__file__).resolve().parents[2]
    empty_file = repo_root / "outputs" / f"test_run_initial_empty_{time.time_ns()}.db"
    empty_file.parent.mkdir(parents=True, exist_ok=True)
    empty_file.write_text("", encoding="utf-8")
    try:
        with pytest.raises(RuntimeError, match="empty sqlite file"):
            run_initial._create_db_agents(
                {"EMPTY": DatabaseSource(name="EMPTY", driver="sqlite", dsn=str(empty_file), options={})},
                cast(Any, registry),
            )
    finally:
        if empty_file.exists():
            empty_file.unlink()

    db_file = repo_root / "outputs" / f"test_run_initial_relative_{time.time_ns()}.db"
    db_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = sqlite3.connect(str(db_file))
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        relative_dsn = str(db_file.relative_to(repo_root))
        db_agents = run_initial._create_db_agents(
            {"REL": DatabaseSource(name="REL", driver="sqlite", dsn=relative_dsn, options={})},
            cast(Any, registry),
        )
        assert list(db_agents.keys()) == ["REL"]
        assert Path(db_agents["REL"].db_path).resolve() == db_file.resolve()
        assert Path(registry.sources[-1].dsn).resolve() == db_file.resolve()
        for agent in db_agents.values():
            agent.close()
    finally:
        if db_file.exists():
            db_file.unlink()


def test_run_initial_create_db_agents_keeps_non_sqlite_source_unchanged() -> None:
    class CapturingRegistry:
        def __init__(self) -> None:
            self.sources: list[DatabaseSource] = []

        def create_agent(self, source: DatabaseSource) -> FakeDatabaseAgent:
            self.sources.append(source)
            return FakeDatabaseAgent(source.dsn)

        def supported_drivers(self) -> tuple[str, ...]:
            return ("sqlite", "postgres")

    registry = CapturingRegistry()
    source = DatabaseSource(
        name="PG",
        driver="postgres",
        dsn="postgresql://127.0.0.1:5432/app",
        options={},
    )
    db_agents = run_initial._create_db_agents({"PG": source}, cast(Any, registry))

    assert registry.sources[0].dsn == source.dsn
    assert db_agents["PG"].db_path == source.dsn
    for agent in db_agents.values():
        agent.close()


def test_run_initial_load_runtime_db_sources_falls_back_on_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_invalid(
        *,
        legacy_db_paths: dict[str, str],
    ) -> dict[str, DatabaseSource]:
        del legacy_db_paths
        raise ValueError("bad source json")

    monkeypatch.setattr(run_initial, "load_db_sources_from_env", _raise_invalid)
    monkeypatch.setattr(
        run_initial,
        "DB_PATHS",
        {
            "IMDB": "data/dbs/imdb.db",
            " ": "data/dbs/ignored.db",
            "TMDB": "   ",
        },
    )

    sources = run_initial._load_runtime_db_sources()

    assert set(sources.keys()) == {"IMDB"}
    assert sources["IMDB"].driver == "sqlite"
    assert sources["IMDB"].dsn == "data/dbs/imdb.db"


def test_run_initial_create_db_agents_closes_open_agents_on_failure(
    tmp_path: Path,
) -> None:
    class FlakyRegistry:
        def __init__(self) -> None:
            self.created_agents: list[FakeDatabaseAgent] = []

        def create_agent(self, source: DatabaseSource) -> FakeDatabaseAgent:
            if source.name == "BAD":
                raise ValueError("boom")
            agent = FakeDatabaseAgent(source.dsn)
            self.created_agents.append(agent)
            return agent

        def supported_drivers(self) -> tuple[str, ...]:
            return ("sqlite",)

    good_db = tmp_path / "good.db"
    bad_db = tmp_path / "bad.db"
    for db_file in (good_db, bad_db):
        conn = sqlite3.connect(str(db_file))
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()

    registry = FlakyRegistry()
    sources = {
        "GOOD": DatabaseSource(name="GOOD", driver="sqlite", dsn=str(good_db), options={}),
        "BAD": DatabaseSource(name="BAD", driver="sqlite", dsn=str(bad_db), options={}),
    }
    with pytest.raises(RuntimeError, match="Failed to create database agent for source 'BAD'"):
        run_initial._create_db_agents(sources, cast(Any, registry))

    assert len(registry.created_agents) == 1
    assert registry.created_agents[0].closed is True


def test_run_initial_run_all_happy_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    captured_records: list[dict[str, Any]] = []

    one_db = tmp_path / "one.db"
    two_db = tmp_path / "two.db"
    for db_file in (one_db, two_db):
        conn = sqlite3.connect(str(db_file))
        conn.execute("CREATE TABLE movie (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO movie (name) VALUES (?)", ("A",))
        conn.commit()
        conn.close()

    monkeypatch.setattr(
        run_initial,
        "_collect_candidate_sources",
        lambda _db_folder: _make_sources({"ONE": str(one_db), "TWO": str(two_db)}),
    )
    monkeypatch.setattr(run_initial, "DatabasePluginRegistry", lambda: FakeRegistry())
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
    monkeypatch.setattr(run_initial, "IPFSClient", lambda: fake_ipfs)
    monkeypatch.setattr(run_initial, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(run_initial, "append_run_record", lambda record: captured_records.append(record))

    def fake_get_all_fields(agent: FakeDatabaseAgent) -> list[dict[str, Any]]:
        if Path(agent.db_path).name == "one.db":
            return [{"table": "movie", "field": "name", "samples": ["A"]}]
        return [
            {"table": "movie", "field": "id", "samples": [1]},
            {"table": "credits", "field": "movie_id", "samples": [1]},
        ]

    monkeypatch.setattr(run_initial, "get_all_fields", fake_get_all_fields)
    monkeypatch.setattr(
        run_initial,
        "generate_db_data",
        lambda _agents: {
            "ONE": {"movie": ["name"]},
            "TWO": {"movie": ["id"], "credits": ["movie_id"]},
        },
    )

    class FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            return {
                "table": sample["table"],
                "field": sample["field"],
                "description": f"desc:{sample['field']}",
            }

    class FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                {
                    "canonical_name": "within_joined",
                    "fields": [f"{item['table']}.{item['field']}" for item in field_desc_list],
                    "description": "within",
                }
            ]

        def unify_across_domains(self, domain_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [
                {
                    "canonical_name": "global_name",
                    "fields": domain_items[0]["fields"],
                    "description": "global",
                }
            ]

    class FakeKnowledgeGraphAgent:
        def generate_cypher(
            self,
            run_record: dict[str, Any],
            db_data: dict[str, dict[str, list[str]]],
            domain_field_desc_map: dict[str, list[dict[str, Any]]],
            domain_unified_map: dict[str, list[dict[str, Any]]],
            unified_fields: list[dict[str, Any]],
        ) -> list[str]:
            assert len(run_record["domains"]) == 2
            assert "ONE" in db_data and "TWO" in db_data
            assert "ONE" in domain_field_desc_map and "TWO" in domain_field_desc_map
            assert "ONE" in domain_unified_map and "TWO" in domain_unified_map
            assert len(unified_fields) == 1
            return ["MERGE (:Smoke {name:'ok'});"]

    monkeypatch.setattr(run_initial, "FieldDescriptionAgent", FakeFieldDescriptionAgent)
    monkeypatch.setattr(run_initial, "FieldSemanticAgent", FakeFieldSemanticAgent)
    monkeypatch.setattr(run_initial, "KnowledgeGraphAgent", FakeKnowledgeGraphAgent)

    run_initial.run_all()

    assert len(captured_records) == 1
    record = captured_records[0]
    assert record["status"] == "completed"
    assert len(record["domains"]) == 2
    assert record["unified_field_count"] == 1
    assert record["cypher_count"] == 1


def test_run_initial_run_all_persists_failed_record_before_raise(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = FakeIPFS()
    captured_records: list[dict[str, Any]] = []

    db_file = tmp_path / "bad_step.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE movie (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        run_initial,
        "_collect_candidate_sources",
        lambda _db_folder: _make_sources({"BAD_STEP": str(db_file)}),
    )
    monkeypatch.setattr(run_initial, "DatabasePluginRegistry", lambda: FakeRegistry())
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
    monkeypatch.setattr(run_initial, "IPFSClient", lambda: fake_ipfs)
    monkeypatch.setattr(run_initial, "save_json", _make_save_json(tmp_path / "outputs"))
    monkeypatch.setattr(run_initial, "append_run_record", lambda record: captured_records.append(record))
    monkeypatch.setattr(
        run_initial,
        "get_all_fields",
        lambda _agent: (_ for _ in ()).throw(RuntimeError("sampling failed in run_initial")),
    )

    with pytest.raises(RuntimeError, match="sampling failed in run_initial"):
        run_initial.run_all()

    assert len(captured_records) == 1
    record = captured_records[0]
    assert record["status"] == "failed"
    assert "sampling failed in run_initial" in record["error"]

    manifest_files = list((tmp_path / "outputs").glob("run_manifest_*.json"))
    assert len(manifest_files) == 1


def test_run_initial_run_pipeline_calls_run_all(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"called": 0}

    def _fake_run_all() -> None:
        state["called"] += 1

    monkeypatch.setattr(run_initial, "run_all", _fake_run_all)
    run_initial.run_pipeline()

    assert state["called"] == 1


def test_run_initial_requires_string_auto_db_folder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_initial, "AUTO_PIPELINE_DEFAULTS", {"db_folder": 123})
    with pytest.raises(RuntimeError, match="AUTO_DB_FOLDER must be a non-empty string"):
        run_initial.run_all()


@pytest.mark.parametrize(
    ("payload", "error_pattern"),
    [
        ({"samples": "bad"}, "must be a list"),
        ([{"field": "id", "samples": [1]}], "missing non-empty table"),
        ([{"table": "movie", "samples": [1]}], "missing non-empty field"),
        (["bad"], "must be an object"),
    ],
)
def test_run_initial_coerce_sample_records_rejects_invalid_payload(
    payload: object,
    error_pattern: str,
) -> None:
    with pytest.raises(RuntimeError, match=error_pattern):
        run_initial._coerce_sample_records(payload)


def test_run_initial_coerce_sample_records_accepts_valid_artifact() -> None:
    payload = {
        "summary": {"db_name": "ONE"},
        "samples": [{"table": "movie", "field": "id", "samples": [1]}],
    }
    records = run_initial._coerce_sample_records(payload)

    assert len(records) == 1
    assert records[0]["table"] == "movie"
    assert records[0]["field"] == "id"


def test_run_auto_monitor_skips_invalid_source_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ipfs = FakeIPFS()
    state = {"idx": 0}
    snapshots: list[dict[str, DatabaseSource] | Exception] = [
        {},
        ValueError("bad json"),
        {},
    ]

    def fake_collect_sources(_db_folder: str) -> dict[str, DatabaseSource]:
        idx = state["idx"]
        state["idx"] += 1
        item = snapshots[min(idx, len(snapshots) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    sleep_calls = {"count": 0}

    def fake_sleep(_seconds: int) -> None:
        sleep_calls["count"] += 1
        if sleep_calls["count"] >= 2:
            raise RuntimeError("stop loop")

    monkeypatch.setattr(run_auto, "_collect_candidate_sources", fake_collect_sources)
    monkeypatch.setattr(run_auto, "_new_registry", lambda: object())
    monkeypatch.setattr("src.pipeline.run_auto.time.sleep", fake_sleep)
    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"poll_interval_sec": 0})

    with pytest.raises(RuntimeError, match="stop loop"):
        run_auto.monitor_and_process_new_database(cast(Any, ipfs), "data/dbs", "uf-cid")


def test_run_auto_requires_previous_unified_fields_cid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_auto, "AUTO_PIPELINE_DEFAULTS", {"previous_unified_fields_cid": "", "db_folder": "data/dbs"})
    monkeypatch.setattr(run_auto, "IPFSClient", lambda: object())

    with pytest.raises(RuntimeError, match="AUTO_PREVIOUS_UNIFIED_FIELDS_CID is empty"):
        run_auto.run_auto()


def test_run_auto_requires_string_db_folder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        run_auto,
        "AUTO_PIPELINE_DEFAULTS",
        {"previous_unified_fields_cid": "uf-cid", "db_folder": 1, "poll_interval_sec": 1},
    )
    monkeypatch.setattr(run_auto, "IPFSClient", lambda: object())

    with pytest.raises(RuntimeError, match="AUTO_DB_FOLDER must be a non-empty string"):
        run_auto.run_auto()


def test_run_auto_invokes_monitor_with_runtime_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_ipfs = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_auto,
        "AUTO_PIPELINE_DEFAULTS",
        {
            "previous_unified_fields_cid": "uf-cid",
            "db_folder": "data/dbs",
            "poll_interval_sec": 1,
        },
    )
    monkeypatch.setattr(run_auto, "IPFSClient", lambda: fake_ipfs)

    def fake_monitor(ipfs: Any, db_folder: str, previous_unified_fields_cid: str) -> None:
        captured["ipfs"] = ipfs
        captured["db_folder"] = db_folder
        captured["uf_cid"] = previous_unified_fields_cid

    monkeypatch.setattr(run_auto, "monitor_and_process_new_database", fake_monitor)
    run_auto.run_auto()

    assert captured["ipfs"] is fake_ipfs
    assert captured["db_folder"] == "data/dbs"
    assert captured["uf_cid"] == "uf-cid"


def test_run_domain_share_parse_args_clamps_timeout_and_max_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_domain_share.py",
            "--timeout",
            "1",
            "--max-fields-per-domain",
            "-2",
            "--domain",
            "IMDB",
            "--mock-llm",
            "--skip-chain",
        ],
    )
    cfg = domain_share.parse_args()

    assert cfg.timeout_sec == 3
    assert cfg.max_fields_per_domain == 0
    assert cfg.selected_domains == ["IMDB"]
    assert cfg.mock_llm is True
    assert cfg.skip_chain is True


@pytest.mark.parametrize(
    ("payload", "error_pattern"),
    [
        ([{"field": "id", "samples": [1]}], "missing non-empty table"),
        ([{"table": "movie", "samples": [1]}], "missing non-empty field"),
        ([{"table": " ", "field": "id", "samples": [1]}], "missing non-empty table"),
        ([{"table": "movie", "field": " ", "samples": [1]}], "missing non-empty field"),
    ],
)
def test_run_domain_share_coerce_sample_records_requires_table_and_field(
    payload: Any,
    error_pattern: str,
) -> None:
    with pytest.raises(RuntimeError, match=error_pattern):
        domain_share._coerce_sample_records(payload)


def test_run_domain_share_sample_fields_handles_quoted_table_name(
    tmp_path: Path,
) -> None:
    db_file = tmp_path / "quoted_table.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute('CREATE TABLE "movie""meta" ("title" TEXT)')
    conn.execute('INSERT INTO "movie""meta" ("title") VALUES (?)', ("A",))
    conn.commit()
    conn.close()

    agent = DatabaseAgent(str(db_file))
    try:
        samples = domain_share._sample_fields_for_domain(agent, max_fields=1)
    finally:
        agent.close()

    assert len(samples) == 1
    assert samples[0]["table"] == 'movie"meta'
    assert samples[0]["field"] == "title"
    assert isinstance(samples[0]["samples"], list)


def test_run_domain_share_put_file_on_chain_parsing_and_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "ipfs-chain.exe"
    fake_bin.write_text("bin", encoding="utf-8")

    class Proc:
        def __init__(self, returncode: int, stdout: str, stderr: str):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    monkeypatch.setattr(
        "src.pipeline.run_domain_share.subprocess.run",
        lambda *_args, **_kwargs: Proc(0, "  cid: cid-ok  \nTxHash : tx-ok\n", ""),
    )
    cid, tx_hash = domain_share._put_file_on_chain(
        ipfs_chain_bin=fake_bin,
        receiver="receiver",
        key="k1",
        file_path=fake_bin,
        rpc_addr="127.0.0.1:45558",
        ipfs_api="http://127.0.0.1:5001",
        timeout_sec=6,
    )
    assert cid == "cid-ok"
    assert tx_hash == "tx-ok"

    monkeypatch.setattr(
        "src.pipeline.run_domain_share.subprocess.run",
        lambda *_args, **_kwargs: Proc(0, "CID: cid-only\n", ""),
    )
    with pytest.raises(RuntimeError, match="failed to parse CID/TxHash"):
        domain_share._put_file_on_chain(
            ipfs_chain_bin=fake_bin,
            receiver="receiver",
            key="k2",
            file_path=fake_bin,
            rpc_addr="127.0.0.1:45558",
            ipfs_api="http://127.0.0.1:5001",
            timeout_sec=6,
        )

    def _raise_timeout(*_args: object, **_kwargs: object) -> Proc:
        raise std_subprocess.TimeoutExpired(cmd="ipfs-chain put", timeout=8)

    monkeypatch.setattr("src.pipeline.run_domain_share.subprocess.run", _raise_timeout)
    with pytest.raises(RuntimeError, match="ipfs-chain put timed out"):
        domain_share._put_file_on_chain(
            ipfs_chain_bin=fake_bin,
            receiver="receiver",
            key="k3",
            file_path=fake_bin,
            rpc_addr="127.0.0.1:45558",
            ipfs_api="http://127.0.0.1:5001",
            timeout_sec=6,
        )


def test_run_domain_share_ensure_ipfs_chain_binary_build_error_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    binary_path = tmp_path / "bin" / "ipfs-chain.exe"
    go_root = tmp_path / "go-norn"
    go_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(RuntimeError, match="provide --go-norn-root"):
        domain_share._ensure_ipfs_chain_binary(binary_path, None)

    with pytest.raises(RuntimeError, match="go-norn root is not a directory"):
        domain_share._ensure_ipfs_chain_binary(binary_path, tmp_path / "missing-root")

    def raise_file_not_found(*_args: object, **_kwargs: object) -> object:
        raise FileNotFoundError("go")

    monkeypatch.setattr("src.pipeline.run_domain_share.subprocess.run", raise_file_not_found)
    with pytest.raises(RuntimeError, match="go tool not found while building ipfs-chain"):
        domain_share._ensure_ipfs_chain_binary(binary_path, go_root)

    class Proc:
        def __init__(self, returncode: int, stdout: str, stderr: str):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    monkeypatch.setattr(
        "src.pipeline.run_domain_share.subprocess.run",
        lambda *_args, **_kwargs: Proc(0, "ok", ""),
    )
    with pytest.raises(RuntimeError, match="build reported success but binary is missing"):
        domain_share._ensure_ipfs_chain_binary(binary_path, go_root)

    def build_and_create(*_args: object, **_kwargs: object) -> Proc:
        binary_path.parent.mkdir(parents=True, exist_ok=True)
        binary_path.write_text("bin", encoding="utf-8")
        return Proc(0, "ok", "")

    monkeypatch.setattr("src.pipeline.run_domain_share.subprocess.run", build_and_create)
    domain_share._ensure_ipfs_chain_binary(binary_path, go_root)
    assert binary_path.is_file()


def test_helpers_for_chain_binary_and_put_parser(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    existing_bin = tmp_path / "already_exists.exe"
    existing_bin.write_text("x", encoding="utf-8")
    pipeline_run._ensure_ipfs_chain_binary(existing_bin, None)

    with pytest.raises(RuntimeError, match="provide GO_NORN_ROOT"):
        pipeline_run._ensure_ipfs_chain_binary(tmp_path / "missing.exe", None)
    with pytest.raises(RuntimeError, match="GO_NORN_ROOT is not a directory"):
        pipeline_run._ensure_ipfs_chain_binary(tmp_path / "missing.exe", tmp_path / "not-a-dir")

    class Proc:
        def __init__(self, returncode: int, stdout: str, stderr: str):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    monkeypatch.setattr(std_subprocess, "run", lambda *_args, **_kwargs: Proc(0, "  cid: cid-ok \nTxHash : tx-ok\n", ""))
    cid, tx_hash = pipeline_run._put_file_on_chain(
        ipfs_chain_bin=existing_bin,
        receiver="receiver",
        key="k",
        file_path=existing_bin,
        rpc_addr="rpc",
        ipfs_api="ipfs",
        timeout_sec=3,
    )
    assert cid == "cid-ok"
    assert tx_hash == "tx-ok"

    def _raise_timeout(*_args: object, **_kwargs: object) -> Proc:
        raise std_subprocess.TimeoutExpired(cmd="ipfs-chain put", timeout=5)

    monkeypatch.setattr(std_subprocess, "run", _raise_timeout)
    with pytest.raises(RuntimeError, match="ipfs-chain put timed out"):
        pipeline_run._put_file_on_chain(
            ipfs_chain_bin=existing_bin,
            receiver="receiver",
            key="k",
            file_path=existing_bin,
            rpc_addr="rpc",
            ipfs_api="ipfs",
            timeout_sec=3,
        )

    monkeypatch.setattr(std_subprocess, "run", lambda *_args, **_kwargs: Proc(1, "", "err"))
    with pytest.raises(RuntimeError, match="ipfs-chain put failed"):
        pipeline_run._put_file_on_chain(
            ipfs_chain_bin=existing_bin,
            receiver="receiver",
            key="k",
            file_path=existing_bin,
            rpc_addr="rpc",
            ipfs_api="ipfs",
            timeout_sec=3,
        )


def test_helpers_for_chain_binary_build_error_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    binary_path = tmp_path / "bin" / "ipfs-chain.exe"
    go_root = tmp_path / "go-root"
    go_root.mkdir(parents=True, exist_ok=True)

    class Proc:
        def __init__(self, returncode: int, stdout: str, stderr: str):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def _raise_file_not_found(*_args: object, **_kwargs: object) -> Proc:
        raise FileNotFoundError("go")

    monkeypatch.setattr(std_subprocess, "run", _raise_file_not_found)
    with pytest.raises(RuntimeError, match="go tool not found while building ipfs-chain"):
        pipeline_run._ensure_ipfs_chain_binary(binary_path, go_root)

    def _raise_timeout(*_args: object, **_kwargs: object) -> Proc:
        raise std_subprocess.TimeoutExpired(cmd="go build", timeout=180)

    monkeypatch.setattr(std_subprocess, "run", _raise_timeout)
    with pytest.raises(RuntimeError, match="building ipfs-chain timed out after 180s"):
        pipeline_run._ensure_ipfs_chain_binary(binary_path, go_root)

    monkeypatch.setattr(std_subprocess, "run", lambda *_args, **_kwargs: Proc(0, "ok", ""))
    with pytest.raises(RuntimeError, match="build reported success but binary is missing"):
        pipeline_run._ensure_ipfs_chain_binary(binary_path, go_root)

    def _build_and_create(*_args: object, **_kwargs: object) -> Proc:
        binary_path.parent.mkdir(parents=True, exist_ok=True)
        binary_path.write_text("bin", encoding="utf-8")
        return Proc(0, "ok", "")

    monkeypatch.setattr(std_subprocess, "run", _build_and_create)
    pipeline_run._ensure_ipfs_chain_binary(binary_path, go_root)
    assert binary_path.is_file()


def test_run_helpers_parse_domain_share_defaults_strictly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pipeline_run,
        "DOMAIN_SHARE_DEFAULTS",
        {
            "ipfs_chain_bin": "  bin/ipfs-chain.exe  ",
            "go_norn_root": "   ",
            "receiver": "  receiver  ",
            "rpc_addr": " 127.0.0.1:45558 ",
            "ipfs_api": " http://127.0.0.1:5001 ",
            "timeout_sec": "2",
        },
    )

    assert pipeline_run._domain_share_required_str("receiver") == "receiver"
    assert pipeline_run._domain_share_optional_str("go_norn_root") is None
    assert pipeline_run._domain_share_timeout_sec() == 3

    monkeypatch.setattr(
        pipeline_run,
        "DOMAIN_SHARE_DEFAULTS",
        {
            "ipfs_chain_bin": "bin/ipfs-chain.exe",
            "go_norn_root": 1,
            "receiver": "receiver",
            "rpc_addr": "127.0.0.1:45558",
            "ipfs_api": "http://127.0.0.1:5001",
            "timeout_sec": "bad",
        },
    )
    with pytest.raises(RuntimeError, match="must be a string when provided"):
        pipeline_run._domain_share_optional_str("go_norn_root")
    with pytest.raises(RuntimeError, match="must be an integer"):
        pipeline_run._domain_share_timeout_sec()

    monkeypatch.setattr(
        pipeline_run,
        "DOMAIN_SHARE_DEFAULTS",
        {
            "ipfs_chain_bin": "bin/ipfs-chain.exe",
            "go_norn_root": "",
            "receiver": 9,
            "rpc_addr": "127.0.0.1:45558",
            "ipfs_api": "http://127.0.0.1:5001",
            "timeout_sec": 6,
        },
    )
    with pytest.raises(RuntimeError, match="must be a non-empty string"):
        pipeline_run._domain_share_required_str("receiver")


def test_knowledge_graph_agent_generates_expected_sections() -> None:
    agent = kg_agent.KnowledgeGraphAgent()
    run_record = {
        "domains": [
            {
                "db_name": "IMDB",
                "sample_file": "samples_IMDB.json",
                "samples_cid": "cid1",
                "field_descriptions_file": "field_desc_IMDB.json",
                "field_descriptions_cid": "cid2",
                "domain_unified_file": "domain_unified_IMDB.json",
                "domain_unified_cid": "cid3",
            }
        ],
        "unified_fields_file": "unified_fields.json",
        "unified_fields_cid": "cid4",
        "cypher_file": "cypher.json",
        "cypher_cid": "cid5",
    }
    db_data = {"IMDB": {"movie": ["id", "title"]}}
    domain_field_desc_map = {
        "IMDB": [
            {"table": "movie", "field": "id", "description": "id desc"},
            {"table": "movie", "field": "title", "description": "title desc"},
        ]
    }
    domain_unified_map = {
        "IMDB": [
            {
                "canonical_name": "movie_id",
                "fields": ["IMDB.movie.id"],
                "description": "movie id",
            }
        ]
    }
    unified_fields = [
        {
            "canonical_name": "movie_title",
            "fields": ["IMDB.movie.title"],
            "description": "standard movie title",
        }
    ]

    cypher_list = agent.generate_cypher(
        run_record=run_record,
        db_data=db_data,
        domain_field_desc_map=domain_field_desc_map,
        domain_unified_map=domain_unified_map,
        unified_fields=unified_fields,
    )

    assert len(cypher_list) > 10
    joined = "\n".join(cypher_list)
    assert "DataSource" in joined
    assert "ResourceFile" in joined
    assert "StandardPropertyConcept" in joined
    assert "StandardEntityConcept" in joined
