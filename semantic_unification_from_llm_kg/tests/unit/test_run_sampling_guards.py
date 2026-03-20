from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.pipeline.run_sampling as run_sampling
from src.db.plugin_registry import DatabaseSource


class _FakeAgent:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeRegistry:
    def create_agent(self, source: DatabaseSource) -> _FakeAgent:
        del source
        return _FakeAgent()

    def supported_drivers(self) -> tuple[str, ...]:
        return ("sqlite",)


class _FakeIPFS:
    def __init__(self) -> None:
        self.uploaded: list[object] = []

    def add_json(self, obj: object) -> str:
        self.uploaded.append(obj)
        return "samples-cid"


def test_run_sampling_load_runtime_sources_rejects_invalid_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_sampling,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: cast(Any, []),
    )
    with pytest.raises(RuntimeError, match="must return a source map"):
        run_sampling._load_runtime_db_sources()

    monkeypatch.setattr(
        run_sampling,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {
            "  ": DatabaseSource(name="X", driver="sqlite", dsn="x.db", options={})
        },
    )
    with pytest.raises(RuntimeError, match="name must be a non-empty string"):
        run_sampling._load_runtime_db_sources()

    monkeypatch.setattr(
        run_sampling,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {"ONE": cast(Any, object())},
    )
    with pytest.raises(RuntimeError, match="invalid source object"):
        run_sampling._load_runtime_db_sources()


def test_run_sampling_only_raises_when_no_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(run_sampling, "_load_runtime_db_sources", lambda: {})
    with pytest.raises(RuntimeError, match="No database sources configured"):
        run_sampling.run_sampling_only(upload_to_ipfs=False, timestamp="20260320_000010")


def test_run_sampling_only_uploads_to_ipfs_when_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_file = tmp_path / "one.db"
    conn = sqlite3.connect(str(db_file))
    conn.execute("CREATE TABLE movie (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    source = DatabaseSource(name="ONE", driver="sqlite", dsn=str(db_file), options={})
    fake_ipfs = _FakeIPFS()
    created_agents: list[_FakeAgent] = []

    class _LocalRegistry:
        def create_agent(self, source_obj: DatabaseSource) -> _FakeAgent:
            del source_obj
            agent = _FakeAgent()
            created_agents.append(agent)
            return agent

        def supported_drivers(self) -> tuple[str, ...]:
            return ("sqlite",)

    monkeypatch.setattr(run_sampling, "_load_runtime_db_sources", lambda: {"ONE": source})
    monkeypatch.setattr(run_sampling, "DatabasePluginRegistry", lambda: cast(Any, _LocalRegistry()))
    monkeypatch.setattr(
        run_sampling,
        "get_all_fields",
        lambda _agent: [{"table": "movie", "field": "id", "samples": [1]}],
    )
    monkeypatch.setattr(run_sampling, "save_json", lambda _data, _name: str(tmp_path / "samples.json"))
    monkeypatch.setattr(run_sampling, "IPFSClient", lambda: cast(Any, fake_ipfs))

    result = run_sampling.run_sampling_only(upload_to_ipfs=True, timestamp="20260320_000011")

    assert result["samples_cid"] == "samples-cid"
    assert isinstance(result["databases"], dict)
    assert cast(dict[str, int], result["databases"])["ONE"] == 1
    assert len(fake_ipfs.uploaded) == 1
    assert created_agents[0].closed is True
