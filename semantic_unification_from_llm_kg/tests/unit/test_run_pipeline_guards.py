from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.pipeline.run as pipeline_run
from src.db.plugin_registry import DatabasePluginRegistry, DatabaseSource
from src.db.unified.field_unit import FieldUnit


def test_domain_share_optional_str_and_timeout_parsers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_run, "DOMAIN_SHARE_DEFAULTS", {"go_norn_root": None})
    assert pipeline_run._domain_share_optional_str("go_norn_root") is None

    monkeypatch.setattr(pipeline_run, "DOMAIN_SHARE_DEFAULTS", {"go_norn_root": "  /tmp/go  "})
    assert pipeline_run._domain_share_optional_str("go_norn_root") == "/tmp/go"

    monkeypatch.setattr(pipeline_run, "DOMAIN_SHARE_DEFAULTS", {"timeout_sec": None})
    with pytest.raises(RuntimeError, match="must be an integer"):
        pipeline_run._domain_share_timeout_sec()


def test_ensure_ipfs_chain_binary_rejects_invalid_path_and_build_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_dir = tmp_path / "ipfs-chain"
    as_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(RuntimeError, match="is not a file"):
        pipeline_run._ensure_ipfs_chain_binary(as_dir, tmp_path)

    binary_path = tmp_path / "bin" / "ipfs-chain"
    go_root = tmp_path / "go-root"
    go_root.mkdir(parents=True, exist_ok=True)

    class _ProcResult:
        returncode = 1
        stdout = "out"
        stderr = "err"

    monkeypatch.setattr(
        "src.pipeline.run.subprocess.run",
        lambda *_args, **_kwargs: _ProcResult(),
    )
    with pytest.raises(RuntimeError, match="failed to build ipfs-chain"):
        pipeline_run._ensure_ipfs_chain_binary(binary_path, go_root)


def test_put_file_on_chain_rejects_unparseable_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ProcResult:
        returncode = 0
        stdout = "missing expected keys"
        stderr = ""

    monkeypatch.setattr(
        "src.pipeline.run.subprocess.run",
        lambda *_args, **_kwargs: _ProcResult(),
    )
    with pytest.raises(RuntimeError, match="failed to parse CID/TxHash"):
        pipeline_run._put_file_on_chain(
            ipfs_chain_bin=tmp_path / "ipfs-chain",
            receiver="receiver",
            key="REGISTER:K",
            file_path=tmp_path / "artifact.json",
            rpc_addr="rpc",
            ipfs_api="ipfs",
            timeout_sec=3,
        )


def test_load_runtime_db_sources_rejects_invalid_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pipeline_run,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: cast(Any, []),
    )
    with pytest.raises(RuntimeError, match="must return a source map"):
        pipeline_run._load_runtime_db_sources()

    monkeypatch.setattr(
        pipeline_run,
        "load_db_sources_from_env",
        lambda *, legacy_db_paths: {" ": DatabaseSource(name="A", driver="sqlite", dsn="a.db", options={})},
    )
    with pytest.raises(RuntimeError, match="name must be a non-empty string"):
        pipeline_run._load_runtime_db_sources()


def test_new_registry_returns_registry() -> None:
    registry = pipeline_run._new_registry()
    assert isinstance(registry, DatabasePluginRegistry)


def test_create_db_agents_wraps_unsupported_driver_and_closes_created_agents() -> None:
    class _FakeAgent:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    created_agents: list[_FakeAgent] = []

    class _FakeRegistry:
        def create_agent(self, source: DatabaseSource) -> _FakeAgent:
            if source.name == "BAD":
                raise KeyError("unsupported")
            agent = _FakeAgent()
            created_agents.append(agent)
            return agent

        def supported_drivers(self) -> tuple[str, ...]:
            return ("sqlite",)

    with pytest.raises(RuntimeError, match="Unsupported database driver"):
        pipeline_run._create_db_agents(
            {
                "GOOD": DatabaseSource(name="GOOD", driver="sqlite", dsn="good.db", options={}),
                "BAD": DatabaseSource(name="BAD", driver="postgres", dsn="pg://db", options={}),
            },
            cast(Any, _FakeRegistry()),
        )

    assert created_agents and created_agents[0].closed is True


def test_run_all_rejects_empty_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_run, "_load_runtime_db_sources", lambda: {})
    with pytest.raises(RuntimeError, match="No database sources configured"):
        pipeline_run.run_all()


def test_run_all_applies_max_fields_per_domain_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeIPFS:
        pass

    monkeypatch.setattr(
        pipeline_run,
        "_load_runtime_db_sources",
        lambda: {
            "DB": DatabaseSource(name="DB", driver="sqlite", dsn="db.sqlite", options={}),
        },
    )
    monkeypatch.setattr(pipeline_run, "IPFSClient", _FakeIPFS)
    monkeypatch.setattr(
        pipeline_run,
        "DOMAIN_SHARE_DEFAULTS",
        {
            "ipfs_chain_bin": "bin/ipfs-chain",
            "go_norn_root": "go-norn",
            "receiver": "receiver",
            "rpc_addr": "rpc",
            "ipfs_api": "ipfs",
            "timeout_sec": 3,
        },
    )
    monkeypatch.setattr(
        pipeline_run,
        "PIPELINE_CONFIG",
        {
            "llm_desc_max_workers": 1,
            "llm_desc_domain_timeout_sec": 1,
            "run_max_fields_per_domain": 1,
        },
    )
    monkeypatch.setattr(pipeline_run, "_ensure_ipfs_chain_binary", lambda *_args, **_kwargs: None)
    captured_max_fields: dict[str, int] = {}
    monkeypatch.setattr(
        pipeline_run,
        "extract_field_units_by_source",
        lambda _sources, *, max_fields_per_domain=0: (
            captured_max_fields.__setitem__("value", max_fields_per_domain),
            {
                "DB": [
                    FieldUnit(
                        source_name="DB",
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

    def _stop_after_cap(db_name: str, timestamp: str, samples: list[dict[str, Any]]) -> dict[str, Any]:
        assert db_name == "DB"
        assert isinstance(timestamp, str)
        assert len(samples) == 1
        raise StopIteration("stop-after-cap")

    monkeypatch.setattr(pipeline_run, "_build_sample_artifact", _stop_after_cap)

    with pytest.raises(StopIteration, match="stop-after-cap"):
        pipeline_run.run_all()

    assert captured_max_fields["value"] == 1


def test_run_pipeline_delegates_to_run_all(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"value": False}

    def _fake_run_all() -> None:
        called["value"] = True

    monkeypatch.setattr(pipeline_run, "run_all", _fake_run_all)
    pipeline_run.run_pipeline()
    assert called["value"] is True
