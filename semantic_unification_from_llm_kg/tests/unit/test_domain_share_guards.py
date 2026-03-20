from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.pipeline.run_domain_share as domain_share
from src.db.plugin_registry import DatabaseSource
from src.pipeline.orchestration_common import DESCRIPTION_FAILED


def _cfg(
    tmp_path: Path,
    *,
    selected_domains: list[str] | None = None,
    strict: bool = False,
    skip_chain: bool = True,
    mock_llm: bool = True,
) -> domain_share.DomainShareConfig:
    return domain_share.DomainShareConfig(
        ipfs_chain_bin=tmp_path / "ipfs-chain",
        go_norn_root=None,
        receiver="receiver",
        rpc_addr="http://127.0.0.1:8545",
        ipfs_api="http://127.0.0.1:5001",
        timeout_sec=5,
        strict=strict,
        skip_chain=skip_chain,
        selected_domains=selected_domains or [],
        max_fields_per_domain=0,
        mock_llm=mock_llm,
    )


def test_slugify_hashes_when_safe_tag_is_domain() -> None:
    slug = domain_share._slugify("!!!")
    assert slug.startswith("domain_")
    assert len(slug) == len("domain_") + 8


def test_resolve_sqlite_path_converts_relative_path_to_absolute() -> None:
    resolved = domain_share._resolve_sqlite_path("data/dbs/test.db")
    assert resolved.is_absolute()


def test_create_agent_for_source_validates_dsn_and_wraps_unsupported_driver(
    tmp_path: Path,
) -> None:
    class _FakeRegistry:
        def create_agent(self, _source: DatabaseSource) -> object:
            raise KeyError("bad_driver")

        def supported_drivers(self) -> tuple[str, ...]:
            return ("sqlite",)

    with pytest.raises(RuntimeError, match="empty DSN"):
        domain_share._create_agent_for_source(
            cast(Any, _FakeRegistry()),
            DatabaseSource(name="X", driver="sqlite", dsn=" ", options={}),
        )

    with pytest.raises(RuntimeError, match="missing sqlite file"):
        domain_share._create_agent_for_source(
            cast(Any, _FakeRegistry()),
            DatabaseSource(
                name="MISSING",
                driver="sqlite",
                dsn=str(tmp_path / "missing.db"),
                options={},
            ),
        )

    with pytest.raises(RuntimeError, match="Unsupported database driver"):
        domain_share._create_agent_for_source(
            cast(Any, _FakeRegistry()),
            DatabaseSource(name="PG", driver="postgres", dsn="postgres://x", options={}),
        )


def test_sample_fields_for_domain_fallback_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        domain_share,
        "get_all_fields",
        lambda _agent: [
            {"table": "movie", "field": "id", "samples": [1]},
            {"table": "movie", "field": "title", "samples": ["A"]},
        ],
    )

    assert len(domain_share._sample_fields_for_domain(cast(Any, object()), 0)) == 2
    assert len(domain_share._sample_fields_for_domain(cast(Any, object()), 1)) == 1

    class _BrokenConn:
        def cursor(self) -> object:
            raise RuntimeError("boom")

    class _BrokenAgent:
        conn = _BrokenConn()

        def sample_field(self, _table: str, _field: str) -> dict[str, Any]:
            return {"samples": [1]}

    assert len(domain_share._sample_fields_for_domain(cast(Any, _BrokenAgent()), 1)) == 1


def test_sample_fields_for_domain_validates_sample_shape_and_limits() -> None:
    class _Cursor:
        def __init__(self) -> None:
            self._query = ""

        def execute(self, query: str) -> None:
            self._query = query

        def fetchall(self) -> list[tuple[object, ...]]:
            if "sqlite_master" in self._query:
                return [("",), ("movie",)]
            if "PRAGMA table_info" in self._query:
                return [(0, "id"), (1, "title")]
            return []

    class _Conn:
        def __init__(self) -> None:
            self._cursor = _Cursor()

        def cursor(self) -> _Cursor:
            return self._cursor

    class _BadSampleAgent:
        def __init__(self, sample_value: object) -> None:
            self.conn = _Conn()
            self._sample_value = sample_value

        def sample_field(self, _table: str, _field: str) -> object:
            return self._sample_value

    with pytest.raises(RuntimeError, match="must return an object"):
        domain_share._sample_fields_for_domain(cast(Any, _BadSampleAgent(1)), 2)

    with pytest.raises(RuntimeError, match="returned non-list 'samples'"):
        domain_share._sample_fields_for_domain(
            cast(Any, _BadSampleAgent({"samples": "bad"})),
            2,
        )

    class _GoodAgent:
        def __init__(self) -> None:
            self.conn = _Conn()

        def sample_field(self, table: str, field: str) -> dict[str, Any]:
            return {"table": table, "field": field, "samples": [f"{table}.{field}"]}

    limited = domain_share._sample_fields_for_domain(cast(Any, _GoodAgent()), 1)
    assert len(limited) == 1
    assert limited[0]["table"] == "movie"


def test_sample_fields_for_domain_returns_at_loop_end_when_no_nonempty_samples() -> None:
    class _Cursor:
        def __init__(self) -> None:
            self._query = ""

        def execute(self, query: str) -> None:
            self._query = query

        def fetchall(self) -> list[tuple[object, ...]]:
            if "sqlite_master" in self._query:
                return [("movie",), ("series",)]
            if "PRAGMA table_info" in self._query:
                return [(0, "id")]
            return []

    class _Conn:
        def __init__(self) -> None:
            self._cursor = _Cursor()

        def cursor(self) -> _Cursor:
            return self._cursor

    class _Agent:
        def __init__(self) -> None:
            self.conn = _Conn()

        def sample_field(self, table: str, field: str) -> dict[str, Any]:
            return {"table": table, "field": field, "samples": []}

    assert domain_share._sample_fields_for_domain(cast(Any, _Agent()), 2) == []


def test_ensure_ipfs_chain_binary_validates_path_arguments(
    tmp_path: Path,
) -> None:
    as_dir = tmp_path / "ipfs-chain"
    as_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(RuntimeError, match="is not a file"):
        domain_share._ensure_ipfs_chain_binary(as_dir, None)

    missing_bin = tmp_path / "missing-bin"
    with pytest.raises(RuntimeError, match="provide --go-norn-root"):
        domain_share._ensure_ipfs_chain_binary(missing_bin, None)

    fake_go_root = tmp_path / "go-root.txt"
    fake_go_root.write_text("x", encoding="utf-8")
    with pytest.raises(RuntimeError, match="not a directory"):
        domain_share._ensure_ipfs_chain_binary(missing_bin, fake_go_root)


def test_ensure_ipfs_chain_binary_returns_when_binary_exists_file(tmp_path: Path) -> None:
    binary_path = tmp_path / "ipfs-chain"
    binary_path.write_text("bin", encoding="utf-8")
    domain_share._ensure_ipfs_chain_binary(binary_path, None)


def test_ensure_ipfs_chain_binary_handles_build_timeout_and_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binary_path = tmp_path / "bin" / "ipfs-chain"
    go_root = tmp_path / "go-root"
    go_root.mkdir(parents=True, exist_ok=True)

    def _raise_timeout(*_args: object, **_kwargs: Any) -> object:
        raise subprocess.TimeoutExpired(cmd=["go", "build"], timeout=180)

    monkeypatch.setattr("src.pipeline.run_domain_share.subprocess.run", _raise_timeout)
    with pytest.raises(RuntimeError, match="timed out after 180s"):
        domain_share._ensure_ipfs_chain_binary(binary_path, go_root)

    class _ProcResult:
        returncode = 1
        stdout = "out"
        stderr = "err"

    monkeypatch.setattr(
        "src.pipeline.run_domain_share.subprocess.run",
        lambda *_args, **_kwargs: _ProcResult(),
    )
    with pytest.raises(RuntimeError, match="failed to build ipfs-chain"):
        domain_share._ensure_ipfs_chain_binary(binary_path, go_root)


def test_put_file_on_chain_rejects_nonzero_exit_and_unparseable_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ProcResult:
        def __init__(self, *, returncode: int, stdout: str, stderr: str) -> None:
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    monkeypatch.setattr(
        "src.pipeline.run_domain_share.subprocess.run",
        lambda *_args, **_kwargs: _ProcResult(returncode=1, stdout="bad", stderr="boom"),
    )
    with pytest.raises(RuntimeError, match="ipfs-chain put failed"):
        domain_share._put_file_on_chain(
            ipfs_chain_bin=tmp_path / "ipfs-chain",
            receiver="r",
            key="k",
            file_path=tmp_path / "data.json",
            rpc_addr="rpc",
            ipfs_api="ipfs",
            timeout_sec=3,
        )

    monkeypatch.setattr(
        "src.pipeline.run_domain_share.subprocess.run",
        lambda *_args, **_kwargs: _ProcResult(returncode=0, stdout="no cid lines", stderr=""),
    )
    with pytest.raises(RuntimeError, match="failed to parse CID/TxHash"):
        domain_share._put_file_on_chain(
            ipfs_chain_bin=tmp_path / "ipfs-chain",
            receiver="r",
            key="k",
            file_path=tmp_path / "data.json",
            rpc_addr="rpc",
            ipfs_api="ipfs",
            timeout_sec=3,
        )


def test_make_ipfs_client_appends_api_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    class _FakeIPFSClient:
        def __init__(self, api_url: str) -> None:
            captured["api_url"] = api_url

    monkeypatch.setattr(domain_share, "IPFSClient", _FakeIPFSClient)
    domain_share._make_ipfs_client("http://127.0.0.1:5001")
    assert captured["api_url"] == "http://127.0.0.1:5001/api/v0"


def test_make_ipfs_client_keeps_existing_api_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    class _FakeIPFSClient:
        def __init__(self, api_url: str) -> None:
            captured["api_url"] = api_url

    monkeypatch.setattr(domain_share, "IPFSClient", _FakeIPFSClient)
    domain_share._make_ipfs_client("http://127.0.0.1:5001/api/v0")
    assert captured["api_url"] == "http://127.0.0.1:5001/api/v0"


def test_coerce_sample_records_rejects_invalid_rows() -> None:
    with pytest.raises(RuntimeError, match="must be a list"):
        domain_share._coerce_sample_records({"samples": "bad"})

    with pytest.raises(RuntimeError, match="must be an object"):
        domain_share._coerce_sample_records([1])

    with pytest.raises(RuntimeError, match="missing non-empty table"):
        domain_share._coerce_sample_records([{"field": "id"}])

    with pytest.raises(RuntimeError, match="missing non-empty field"):
        domain_share._coerce_sample_records([{"table": "movie"}])


def test_generate_domain_descriptions_handles_missing_fd_agent_via_failed_marker() -> None:
    result = domain_share._generate_domain_descriptions(
        sample_payload=[{"table": "movie", "field": "id", "samples": [1]}],
        mock_llm=False,
        fd_agent=None,
    )
    assert result[0]["description"] == DESCRIPTION_FAILED


def test_run_domain_share_rejects_empty_sources_and_unmatched_domain_filter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(domain_share, "_load_runtime_db_sources", lambda: {})
    with pytest.raises(RuntimeError, match="No database sources configured"):
        domain_share.run_domain_share(_cfg(tmp_path))

    monkeypatch.setattr(
        domain_share,
        "_load_runtime_db_sources",
        lambda: {
            "IMDB": DatabaseSource(name="IMDB", driver="sqlite", dsn="data/dbs/IMDB.db", options={})
        },
    )
    with pytest.raises(RuntimeError, match="No domains matched --domain values"):
        domain_share.run_domain_share(_cfg(tmp_path, selected_domains=["TMDB"]))


def test_main_prints_json_result(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(domain_share, "parse_args", lambda: _cfg(tmp_path))
    monkeypatch.setattr(domain_share, "run_domain_share", lambda _cfg_obj: {"ok": 1})

    domain_share.main()
    output = capsys.readouterr().out
    assert "\"ok\": 1" in output
