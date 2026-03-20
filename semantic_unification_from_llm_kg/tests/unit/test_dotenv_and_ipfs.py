from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.configs.dotenv_loader as dotenv_loader
import src.storage.ipfs_client as ipfs_client


def test_strip_quotes_handles_wrapped_and_plain_values() -> None:
    assert dotenv_loader._strip_quotes("'abc'") == "abc"
    assert dotenv_loader._strip_quotes('"xyz"') == "xyz"
    assert dotenv_loader._strip_quotes("plain") == "plain"


def test_load_dotenv_file_parses_lines_and_does_not_override_existing_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_path = tmp_path / ".env.test"
    env_path.write_text(
        "\n".join(
            [
                "# comment",
                "NO_EQUALS_LINE",
                "=value_should_be_ignored",
                "FOO='bar'",
                'export BAR="baz"',
                "EMPTY_KEY = value_should_be_ignored",
                "SPACED =   hello world   ",
                "EXISTING=from_file",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("EXISTING", "from_env")
    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.delenv("BAR", raising=False)
    monkeypatch.delenv("SPACED", raising=False)

    dotenv_loader.load_dotenv_file(env_path)

    assert os.environ["FOO"] == "bar"
    assert os.environ["BAR"] == "baz"
    assert os.environ["SPACED"] == "hello world"
    assert os.environ["EXISTING"] == "from_env"


def test_load_dotenv_file_ignores_missing_path(tmp_path: Path) -> None:
    missing = tmp_path / "missing.env"
    dotenv_loader.load_dotenv_file(missing)
    assert not missing.exists()


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        json_payload: dict[str, Any] | None = None,
        text_payload: str = "",
        content_payload: bytes = b"",
    ) -> None:
        self.status_code = status_code
        self._json_payload = json_payload or {}
        self.text = text_payload
        self.content = content_payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"status={self.status_code}")

    def json(self) -> dict[str, Any]:
        return self._json_payload


def test_ipfs_client_add_json_and_cat_json(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append((url, kwargs))
        if url.endswith("/add"):
            return _FakeResponse(json_payload={"Hash": "cid-add-json"})
        if url.endswith("/cat"):
            return _FakeResponse(text_payload=json.dumps({"x": 1, "y": "z"}))
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(ipfs_client.requests, "post", fake_post)

    client = ipfs_client.IPFSClient(api_url="http://127.0.0.1:5001/api/v0/")
    cid = client.add_json({"hello": "world"})
    payload = client.cat_json("cid-add-json")

    assert cid == "cid-add-json"
    assert payload == {"x": 1, "y": "z"}
    assert calls[0][0].endswith("/add")
    assert calls[1][0].endswith("/cat")
    assert calls[1][1]["params"] == {"arg": "cid-add-json"}


def test_ipfs_client_add_file_and_cat_raw(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    test_file = tmp_path / "artifact.json"
    test_file.write_text('{"a":1}', encoding="utf-8")

    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        calls.append((url, kwargs))
        if url.endswith("/add"):
            return _FakeResponse(json_payload={"Hash": "cid-add-file"})
        if url.endswith("/cat"):
            return _FakeResponse(content_payload=b"raw-binary")
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(ipfs_client.requests, "post", fake_post)

    client = ipfs_client.IPFSClient(api_url="http://127.0.0.1:5001/api/v0", timeout=5)
    cid = client.add_file(str(test_file))
    raw = client.cat_raw("cid-add-file")

    assert cid == "cid-add-file"
    assert raw == b"raw-binary"
    assert calls[0][1]["timeout"] == 5
    assert calls[1][1]["params"] == {"arg": "cid-add-file"}


def test_ipfs_client_add_json_rejects_missing_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_post(url: str, **kwargs: Any) -> _FakeResponse:
        del kwargs
        if url.endswith("/add"):
            return _FakeResponse(json_payload={})
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(ipfs_client.requests, "post", fake_post)

    client = ipfs_client.IPFSClient(api_url="http://127.0.0.1:5001/api/v0")
    with pytest.raises(RuntimeError, match="missing non-empty 'Hash'"):
        client.add_json({"hello": "world"})


def test_extract_cid_rejects_non_object_payload() -> None:
    with pytest.raises(RuntimeError, match="must be a JSON object"):
        ipfs_client._extract_cid("bad")
