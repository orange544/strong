from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.llm.description_agent as description_agent
import src.llm.semantic as semantic
import src.service.llm_service as llm_service


class _FakeMessage:
    def __init__(self, content: str):
        self.content = content


class _FakeChoice:
    def __init__(self, content: str):
        self.message = _FakeMessage(content)


class _FakeResponse:
    def __init__(self, content: str):
        self.choices = [_FakeChoice(content)]


class _FakeCompletions:
    def __init__(self, content: str):
        self._content = content
        self.last_kwargs: dict[str, Any] = {}

    def create(self, **kwargs: Any) -> _FakeResponse:
        self.last_kwargs = kwargs
        return _FakeResponse(self._content)


class _FakeChat:
    def __init__(self, content: str):
        self.completions = _FakeCompletions(content)


class _FakeOpenAI:
    def __init__(self, content: str, **_kwargs: Any):
        self.chat = _FakeChat(content)


def test_description_agent_generates_description_and_strips_think(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        description_agent,
        "OpenAI",
        lambda **kwargs: _FakeOpenAI("</think>release year field", **kwargs),
    )

    agent = description_agent.FieldDescriptionAgent(
        api_key="k",
        base_url="http://127.0.0.1:1234/v1",
        model_name="m",
    )
    result = agent.generate_description(
        {"table": "movie", "field": "year", "samples": [1999, 2001]}
    )

    assert result["table"] == "movie"
    assert result["field"] == "year"
    assert result["description"] == "release year field"


def test_description_agent_without_timeout_uses_plain_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_init: dict[str, object] = {}
    captured_request: dict[str, object] = {}

    class _NoTimeoutCompletions:
        def create(self, **kwargs: object) -> _FakeResponse:
            captured_request.update(kwargs)
            return _FakeResponse("plain description")

    class _NoTimeoutChat:
        def __init__(self) -> None:
            self.completions = _NoTimeoutCompletions()

    class _NoTimeoutOpenAI:
        def __init__(self, **kwargs: object) -> None:
            captured_init.update(kwargs)
            self.chat = _NoTimeoutChat()

    monkeypatch.setenv("LLM_REQUEST_TIMEOUT_SEC", "0")
    monkeypatch.setenv("LLM_MAX_RETRIES", "2")
    monkeypatch.setattr(description_agent, "OpenAI", _NoTimeoutOpenAI)

    agent = description_agent.FieldDescriptionAgent(
        api_key="k",
        base_url="http://127.0.0.1:1234/v1",
        model_name="m",
    )
    result = agent.generate_description(
        {"table": "movie", "field": "title", "samples": {"unexpected": "object"}}
    )

    assert result["description"] == "plain description"
    assert "timeout" not in captured_init
    assert "timeout" not in captured_request
    assert captured_init["max_retries"] == 2


def test_semantic_helpers_strip_and_parse() -> None:
    fenced = "```json\n[{\"canonical_name\":\"x\",\"fields\":[],\"description\":\"d\"}]\n```"
    stripped = semantic._strip_markdown_json_fence(fenced)
    parsed = semantic._parse_json_array(stripped)

    assert stripped.startswith("[")
    assert parsed[0]["canonical_name"] == "x"


def test_semantic_helpers_strip_fence_without_json_tag() -> None:
    fenced = "```\n[{\"canonical_name\":\"y\",\"fields\":[],\"description\":\"d\"}]\n```"
    stripped = semantic._strip_markdown_json_fence(fenced)
    parsed = semantic._parse_json_array(stripped)

    assert stripped.startswith("[")
    assert parsed[0]["canonical_name"] == "y"


def test_semantic_parse_json_array_rejects_invalid_payload() -> None:
    with pytest.raises(RuntimeError, match="must be a JSON array"):
        semantic._parse_json_array("{\"a\":1}")

    with pytest.raises(RuntimeError, match="must be an object"):
        semantic._parse_json_array("[1]")


def test_semantic_agent_unify_within_domain_calls_llm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        semantic,
        "OpenAI",
        lambda **kwargs: _FakeOpenAI(
            "[{\"canonical_name\":\"release_year\",\"fields\":[\"DB.movie.year\"],\"description\":\"year\"}]",
            **kwargs,
        ),
    )

    agent = semantic.FieldSemanticAgent(
        api_key="k",
        base_url="http://127.0.0.1:1234/v1",
        model_name="m",
    )
    result = agent.unify_within_domain(
        [{"db_name": "DB", "table": "movie", "field": "year", "description": "year"}]
    )

    assert len(result) == 1
    assert result[0]["canonical_name"] == "release_year"


def test_semantic_agent_unify_across_domains_calls_llm_with_cross_domain_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeOpenAI(
        "[{\"canonical_name\":\"title\",\"fields\":[\"A.movie.title\",\"B.movie.title\"],\"description\":\"title\"}]"
    )
    monkeypatch.setattr(semantic, "OpenAI", lambda **kwargs: fake_client)

    agent = semantic.FieldSemanticAgent(
        api_key="k",
        base_url="http://127.0.0.1:1234/v1",
        model_name="m",
    )
    result = agent.unify_across_domains(
        [
            {"db_name": "A", "canonical_name": "title", "fields": ["A.movie.title"], "description": "title"},
            {"db_name": "B", "canonical_name": "title", "fields": ["B.movie.title"], "description": "title"},
        ]
    )

    assert result[0]["canonical_name"] == "title"
    prompt = str(fake_client.chat.completions.last_kwargs["messages"][1]["content"])
    assert "across different databases" in prompt


def test_semantic_agent_wraps_empty_content_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(semantic, "OpenAI", lambda **kwargs: _FakeOpenAI(" ", **kwargs))
    agent = semantic.FieldSemanticAgent(
        api_key="k",
        base_url="http://127.0.0.1:1234/v1",
        model_name="m",
    )

    with pytest.raises(RuntimeError, match="LLM semantic unify failed: model returned empty content"):
        agent.unify_within_domain(
            [{"db_name": "DB", "table": "movie", "field": "year", "description": "year"}]
        )


def test_semantic_agent_wraps_parse_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(semantic, "OpenAI", lambda **kwargs: _FakeOpenAI("[1]", **kwargs))
    agent = semantic.FieldSemanticAgent(
        api_key="k",
        base_url="http://127.0.0.1:1234/v1",
        model_name="m",
    )

    with pytest.raises(RuntimeError, match="LLM semantic unify failed: LLM response item at index 0 must be an object"):
        agent.unify_within_domain(
            [{"db_name": "DB", "table": "movie", "field": "year", "description": "year"}]
        )


def test_semantic_agent_omits_timeout_when_timeout_config_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_init: dict[str, object] = {}
    captured_request: dict[str, object] = {}

    class _Completions:
        def create(self, **kwargs: object) -> _FakeResponse:
            captured_request.update(kwargs)
            return _FakeResponse(
                "[{\"canonical_name\":\"x\",\"fields\":[\"DB.movie.x\"],\"description\":\"x\"}]"
            )

    class _Chat:
        def __init__(self) -> None:
            self.completions = _Completions()

    class _OpenAIWithoutTimeout:
        def __init__(self, **kwargs: object) -> None:
            captured_init.update(kwargs)
            self.chat = _Chat()

    monkeypatch.setattr(semantic, "LLM_UNIFY_TIMEOUT_SEC", None)
    monkeypatch.setattr(semantic, "OpenAI", _OpenAIWithoutTimeout)

    agent = semantic.FieldSemanticAgent(
        api_key="k",
        base_url="http://127.0.0.1:1234/v1",
        model_name="m",
    )
    result = agent.unify_within_domain(
        [{"db_name": "DB", "table": "movie", "field": "x", "description": "x"}]
    )

    assert result[0]["canonical_name"] == "x"
    assert "timeout" not in captured_init
    assert "timeout" not in captured_request


def test_llm_service_pipeline_and_update(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeIPFS:
        def cat_json(self, _cid: str) -> object:
            return [{"table": "movie", "field": "title", "samples": ["A"]}]

    class FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            self.api_key = api_key
            self.base_url = base_url
            self.model_name = model_name

        def generate_description(self, item: dict[str, Any]) -> dict[str, str]:
            return {
                "table": str(item["table"]),
                "field": str(item["field"]),
                "description": "title text",
            }

    monkeypatch.setattr(llm_service, "FieldDescriptionAgent", FakeFieldDescriptionAgent)
    monkeypatch.setattr(llm_service, "save_json", lambda _data, _name: "ignored.json")

    descriptions = llm_service.run_llm_pipeline(
        ipfs=cast(Any, FakeIPFS()),
        samples_cid="cid",
        timestamp="20260320_000000",
        llm_config={},
    )
    assert descriptions[0]["description"] == "title text"

    merged = llm_service.update_unified_fields_with_new_descriptions(
        previous_unified_fields=[
            {"canonical_name": "title", "fields": ["OLD.movie.title"], "description": "title text"}
        ],
        new_field_descriptions=[{"field": "NEW.movie.title", "description": "title text"}],
    )
    assert merged[0]["fields"][-1] == "NEW.movie.title"


def test_llm_service_coerce_samples_rejects_invalid_payloads() -> None:
    with pytest.raises(RuntimeError, match="must be a list"):
        llm_service._coerce_samples({"bad": "shape"})

    with pytest.raises(RuntimeError, match="must be an object"):
        llm_service._coerce_samples([1])


def test_llm_service_pipeline_uses_default_config_when_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeIPFS:
        def cat_json(self, _cid: str) -> object:
            return [{"table": "movie", "field": "title", "samples": ["A"]}]

    class FakeFieldDescriptionAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str):
            assert api_key == ""
            assert base_url == ""
            assert model_name == ""

        def generate_description(self, item: dict[str, Any]) -> dict[str, str]:
            return {
                "table": str(item["table"]),
                "field": str(item["field"]),
                "description": "ok",
            }

    monkeypatch.setattr(llm_service, "FieldDescriptionAgent", FakeFieldDescriptionAgent)
    monkeypatch.setattr(llm_service, "save_json", lambda _data, _name: "ignored.json")

    descriptions = llm_service.run_llm_pipeline(
        ipfs=cast(Any, FakeIPFS()),
        samples_cid="cid",
    )
    assert descriptions[0]["description"] == "ok"


def test_llm_service_update_unified_fields_handles_non_list_fields_and_unmatched() -> None:
    merged = llm_service.update_unified_fields_with_new_descriptions(
        previous_unified_fields=[
            {"canonical_name": "x", "fields": "bad", "description": "same"},
        ],
        new_field_descriptions=[
            {"field": "NEW.movie.same", "description": "same"},
            {"field": "NEW.movie.other", "description": "other"},
        ],
    )

    assert isinstance(merged[1]["fields"], list)
    assert merged[0]["canonical_name"] == "NEW.movie.other"
