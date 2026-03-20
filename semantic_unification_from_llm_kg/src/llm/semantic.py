from __future__ import annotations

import json
from typing import Any

from openai import OpenAI

from src.configs.config import (
    LLM_MAX_RETRIES,
    LLM_UNIFY_MAX_TOKENS,
    LLM_UNIFY_TIMEOUT_SEC,
)


def _strip_markdown_json_fence(content: str) -> str:
    text = content.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
    return text.strip()


def _parse_json_array(content: str) -> list[dict[str, Any]]:
    parsed = json.loads(_strip_markdown_json_fence(content))
    if not isinstance(parsed, list):
        raise RuntimeError("LLM response must be a JSON array")

    result: list[dict[str, Any]] = []
    for index, item in enumerate(parsed):
        if not isinstance(item, dict):
            raise RuntimeError(f"LLM response item at index {index} must be an object")
        result.append(item)
    return result


class FieldSemanticAgent:
    def __init__(self, api_key: str, base_url: str, model_name: str):
        self.request_timeout = LLM_UNIFY_TIMEOUT_SEC
        self.max_retries = LLM_MAX_RETRIES

        client_kwargs: dict[str, Any] = {
            "api_key": api_key,
            "base_url": base_url,
            "max_retries": self.max_retries,
        }
        if self.request_timeout is not None:
            client_kwargs["timeout"] = self.request_timeout

        self.client = OpenAI(**client_kwargs)
        self.model_name = model_name

    def _call_llm(self, prompt: str) -> list[dict[str, Any]]:
        try:
            request_kwargs: dict[str, Any] = {
                "model": self.model_name,
                "messages": [
                    {"role": "system", "content": "You are a structured semantic unification assistant."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
                "max_tokens": LLM_UNIFY_MAX_TOKENS,
            }
            if self.request_timeout is not None:
                request_kwargs["timeout"] = self.request_timeout

            response = self.client.chat.completions.create(**request_kwargs)
            content_obj = response.choices[0].message.content
            content = content_obj.strip() if isinstance(content_obj, str) else ""
            if not content:
                raise RuntimeError("model returned empty content")

            return _parse_json_array(content)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"LLM semantic unify failed: {exc}") from exc

    def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
        json_input = json.dumps(field_desc_list, ensure_ascii=False, indent=2)
        prompt = (
            "Group semantically similar fields within the same database across different tables. "
            "Each group can contain at most two fields.\n"
            "Output only a JSON array with objects:\n"
            "{\"canonical_name\": str, \"fields\": [str, ...], \"description\": str}\n"
            f"input:\n{json_input}"
        )
        return self._call_llm(prompt)

    def unify_across_domains(self, domain_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        json_input = json.dumps(domain_items, ensure_ascii=False, indent=2)
        prompt = (
            "Group semantically similar field units across different databases. "
            "Items from the same database should not be grouped together. "
            "Each group can contain at most two items.\n"
            "Output only a JSON array with objects:\n"
            "{\"canonical_name\": str, \"fields\": [str, ...], \"description\": str}\n"
            f"input:\n{json_input}"
        )
        return self._call_llm(prompt)
