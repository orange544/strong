from __future__ import annotations

import os
from typing import Any

from openai import OpenAI


class FieldDescriptionAgent:
    def __init__(self, api_key: str, base_url: str, model_name: str):
        raw_timeout = float(os.getenv("LLM_REQUEST_TIMEOUT_SEC", "45"))
        self.request_timeout = None if raw_timeout <= 0 else raw_timeout
        self.max_retries = int(os.getenv("LLM_MAX_RETRIES", "1"))

        client_kwargs: dict[str, Any] = {
            "api_key": api_key,
            "base_url": base_url,
            "max_retries": self.max_retries,
        }
        if self.request_timeout is not None:
            client_kwargs["timeout"] = self.request_timeout

        self.client = OpenAI(**client_kwargs)
        self.model_name = model_name

    def generate_description(self, field_json: dict[str, Any]) -> dict[str, str]:
        table = str(field_json.get("table", ""))
        field = str(field_json.get("field", ""))
        samples_obj = field_json.get("samples", [])
        samples = ", ".join(str(item) for item in samples_obj) if isinstance(samples_obj, list) else ""

        prompt = (
            "You are a database field semantic assistant. "
            "Given a field name and sample values, produce one concise English description.\n"
            f"field: {field}\n"
            f"samples: {samples}"
        )

        request_kwargs: dict[str, Any] = {
            "model": self.model_name,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Return only the final one-sentence English description. "
                        "Do not include reasoning."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
        }
        if self.request_timeout is not None:
            request_kwargs["timeout"] = self.request_timeout

        response = self.client.chat.completions.create(**request_kwargs)
        content_obj = response.choices[0].message.content
        raw_text = content_obj if isinstance(content_obj, str) else ""

        if "</think>" in raw_text:
            description = raw_text.split("</think>", 1)[1].strip()
        else:
            description = raw_text.strip()

        return {
            "table": table,
            "field": field,
            "description": description,
        }
