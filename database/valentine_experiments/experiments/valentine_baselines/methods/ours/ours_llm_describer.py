from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from dataclasses import dataclass
from typing import Any

DESCRIPTION_FAILED = "generation_failed"


@dataclass(frozen=True)
class LLMRuntimeConfig:
    api_key: str
    base_url: str
    model_name: str
    temperature: float
    timeout_sec: float | None
    max_retries: int
    max_workers: int
    domain_timeout_sec: int


class LLMColumnDescriptionAgent:
    def __init__(self, config: LLMRuntimeConfig):
        # Delay import so non-LLM mode does not require openai package.
        from openai import OpenAI

        client_kwargs: dict[str, Any] = {
            "api_key": config.api_key,
            "base_url": config.base_url,
            "max_retries": max(0, config.max_retries),
        }
        if config.timeout_sec is not None and config.timeout_sec > 0:
            client_kwargs["timeout"] = config.timeout_sec

        self.client = OpenAI(**client_kwargs)
        self.config = config

    @staticmethod
    def _clean_model_text(raw_text: str) -> str:
        text = raw_text.strip()
        if "</think>" in text:
            text = text.split("</think>", 1)[1].strip()
        return text

    def generate_description(self, sample_row: dict[str, Any]) -> dict[str, Any]:
        table_name = str(sample_row.get("table_name", "")).strip()
        column_name = str(sample_row.get("column_name", "")).strip()
        sample_values = [
            str(item).strip()
            for item in sample_row.get("sample_values", [])
            if str(item).strip()
        ]
        sample_text = ", ".join(sample_values)

        prompt = (
            "You are a database field semantic assistant. "
            "Given table name, field name, and sample values, "
            "produce one concise English description.\n"
            f"table: {table_name}\n"
            f"field: {column_name}\n"
            f"samples: {sample_text}"
        )

        request_kwargs: dict[str, Any] = {
            "model": self.config.model_name,
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
            "temperature": self.config.temperature,
        }
        if self.config.timeout_sec is not None and self.config.timeout_sec > 0:
            request_kwargs["timeout"] = self.config.timeout_sec

        response = self.client.chat.completions.create(**request_kwargs)
        content_obj = response.choices[0].message.content
        raw_text = content_obj if isinstance(content_obj, str) else ""
        description = self._clean_model_text(raw_text)

        return {
            "table_name": table_name,
            "column_name": column_name,
            "description": description,
            "llm_model": self.config.model_name,
        }


def describe_sampled_columns_with_llm(
    *,
    sampled_columns: list[dict[str, Any]],
    agent: LLMColumnDescriptionAgent,
    max_workers: int,
    domain_timeout_sec: int,
) -> list[dict[str, Any]]:
    if not sampled_columns:
        return []

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        future_to_sample = {
            executor.submit(agent.generate_description, sample): sample
            for sample in sampled_columns
        }
        try:
            for future in as_completed(future_to_sample, timeout=max(1, domain_timeout_sec)):
                sample = future_to_sample[future]
                try:
                    results.append(future.result())
                except Exception:  # noqa: BLE001
                    results.append(
                        {
                            "table_name": str(sample.get("table_name", "")),
                            "column_name": str(sample.get("column_name", "")),
                            "description": DESCRIPTION_FAILED,
                            "llm_model": agent.config.model_name,
                        }
                    )
        except TimeoutError:
            for future, sample in future_to_sample.items():
                if future.done():
                    continue
                future.cancel()
                results.append(
                    {
                        "table_name": str(sample.get("table_name", "")),
                        "column_name": str(sample.get("column_name", "")),
                        "description": DESCRIPTION_FAILED,
                        "llm_model": agent.config.model_name,
                    }
                )
    return results

