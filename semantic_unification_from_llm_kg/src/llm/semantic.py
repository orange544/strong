from __future__ import annotations

import json
from typing import Any

from openai import OpenAI

from src.configs.config import (
    LLM_MAX_RETRIES,
    LLM_UNIFY_TIMEOUT_SEC,
)


def _strip_markdown_json_fence(content: str) -> str:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
        if text.lower().startswith("json"):
            text = text[4:].strip()
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


def _validate_field_desc_list(items: list[dict[str, Any]]) -> None:
    required = {"db_name", "table", "field", "description"}
    for index, item in enumerate(items):
        missing = required - set(item.keys())
        if missing:
            raise RuntimeError(
                f"field description item at index {index} missing required keys: {sorted(missing)}"
            )


def _validate_within_domain_output(items: list[dict[str, Any]]) -> None:
    required = {"canonical_name", "fields", "description"}
    for index, item in enumerate(items):
        missing = required - set(item.keys())
        if missing:
            raise RuntimeError(
                f"within-domain output item at index {index} missing required keys: {sorted(missing)}"
            )
        if not isinstance(item["fields"], list):
            raise RuntimeError(
                f"within-domain output item at index {index} field 'fields' must be a list"
            )


def _validate_across_domains_output(items: list[dict[str, Any]]) -> None:
    required = {"canonical_name", "fields", "description"}
    for index, item in enumerate(items):
        missing = required - set(item.keys())
        if missing:
            raise RuntimeError(
                f"across-domains output item at index {index} missing required keys: {sorted(missing)}"
            )
        if not isinstance(item["fields"], list):
            raise RuntimeError(
                f"across-domains output item at index {index} field 'fields' must be a list"
            )


def _check_within_domain_cross_table_only(
    field_desc_list: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
) -> None:
    """
    检查库内归并结果中是否出现同一张表内多个字段被错误归并到同一个簇。
    允许：
      - 同数据库、不同表之间归并
      - 单字段单独成簇
    不允许：
      - 同一张表中的多个字段进入同一个簇
    """
    field_key_to_table: dict[str, str] = {}
    for item in field_desc_list:
        key = f"{item['table']}.{item['field']}"
        field_key_to_table[key] = str(item["table"])

    for index, cluster in enumerate(clusters):
        fields = cluster.get("fields", [])
        tables: list[str] = []
        for field_key in fields:
            table_name = field_key_to_table.get(str(field_key))
            if table_name is not None:
                tables.append(table_name)

        if len(tables) != len(set(tables)):
            raise RuntimeError(
                f"within-domain output item at index {index} groups multiple fields from the same table"
            )


def _check_across_domains_cross_database_only(
    domain_items: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
) -> None:
    """
    检查跨库归并结果中是否把同一数据库中的多个字段/概念放进同一个簇。
    允许：
      - 不同数据库之间归并
      - 单个字段/概念单独成簇
    不允许：
      - 同一数据库中多个成员进入同一个跨库簇
    """
    member_key_to_db: dict[str, str] = {}

    for item in domain_items:
        db_name = str(item.get("db_name", ""))
        table = str(item.get("table", ""))
        field = str(item.get("field", ""))

        # 字段级输入
        if table and field:
            key = f"{table}.{field}"
            member_key_to_db[key] = db_name

        # 概念级输入
        canonical_name = str(item.get("canonical_name", ""))
        if canonical_name and "fields" in item:
            if canonical_name not in member_key_to_db:
                member_key_to_db[canonical_name] = db_name

    for index, cluster in enumerate(clusters):
        fields = cluster.get("fields", [])
        db_names: list[str] = []
        for member_key in fields:
            db_name = member_key_to_db.get(str(member_key))
            if db_name is not None:
                db_names.append(db_name)

        if len(db_names) != len(set(db_names)):
            raise RuntimeError(
                f"across-domains output item at index {index} groups multiple items from the same database"
            )


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
                    {
                        "role": "system",
                        "content": (
                            "You are a structured semantic unification assistant. "
                            "Return only a JSON array and nothing else."
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
            content = content_obj.strip() if isinstance(content_obj, str) else ""
            if not content:
                raise RuntimeError("model returned empty content")

            return _parse_json_array(content)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"LLM semantic unify failed: {exc}") from exc

    def unify_within_domain(self, field_desc_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        输入:
            [
              {
                "db_name": "db1",
                "table": "student",
                "field": "student_id",
                "description": "..."
              },
              ...
            ]

        输出:
            [
              {
                "canonical_name": "student_identifier",
                "fields": ["student.student_id", "enrollment.sid"],
                "description": "..."
              }
            ]
        """
        if not field_desc_list:
            return []

        _validate_field_desc_list(field_desc_list)

        db_names = {str(item["db_name"]) for item in field_desc_list}
        if len(db_names) != 1:
            raise RuntimeError(
                "unify_within_domain expects all input items to belong to the same database"
            )

        db_name = next(iter(db_names))
        json_input = json.dumps(field_desc_list, ensure_ascii=False, indent=2)

        prompt = (
            "Group semantically similar fields within the same database across different tables. "
            "Fields from the same table must not be grouped together. "
            "A group may contain one or more fields. "
            "Do not group fields only by superficial name similarity. "
            "If fields have the same name but different meanings, keep them separate.\n"
            "Output only a JSON array with objects in this format:\n"
            "{\"canonical_name\": str, \"fields\": [str, ...], \"description\": str}\n"
            "In 'fields', each field must be represented as 'table.field'.\n"
            f"db_name:\n{db_name}\n"
            f"input:\n{json_input}"
        )

        result = self._call_llm(prompt)
        _validate_within_domain_output(result)
        _check_within_domain_cross_table_only(field_desc_list, result)
        return result

    def unify_across_domains(self, domain_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        输入:
            可接受两类成员：
            1. 字段级:
               {
                 "db_name": "db1",
                 "table": "student",
                 "field": "student_id",
                 "description": "..."
               }
            2. 已加 db_name 的库内归并结果:
               {
                 "db_name": "db1",
                 "canonical_name": "student_identifier",
                 "fields": ["student.student_id", "enrollment.sid"],
                 "description": "..."
               }

        输出:
            [
              {
                "canonical_name": "student_identifier",
                "fields": ["student.student_id", "learner.learner_no"],
                "description": "..."
              }
            ]
        """
        if not domain_items:
            return []

        json_input = json.dumps(domain_items, ensure_ascii=False, indent=2)

        prompt = (
            "Group semantically similar items across different databases. "
            "Items from the same database must not be grouped together. "
            "A group may contain one or more items. "
            "Do not group items only by superficial name similarity. "
            "If items have the same name but different meanings, keep them separate.\n"
            "Output only a JSON array with objects in this format:\n"
            "{\"canonical_name\": str, \"fields\": [str, ...], \"description\": str}\n"
            "In 'fields', each member should be represented by its semantic member key. "
            "For field-level items, use 'table.field'. "
            "For concept-level items, you may use the canonical_name or representative field names.\n"
            f"input:\n{json_input}"
        )

        result = self._call_llm(prompt)
        _validate_across_domains_output(result)
        _check_across_domains_cross_database_only(domain_items, result)
        return result