from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.llm.semantic import FieldSemanticAgent
from src.storage.ipfs_client import IPFSClient
from src.utils.io import save_json

_TIMESTAMP_TOKEN_PATTERN = re.compile(r"^[0-9A-Za-z_-]{1,64}$")


def _coerce_non_empty_string(value: object, *, context: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"{context} must be a string")
    normalized = value.strip()
    if not normalized:
        raise RuntimeError(f"{context} must be a non-empty string")
    return normalized


def _coerce_timestamp_token(timestamp: object | None) -> str:
    if timestamp is None:
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    token = _coerce_non_empty_string(timestamp, context="timestamp")
    if not _TIMESTAMP_TOKEN_PATTERN.fullmatch(token):
        raise RuntimeError("timestamp token contains unsafe characters")
    return token


def _coerce_llm_config(payload: object) -> dict[str, str]:
    if not isinstance(payload, Mapping):
        raise RuntimeError("llm_config must be a mapping")

    normalized: dict[str, str] = {}
    for key in ("api_key", "base_url", "model_name"):
        value = payload.get(key, "")
        if value is None:
            normalized[key] = ""
            continue
        if not isinstance(value, str):
            raise RuntimeError(f"llm_config['{key}'] must be a string")
        normalized[key] = value
    return normalized


def _coerce_cid(value: object, *, context: str) -> str:
    return _coerce_non_empty_string(value, context=context)


def _coerce_object_list(payload: object, *, context: str) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        raise RuntimeError(f"{context} must be a list")

    result: list[dict[str, Any]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise RuntimeError(f"{context} item at index {index} must be an object")
        result.append(item)
    return result


def _coerce_field_descriptions(payload: object) -> list[dict[str, Any]]:
    rows = _coerce_object_list(payload, context="field_descriptions")
    for index, item in enumerate(rows):
        table = item.get("table")
        field = item.get("field")
        if not isinstance(table, str) or not table.strip():
            raise RuntimeError(f"field_descriptions item at index {index} missing non-empty table")
        if not isinstance(field, str) or not field.strip():
            raise RuntimeError(f"field_descriptions item at index {index} missing non-empty field")
    return rows


def _coerce_unified_fields(payload: object, *, context: str) -> list[dict[str, Any]]:
    rows = _coerce_object_list(payload, context=context)
    normalized: list[dict[str, Any]] = []

    for index, item in enumerate(rows):
        canonical_name = item.get("canonical_name")
        description = item.get("description", "")
        fields_obj = item.get("fields", [])

        if not isinstance(canonical_name, str) or not canonical_name.strip():
            raise RuntimeError(f"{context} item at index {index} missing non-empty canonical_name")
        if not isinstance(description, str):
            raise RuntimeError(f"{context} item at index {index} has non-string description")
        if not isinstance(fields_obj, list):
            raise RuntimeError(f"{context} item at index {index} has non-list fields")

        fields: list[str] = []
        seen: set[str] = set()
        for field_index, field_value in enumerate(fields_obj):
            if not isinstance(field_value, str):
                raise RuntimeError(
                    f"{context} item at index {index} has non-string field at index {field_index}"
                )
            if field_value in seen:
                continue
            seen.add(field_value)
            fields.append(field_value)

        normalized.append(
            {
                "canonical_name": canonical_name,
                "fields": fields,
                "description": description,
            }
        )
    return normalized


def _unify_new_fields(
    fs_agent: FieldSemanticAgent,
    field_descriptions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not field_descriptions:
        return []

    legacy_unify = getattr(fs_agent, "unify_fields", None)
    if callable(legacy_unify):
        unified_raw = legacy_unify(field_descriptions)
        return _coerce_unified_fields(unified_raw, context="llm_unified_fields")

    within_domain_unify = getattr(fs_agent, "unify_within_domain", None)
    if callable(within_domain_unify):
        unified_raw = within_domain_unify(field_descriptions)
        return _coerce_unified_fields(unified_raw, context="llm_unified_fields")

    raise RuntimeError(
        "FieldSemanticAgent must provide 'unify_fields' or 'unify_within_domain'"
    )


def unify_fields_with_existing(
    field_descriptions: list[dict[str, Any]],
    existing_unified_fields_cid: str,
    ipfs: IPFSClient,
    llm_config: dict[str, str],
    timestamp: str | None = None,
) -> str:
    normalized_timestamp = _coerce_timestamp_token(timestamp)
    normalized_existing_cid = _coerce_cid(
        existing_unified_fields_cid,
        context="existing_unified_fields_cid",
    )
    normalized_llm_config = _coerce_llm_config(llm_config)

    existing_payload = ipfs.cat_json(normalized_existing_cid)
    existing_unified_fields = _coerce_unified_fields(
        existing_payload,
        context="existing_unified_fields",
    )
    normalized_field_descriptions = _coerce_field_descriptions(field_descriptions)

    fs_agent = FieldSemanticAgent(
        api_key=normalized_llm_config["api_key"],
        base_url=normalized_llm_config["base_url"],
        model_name=normalized_llm_config["model_name"],
    )
    new_unified_fields = _unify_new_fields(fs_agent, normalized_field_descriptions)

    updated_unified_fields = merge_unified_fields(existing_unified_fields, new_unified_fields)

    uf_file = f"unified_fields_{normalized_timestamp}.json"
    save_json(updated_unified_fields, uf_file)
    updated_unified_fields_cid = _coerce_cid(
        ipfs.add_json(updated_unified_fields),
        context="ipfs.add_json return value",
    )
    print(f"Unified fields updated, CID={updated_unified_fields_cid}")
    return updated_unified_fields_cid


def merge_unified_fields(
    existing: list[dict[str, Any]],
    new: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    existing_items = _coerce_unified_fields(existing, context="existing_unified_fields")
    new_items = _coerce_unified_fields(new, context="new_unified_fields")

    merged = [
        {
            "canonical_name": item["canonical_name"],
            "fields": list(item["fields"]),
            "description": item["description"],
        }
        for item in existing_items
    ]
    existing_names = {item["canonical_name"] for item in existing_items}

    for item in new_items:
        if item["canonical_name"] in existing_names:
            continue
        merged.append(item)
        existing_names.add(item["canonical_name"])

    return merged
