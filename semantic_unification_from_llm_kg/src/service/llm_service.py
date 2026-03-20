from __future__ import annotations

from typing import Any

from src.llm.description_agent import FieldDescriptionAgent
from src.storage.ipfs_client import IPFSClient
from src.utils.io import save_json


def _coerce_samples(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        raise RuntimeError("sample payload from IPFS must be a list")

    result: list[dict[str, Any]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise RuntimeError(f"sample item at index {index} must be an object")
        result.append(item)
    return result


def run_llm_pipeline(
    ipfs: IPFSClient,
    samples_cid: str,
    timestamp: str | None = None,
    llm_config: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    if llm_config is None:
        llm_config = {}

    print(f"fetching samples from IPFS, CID={samples_cid}")
    samples = _coerce_samples(ipfs.cat_json(samples_cid))

    fd_agent = FieldDescriptionAgent(
        api_key=llm_config.get("api_key", ""),
        base_url=llm_config.get("base_url", ""),
        model_name=llm_config.get("model_name", ""),
    )

    print("generating field descriptions")
    field_descriptions = [fd_agent.generate_description(item) for item in samples]

    fd_file = f"field_descriptions_{timestamp}.json"
    save_json(field_descriptions, fd_file)

    return field_descriptions


def update_unified_fields_with_new_descriptions(
    previous_unified_fields: list[dict[str, Any]],
    new_field_descriptions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    updated_unified_fields: list[dict[str, Any]] = []

    for new_field in new_field_descriptions:
        field_description = new_field.get("description")
        field_name = new_field.get("field")
        matched = False

        for unified_field in previous_unified_fields:
            if unified_field.get("description") == field_description:
                fields_obj = unified_field.get("fields")
                if isinstance(fields_obj, list):
                    fields_obj.append(field_name)
                else:
                    unified_field["fields"] = [field_name]
                matched = True
                break

        if not matched:
            updated_unified_fields.append(
                {
                    "canonical_name": field_name,
                    "fields": [field_name],
                    "description": field_description,
                }
            )

    updated_unified_fields.extend(previous_unified_fields)
    return updated_unified_fields
