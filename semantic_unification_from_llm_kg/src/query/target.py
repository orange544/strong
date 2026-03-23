from __future__ import annotations

from typing import Any


def _get_domain_entry(run_record: dict[str, Any], db_name: str) -> dict[str, Any]:
    domains_obj = run_record.get("domains", [])
    if not isinstance(domains_obj, list):
        raise ValueError("run_record.domains must be a list")

    for domain in domains_obj:
        if not isinstance(domain, dict):
            continue
        if domain.get("db_name") == db_name:
            return domain
    raise ValueError(f"domain not found: {db_name}")


def resolve_in_target_domain(
    match_item: dict[str, Any],
    run_record: dict[str, Any],
    ipfs_client,
) -> dict[str, Any]:
    target_domain = str(match_item["target_domain"])
    target_table = str(match_item["target_table"])
    target_field = str(match_item["target_field"])

    domain_entry = _get_domain_entry(run_record, target_domain)

    desc_cid = domain_entry.get("description_chain_cid") or domain_entry.get("field_descriptions_cid")
    desc_artifact = ipfs_client.cat_json(desc_cid) if desc_cid else {}
    field_descriptions = _coerce_description_records(desc_artifact)

    matched_desc: dict[str, Any] | None = None
    for item in field_descriptions:
        if item.get("table") == target_table and item.get("field") == target_field:
            matched_desc = item
            break

    domain_unified = _coerce_json_list(
        ipfs_client.cat_json(domain_entry["domain_unified_cid"])
        if domain_entry.get("domain_unified_cid")
        else []
    )
    matched_domain_unified: dict[str, Any] | None = None
    for item in domain_unified:
        fields_obj = item.get("fields", [])
        if not isinstance(fields_obj, list):
            continue
        for field_ref in fields_obj:
            if not isinstance(field_ref, str):
                continue
            parts = field_ref.split(".")
            if len(parts) != 3:
                continue
            ref_db, ref_table, ref_field = parts
            if ref_db == target_domain and ref_table == target_table and ref_field == target_field:
                matched_domain_unified = item
                break
        if matched_domain_unified is not None:
            break

    return {
        "target_domain": target_domain,
        "target_table": target_table,
        "target_field": target_field,
        "canonical_name": match_item.get("canonical_name", ""),
        "relation_type": match_item.get("relation_type", ""),
        "score": match_item.get("score", 0.0),
        "field_description": matched_desc,
        "domain_unified_item": matched_domain_unified,
        "resource_refs": {
            "sample_chain_key": domain_entry.get("sample_chain_key"),
            "sample_cid": domain_entry.get("sample_chain_cid") or domain_entry.get("samples_cid"),
            "description_chain_key": domain_entry.get("description_chain_key"),
            "description_cid": domain_entry.get("description_chain_cid")
            or domain_entry.get("field_descriptions_cid"),
            "domain_kg_chain_key": domain_entry.get("domain_kg_chain_key"),
            "domain_kg_cid": domain_entry.get("domain_kg_chain_cid") or domain_entry.get("domain_kg_cid"),
        },
    }


def resolve_matches_in_target_domains(
    matches: list[dict[str, Any]],
    run_record: dict[str, Any],
    ipfs_client,
) -> list[dict[str, Any]]:
    return [resolve_in_target_domain(item, run_record, ipfs_client) for item in matches]


def _coerce_description_records(payload: object) -> list[dict[str, Any]]:
    records_obj = payload.get("field_descriptions", []) if isinstance(payload, dict) else payload
    return _coerce_json_list(records_obj)


def _coerce_json_list(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]
