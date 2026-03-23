from __future__ import annotations

import re
from typing import Any


def locate_in_domain_kg(
    source_domain: str,
    parsed_query: dict[str, Any],
    run_record: dict[str, Any],
    ipfs_client,
) -> dict[str, Any]:
    domain_entry = _get_domain_entry(run_record, source_domain)
    if domain_entry is None:
        return _not_found(parsed_query, reason=f"source domain not found: {source_domain}")

    field_descriptions = _load_field_descriptions(domain_entry, ipfs_client)
    domain_unified = _load_domain_unified(domain_entry, ipfs_client)
    query_type = str(parsed_query.get("query_type", ""))

    if query_type == "canonical_concept":
        canonical_name = str(parsed_query.get("canonical_name", "")).strip()
        if not canonical_name:
            return _not_found(parsed_query, reason="canonical_name is empty")
        matched_fields = _fields_for_canonical(source_domain, canonical_name, domain_unified)
        return {
            "anchor_type": "canonical",
            "canonical_name": canonical_name,
            "matched_fields": matched_fields,
            "matched_field_count": len(matched_fields),
            "query_type": query_type,
        }

    if query_type == "qualified_field":
        db_name = str(parsed_query.get("db_name", "")).strip()
        table_name = str(parsed_query.get("table_name", "")).strip()
        field_name = str(parsed_query.get("field_name", "")).strip()
        if db_name != source_domain:
            return _not_found(parsed_query, reason=f"query domain '{db_name}' != '{source_domain}'")
        return _locate_field_anchor(
            source_domain=source_domain,
            table_name=table_name,
            field_name=field_name,
            parsed_query=parsed_query,
            field_descriptions=field_descriptions,
            domain_unified=domain_unified,
        )

    if query_type == "table_field":
        table_name = str(parsed_query.get("table_name", "")).strip()
        field_name = str(parsed_query.get("field_name", "")).strip()
        return _locate_field_anchor(
            source_domain=source_domain,
            table_name=table_name,
            field_name=field_name,
            parsed_query=parsed_query,
            field_descriptions=field_descriptions,
            domain_unified=domain_unified,
        )

    if query_type in {"field_keyword", "natural_language"}:
        keyword = str(parsed_query.get("keyword", "")).strip()
        return _locate_keyword_anchor(
            source_domain=source_domain,
            keyword=keyword,
            parsed_query=parsed_query,
            field_descriptions=field_descriptions,
            domain_unified=domain_unified,
        )

    return _not_found(parsed_query, reason=f"unsupported query_type: {query_type}")


def _get_domain_entry(
    run_record: dict[str, Any],
    source_domain: str,
) -> dict[str, Any] | None:
    domains_obj = run_record.get("domains", [])
    if not isinstance(domains_obj, list):
        return None

    for item in domains_obj:
        if not isinstance(item, dict):
            continue
        db_name = item.get("db_name")
        if isinstance(db_name, str) and db_name == source_domain:
            return item
    return None


def _load_field_descriptions(domain_entry: dict[str, Any], ipfs_client) -> list[dict[str, Any]]:
    cid_obj = domain_entry.get("description_chain_cid") or domain_entry.get("field_descriptions_cid")
    if not isinstance(cid_obj, str) or not cid_obj.strip():
        return []

    payload = ipfs_client.cat_json(cid_obj)
    records_obj: object = payload
    if isinstance(payload, dict):
        records_obj = payload.get("field_descriptions", [])
    if not isinstance(records_obj, list):
        return []
    return [item for item in records_obj if isinstance(item, dict)]


def _load_domain_unified(domain_entry: dict[str, Any], ipfs_client) -> list[dict[str, Any]]:
    cid_obj = domain_entry.get("domain_unified_cid")
    if not isinstance(cid_obj, str) or not cid_obj.strip():
        return []

    payload = ipfs_client.cat_json(cid_obj)
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _locate_field_anchor(
    *,
    source_domain: str,
    table_name: str,
    field_name: str,
    parsed_query: dict[str, Any],
    field_descriptions: list[dict[str, Any]],
    domain_unified: list[dict[str, Any]],
) -> dict[str, Any]:
    table_name = table_name.strip()
    field_name = field_name.strip()
    if not table_name or not field_name:
        return _not_found(parsed_query, reason="table/field is empty")

    exists = any(
        str(item.get("table", "")).strip() == table_name
        and str(item.get("field", "")).strip() == field_name
        for item in field_descriptions
    )
    if not exists:
        return _not_found(parsed_query, reason=f"field not found: {table_name}.{field_name}")

    full_ref = _full_ref(source_domain, table_name, field_name)
    canonical_name = _canonical_for_field(
        source_domain=source_domain,
        full_field_ref=full_ref,
        domain_unified=domain_unified,
    )
    return {
        "anchor_type": "field",
        "canonical_name": canonical_name,
        "matched_fields": [full_ref],
        "matched_field_count": 1,
        "query_type": parsed_query.get("query_type", ""),
    }


def _locate_keyword_anchor(
    *,
    source_domain: str,
    keyword: str,
    parsed_query: dict[str, Any],
    field_descriptions: list[dict[str, Any]],
    domain_unified: list[dict[str, Any]],
) -> dict[str, Any]:
    tokens = _tokenize(keyword)
    if not tokens:
        return _not_found(parsed_query, reason="keyword is empty")

    canonical_candidates: list[tuple[int, dict[str, Any]]] = []
    for item in domain_unified:
        canonical_name = str(item.get("canonical_name", "")).strip()
        description = str(item.get("description", "")).strip()
        score = _score_tokens([canonical_name, description], tokens)
        if score <= 0:
            continue
        canonical_candidates.append((score, item))

    if canonical_candidates:
        canonical_candidates.sort(
            key=lambda pair: (
                pair[0],
                len(_normalized_fields_for_item(source_domain, pair[1])),
            ),
            reverse=True,
        )
        best_item = canonical_candidates[0][1]
        canonical_name = str(best_item.get("canonical_name", "")).strip()
        matched_fields = _normalized_fields_for_item(source_domain, best_item)
        return {
            "anchor_type": "keyword_canonical",
            "canonical_name": canonical_name,
            "matched_fields": matched_fields,
            "matched_field_count": len(matched_fields),
            "query_type": parsed_query.get("query_type", ""),
        }

    field_hits: list[tuple[int, str, str]] = []
    for item in field_descriptions:
        table_name = str(item.get("table", "")).strip()
        field_name = str(item.get("field", "")).strip()
        description = str(item.get("description", "")).strip()
        if not table_name or not field_name:
            continue
        score = _score_tokens([table_name, field_name, description], tokens)
        if score <= 0:
            continue
        field_hits.append((score, table_name, field_name))

    if not field_hits:
        return _not_found(parsed_query, reason="keyword not matched")

    field_hits.sort(key=lambda row: (row[0], row[1], row[2]), reverse=True)
    matched_fields = [_full_ref(source_domain, table_name, field_name) for _, table_name, field_name in field_hits]
    unique_fields = _dedup(matched_fields)
    canonical_names = {
        _canonical_for_field(
            source_domain=source_domain,
            full_field_ref=full_ref,
            domain_unified=domain_unified,
        )
        for full_ref in unique_fields
    }
    canonical_names.discard("")
    canonical_name = canonical_names.pop() if len(canonical_names) == 1 else ""
    return {
        "anchor_type": "keyword_field",
        "canonical_name": canonical_name,
        "matched_fields": unique_fields,
        "matched_field_count": len(unique_fields),
        "query_type": parsed_query.get("query_type", ""),
    }


def _fields_for_canonical(
    source_domain: str,
    canonical_name: str,
    domain_unified: list[dict[str, Any]],
) -> list[str]:
    matched_fields: list[str] = []
    normalized_query = canonical_name.strip().lower()
    for item in domain_unified:
        candidate = str(item.get("canonical_name", "")).strip()
        if candidate.lower() != normalized_query:
            continue
        matched_fields.extend(_normalized_fields_for_item(source_domain, item))
    return _dedup(matched_fields)


def _canonical_for_field(
    *,
    source_domain: str,
    full_field_ref: str,
    domain_unified: list[dict[str, Any]],
) -> str:
    for item in domain_unified:
        canonical_name = str(item.get("canonical_name", "")).strip()
        if not canonical_name:
            continue
        refs = _normalized_fields_for_item(source_domain, item)
        if full_field_ref in refs:
            return canonical_name
    return ""


def _normalized_fields_for_item(source_domain: str, item: dict[str, Any]) -> list[str]:
    refs_obj = item.get("fields", [])
    if not isinstance(refs_obj, list):
        return []

    refs: list[str] = []
    for raw in refs_obj:
        if not isinstance(raw, str) or not raw.strip():
            continue
        parsed = _parse_field_ref(source_domain, raw)
        if parsed is None:
            continue
        db_name, table_name, field_name = parsed
        if db_name != source_domain:
            continue
        refs.append(_full_ref(db_name, table_name, field_name))
    return _dedup(refs)


def _parse_field_ref(source_domain: str, raw_ref: str) -> tuple[str, str, str] | None:
    parts = [segment.strip() for segment in raw_ref.split(".")]
    if len(parts) == 3 and all(parts):
        return parts[0], parts[1], parts[2]
    if len(parts) == 2 and all(parts):
        return source_domain, parts[0], parts[1]
    return None


def _full_ref(db_name: str, table_name: str, field_name: str) -> str:
    return f"{db_name}.{table_name}.{field_name}"


def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in re.findall(r"[A-Za-z0-9_]+", text)]


def _score_tokens(candidates: list[str], tokens: list[str]) -> int:
    haystack = " ".join(candidates).lower()
    return sum(1 for token in tokens if token and token in haystack)


def _dedup(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _not_found(parsed_query: dict[str, Any], *, reason: str) -> dict[str, Any]:
    return {
        "anchor_type": "not_found",
        "canonical_name": "",
        "matched_fields": [],
        "matched_field_count": 0,
        "query_type": parsed_query.get("query_type", ""),
        "reason": reason,
    }
