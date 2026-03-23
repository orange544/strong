from __future__ import annotations

from typing import Any


def load_alignment_index(run_record: dict[str, Any], ipfs_client) -> list[dict[str, Any]]:
    cid = run_record.get("alignment_chain_cid") or run_record.get("alignment_index_cid")
    if not cid:
        return []
    payload = ipfs_client.cat_json(cid)
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def search_alignment_index(
    source_domain: str,
    anchor: dict[str, Any],
    run_record: dict[str, Any],
    ipfs_client,
) -> list[dict[str, Any]]:
    alignment_index = load_alignment_index(run_record, ipfs_client)
    if not alignment_index:
        return []

    canonical_name = anchor.get("canonical_name", "")
    if not isinstance(canonical_name, str):
        canonical_name = ""

    matched_fields_raw = anchor.get("matched_fields", [])
    matched_fields = {
        item for item in matched_fields_raw if isinstance(item, str) and item.strip()
    }

    results: list[dict[str, Any]] = []
    for rel in alignment_index:
        src_domain = rel.get("source_domain")
        src_table = rel.get("source_table")
        src_field = rel.get("source_field")
        tgt_domain = rel.get("target_domain")
        tgt_table = rel.get("target_table")
        tgt_field = rel.get("target_field")
        if not all(
            isinstance(v, str) and v.strip()
            for v in (src_domain, src_table, src_field, tgt_domain, tgt_table, tgt_field)
        ):
            continue

        src_full = f"{src_domain}.{src_table}.{src_field}"
        tgt_full = f"{tgt_domain}.{tgt_table}.{tgt_field}"

        relation_type = rel.get("relation_type", "SAME_AS")
        score = rel.get("score", 1.0)
        canonical_from_rel = rel.get("canonical_name", "")
        if not isinstance(canonical_from_rel, str):
            canonical_from_rel = ""

        if canonical_name and canonical_from_rel == canonical_name:
            if src_domain == source_domain:
                results.append(
                    {
                        "relation_type": relation_type,
                        "score": score,
                        "canonical_name": canonical_from_rel,
                        "source_full_field": src_full,
                        "target_domain": tgt_domain,
                        "target_table": tgt_table,
                        "target_field": tgt_field,
                    }
                )
            elif tgt_domain == source_domain:
                results.append(
                    {
                        "relation_type": relation_type,
                        "score": score,
                        "canonical_name": canonical_from_rel,
                        "source_full_field": tgt_full,
                        "target_domain": src_domain,
                        "target_table": src_table,
                        "target_field": src_field,
                    }
                )
            continue

        if src_full in matched_fields and src_domain == source_domain:
            results.append(
                {
                    "relation_type": relation_type,
                    "score": score,
                    "canonical_name": canonical_from_rel,
                    "source_full_field": src_full,
                    "target_domain": tgt_domain,
                    "target_table": tgt_table,
                    "target_field": tgt_field,
                }
            )
            continue

        if tgt_full in matched_fields and tgt_domain == source_domain:
            results.append(
                {
                    "relation_type": relation_type,
                    "score": score,
                    "canonical_name": canonical_from_rel,
                    "source_full_field": tgt_full,
                    "target_domain": src_domain,
                    "target_table": src_table,
                    "target_field": src_field,
                }
            )

    dedup: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in results:
        key = (
            str(item["target_domain"]),
            str(item["target_table"]),
            str(item["target_field"]),
            str(item["canonical_name"]),
        )
        old = dedup.get(key)
        if old is None or item.get("score", 0.0) > old.get("score", 0.0):
            dedup[key] = item
    return list(dedup.values())
