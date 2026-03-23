from __future__ import annotations

from typing import Any

from src.query.alignment_search import search_alignment_index
from src.query.local_locator import locate_in_domain_kg
from src.query.parser import parse_query
from src.query.target import resolve_matches_in_target_domains


def federated_query(
    source_domain: str,
    query_text: str,
    run_record: dict[str, Any],
    ipfs_client,
) -> dict[str, Any]:
    """
    Minimal federated semantic query chain:
    1) parse query
    2) locate anchor in source domain
    3) search alignment index for cross-domain matches
    4) resolve matched fields in target domains
    """
    parsed_query = parse_query(query_text)

    anchor = locate_in_domain_kg(
        source_domain=source_domain,
        parsed_query=parsed_query,
        run_record=run_record,
        ipfs_client=ipfs_client,
    )

    if anchor.get("anchor_type") == "not_found":
        return {
            "source_domain": source_domain,
            "query_text": query_text,
            "parsed_query": parsed_query,
            "anchor": anchor,
            "matches": [],
            "resolved_results": [],
        }

    matches = search_alignment_index(
        source_domain=source_domain,
        anchor=anchor,
        run_record=run_record,
        ipfs_client=ipfs_client,
    )
    resolved_results = resolve_matches_in_target_domains(
        matches=matches,
        run_record=run_record,
        ipfs_client=ipfs_client,
    )

    return {
        "source_domain": source_domain,
        "query_text": query_text,
        "parsed_query": parsed_query,
        "anchor": anchor,
        "matches": matches,
        "resolved_results": resolved_results,
    }
