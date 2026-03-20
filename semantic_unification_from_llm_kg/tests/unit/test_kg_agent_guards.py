from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.kg.kg_agent import KnowledgeGraphAgent, esc


def test_esc_escapes_backslash_and_single_quote() -> None:
    assert esc("a\\b'c") == "a\\\\b\\'c"


def test_generate_cypher_skips_invalid_refs_and_missing_artifact_links() -> None:
    agent = KnowledgeGraphAgent()

    run_record: dict[str, Any] = {
        "domains": [
            {"db_name": "DB1"},
            {"db_name": "DB2"},
        ],
        "unified_fields_file": "unified.json",
        "unified_fields_cid": "uf-cid",
    }
    db_data = {"DB1": {"movie": ["id"]}}
    domain_field_desc_map = {
        "MISSING": [{"table": "movie", "field": "id"}],
        "DB2": [{"table": "movie", "field": "title"}],
    }
    domain_unified_map = {
        "DB1": [
            {
                "canonical_name": "movie_id",
                "description": "identifier",
                "fields": ["invalid_ref_without_three_parts"],
            }
        ]
    }
    unified_fields = [
        {
            "canonical_name": "std_movie_id",
            "description": "id",
            "fields": ["invalid_ref", "DB1.movie.id"],
        }
    ]

    cypher = agent.generate_cypher(
        run_record=run_record,
        db_data=db_data,
        domain_field_desc_map=domain_field_desc_map,
        domain_unified_map=domain_unified_map,
        unified_fields=unified_fields,
    )

    assert isinstance(cypher, list)
    assert any("MERGE (ds:DataSource {name:'DB1'})" in stmt for stmt in cypher)
    # Invalid refs are skipped, but valid refs still produce StandardPropertyConcept links.
    assert any("IS_STANDARDIZED_AS" in stmt for stmt in cypher)
