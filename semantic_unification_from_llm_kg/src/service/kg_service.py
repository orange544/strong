from __future__ import annotations

from datetime import datetime
from typing import Any

from src.db.database_agent import DatabaseAgent, generate_db_data
from src.kg.kg_agent import KnowledgeGraphAgent
from src.storage.ipfs_client import IPFSClient
from src.utils.io import save_json


def run_kg_full(
    ipfs: IPFSClient,
    unified_fields_cid: str,
    db_agents: dict[str, DatabaseAgent],
    timestamp: str | None = None,
) -> tuple[str, list[str]]:
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"Fetching unified_fields from IPFS, CID={unified_fields_cid}")
    unified_fields_obj = ipfs.cat_json(unified_fields_cid)
    if not isinstance(unified_fields_obj, list):
        raise RuntimeError("unified_fields payload from IPFS must be a list")
    unified_fields: list[dict[str, Any]] = []
    for index, item in enumerate(unified_fields_obj):
        if not isinstance(item, dict):
            raise RuntimeError(f"unified_fields item at index {index} must be an object")
        unified_fields.append(item)

    db_data = generate_db_data(db_agents)
    kg_agent = KnowledgeGraphAgent()
    empty_domain_map: dict[str, list[dict[str, Any]]] = {name: [] for name in db_data}
    run_record: dict[str, Any] = {
        "timestamp": timestamp,
        "mode": "kg_only_from_existing_unified_fields",
        "domains": [],
        "unified_fields_cid": unified_fields_cid,
    }
    cypher_list = kg_agent.generate_cypher(
        run_record=run_record,
        db_data=db_data,
        domain_field_desc_map=empty_domain_map,
        domain_unified_map=empty_domain_map,
        unified_fields=unified_fields,
    )

    cypher_file = save_json(cypher_list, f"cypher_{timestamp}.json")
    return cypher_file, cypher_list
