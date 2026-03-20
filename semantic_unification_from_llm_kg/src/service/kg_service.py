from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from src.db.database_agent import DatabaseAgent, generate_db_data
from src.kg.kg_agent import KnowledgeGraphAgent
from src.storage.ipfs_client import IPFSClient
from src.utils.io import save_json


def run_kg_full(
    ipfs: IPFSClient,
    unified_fields_cid: str,
    db_agents: Dict[str, DatabaseAgent],
    timestamp: str | None = None,
) -> Tuple[str, List[str]]:
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"Fetching unified_fields from IPFS, CID={unified_fields_cid}")
    unified_fields = ipfs.cat_json(unified_fields_cid)

    db_data = generate_db_data(db_agents)
    kg_agent = KnowledgeGraphAgent()
    cypher_list = kg_agent.generate_cypher(unified_fields, db_data)

    cypher_file = save_json(cypher_list, f"cypher_{timestamp}.json")
    return cypher_file, cypher_list
