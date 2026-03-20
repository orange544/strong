from __future__ import annotations

from datetime import datetime

from src.configs.config import DB_PATHS
from src.db.database_agent import DatabaseAgent, get_all_fields
from src.db.plugin_registry import (
    DatabasePluginRegistry,
    DatabaseSource,
    load_db_sources_from_env,
)
from src.storage.ipfs_client import IPFSClient
from src.utils.io import save_json


def _load_runtime_db_sources() -> dict[str, DatabaseSource]:
    loaded = load_db_sources_from_env(legacy_db_paths=DB_PATHS)
    if not isinstance(loaded, dict):
        raise RuntimeError("load_db_sources_from_env must return a source map")

    normalized: dict[str, DatabaseSource] = {}
    for name, source in loaded.items():
        if not isinstance(name, str) or not name.strip():
            raise RuntimeError("database source name must be a non-empty string")
        if not isinstance(source, DatabaseSource):
            raise RuntimeError(f"database source '{name}' has invalid source object")
        normalized[name] = source
    return normalized


def _create_agent_for_source(
    registry: DatabasePluginRegistry,
    source: DatabaseSource,
) -> DatabaseAgent:
    try:
        return registry.create_agent(source)
    except KeyError as exc:
        supported = ", ".join(registry.supported_drivers()) or "<none>"
        raise RuntimeError(
            f"Unsupported database driver '{source.driver}' for source '{source.name}'. "
            f"Supported drivers: {supported}"
        ) from exc


def run_sampling_only(
    *,
    upload_to_ipfs: bool = False,
    timestamp: str | None = None,
) -> dict[str, object]:
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    db_sources = _load_runtime_db_sources()
    if not db_sources:
        raise RuntimeError("No database sources configured. Set DB_SOURCES_JSON or DB_PATHS.")

    registry = DatabasePluginRegistry()
    all_samples: list[dict[str, object]] = []
    db_stats: dict[str, int] = {}
    for db_name, source in db_sources.items():
        print(f"[Sampling] {db_name} [{source.driver}] -> {source.dsn}")
        agent = _create_agent_for_source(registry, source)
        try:
            samples = get_all_fields(agent)
        finally:
            agent.close()

        for item in samples:
            item["db_name"] = db_name
        all_samples.extend(samples)
        db_stats[db_name] = len(samples)

    output_file = save_json(all_samples, f"samples_{timestamp}.json")

    result: dict[str, object] = {
        "timestamp": timestamp,
        "output_file": output_file,
        "total_fields": len(all_samples),
        "databases": db_stats,
    }

    if upload_to_ipfs:
        ipfs = IPFSClient()
        cid = ipfs.add_json(all_samples)
        result["samples_cid"] = cid
        print(f"[Sampling] uploaded to IPFS, CID={cid}")

    return result
