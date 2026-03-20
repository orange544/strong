from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.configs.config import AUTO_PIPELINE_DEFAULTS, DB_PATHS, LLM_DESC_CONFIG, LLM_UNIFY_CONFIG
from src.db.plugin_registry import (
    DatabasePluginRegistry,
    DatabaseSource,
    load_db_sources_from_env,
)
from src.llm.description_agent import FieldDescriptionAgent
from src.storage.ipfs_client import IPFSClient
from src.utils.io import save_json

if TYPE_CHECKING:
    from src.db.database_agent import DatabaseAgent


def run_llm_pipeline(
    ipfs: IPFSClient,
    samples_cid: str,
    timestamp: str | None = None,
    llm_config: dict[str, str] | None = None,
) -> str:
    """Load sampled fields from IPFS, generate descriptions, then publish descriptions to IPFS."""
    if llm_config is None:
        llm_config = {}

    print(f"Fetching sample payload from IPFS, CID={samples_cid}")
    samples = _coerce_sample_records(ipfs.cat_json(samples_cid))

    fd_agent = FieldDescriptionAgent(
        api_key=llm_config.get("api_key", ""),
        base_url=llm_config.get("base_url", ""),
        model_name=llm_config.get("model_name", ""),
    )

    print("Generating field descriptions")
    field_descriptions = [fd_agent.generate_description(item) for item in samples]

    fd_file = f"field_descriptions_{timestamp}.json"
    save_json(field_descriptions, fd_file)

    field_desc_cid = ipfs.add_json(field_descriptions)
    print(f"Field descriptions generated, CID={field_desc_cid}")
    return field_desc_cid


def _coerce_sample_records(payload: object) -> list[dict[str, Any]]:
    records_obj: object = payload
    if isinstance(payload, dict):
        records_obj = payload.get("samples", [])

    if not isinstance(records_obj, list):
        raise RuntimeError("sample payload from IPFS must be a list or an artifact with 'samples'")

    records: list[dict[str, Any]] = []
    for index, item in enumerate(records_obj):
        if not isinstance(item, dict):
            raise RuntimeError(f"sample item at index {index} must be an object")
        records.append(item)
    return records


def _new_registry() -> DatabasePluginRegistry:
    return DatabasePluginRegistry()


def _load_runtime_db_sources() -> dict[str, DatabaseSource]:
    return load_db_sources_from_env(legacy_db_paths=DB_PATHS)


def _discover_sqlite_sources_from_folder(db_folder: str) -> dict[str, DatabaseSource]:
    if not os.path.isdir(db_folder):
        return {}

    discovered: dict[str, DatabaseSource] = {}
    for filename in sorted(os.listdir(db_folder)):
        db_path = os.path.join(db_folder, filename)
        if not os.path.isfile(db_path):
            continue
        if not filename.lower().endswith((".db", ".sqlite", ".sqlite3")):
            continue

        source_name = os.path.splitext(filename)[0].strip() or filename
        discovered[source_name] = DatabaseSource(
            name=source_name,
            driver="sqlite",
            dsn=db_path,
            options={},
        )
    return discovered


def _collect_candidate_sources(db_folder: str) -> dict[str, DatabaseSource]:
    # Configured sources are authoritative and override auto-discovered names.
    candidates = _discover_sqlite_sources_from_folder(db_folder)
    candidates.update(_load_runtime_db_sources())
    return candidates


def _source_signature(source: DatabaseSource) -> str:
    driver = source.driver.strip().lower()
    dsn = source.dsn.strip()
    if driver == "sqlite" and dsn:
        dsn = _resolve_sqlite_dsn(dsn)
    return f"{source.name}|{driver}|{dsn}"


def _resolve_sqlite_dsn(dsn: str) -> str:
    path = Path(dsn)
    if not path.is_absolute():
        path = (Path(__file__).resolve().parents[2] / path).resolve()
    return str(path)


def _create_agent_for_source(
    registry: DatabasePluginRegistry,
    source: DatabaseSource,
) -> dict[str, DatabaseAgent]:
    dsn = source.dsn.strip()
    if not dsn:
        raise RuntimeError(f"Database source '{source.name}' has an empty DSN")

    source_for_agent = source
    if source.driver.strip().lower() == "sqlite":
        resolved_dsn = _resolve_sqlite_dsn(dsn)
        if not os.path.isfile(resolved_dsn):
            raise RuntimeError(f"New database file not found: {resolved_dsn}")
        source_for_agent = DatabaseSource(
            name=source.name,
            driver=source.driver,
            dsn=resolved_dsn,
            options=dict(source.options),
        )

    try:
        agent = registry.create_agent(source_for_agent)
    except KeyError as exc:
        supported = ", ".join(registry.supported_drivers()) or "<none>"
        raise RuntimeError(
            f"Unsupported database driver '{source.driver}' for source '{source.name}'. "
            f"Supported drivers: {supported}"
        ) from exc

    return {source.name: agent}


def _create_agent_for_new_db(
    registry: DatabasePluginRegistry,
    db_folder: str,
    db_file: str,
) -> dict[str, DatabaseAgent]:
    db_path = os.path.join(db_folder, db_file)
    if not os.path.isfile(db_path):
        raise RuntimeError(f"New database file not found: {db_path}")

    source = DatabaseSource(
        name=db_file,
        driver="sqlite",
        dsn=db_path,
        options={},
    )

    return _create_agent_for_source(registry, source)


def _run_sampling(
    db_agents: dict[str, DatabaseAgent],
    ipfs: IPFSClient,
    timestamp: str,
) -> str:
    from src.service.sample import run_sampling

    return run_sampling(db_agents, ipfs, timestamp)


def _unify_fields_with_existing(
    *,
    field_descriptions: list[dict[str, object]],
    existing_unified_fields_cid: str,
    ipfs: IPFSClient,
    llm_config: dict[str, str],
) -> str:
    from src.service.semantic_service import unify_fields_with_existing

    return unify_fields_with_existing(
        field_descriptions=field_descriptions,
        existing_unified_fields_cid=existing_unified_fields_cid,
        ipfs=ipfs,
        llm_config=llm_config,
    )


def _run_kg_full(
    ipfs: IPFSClient,
    unified_fields_cid: str,
    db_agents: dict[str, DatabaseAgent],
) -> tuple[str, list[str]]:
    from src.service.kg_service import run_kg_full

    return run_kg_full(ipfs, unified_fields_cid, db_agents)


def monitor_and_process_new_database(
    ipfs: IPFSClient,
    db_folder: str,
    previous_unified_fields_cid: str,
) -> None:
    """Incrementally process newly discovered DB sources from config and folder."""
    try:
        initial_sources = _collect_candidate_sources(db_folder)
    except ValueError as exc:
        print(f"Initial source discovery failed, starting with empty baseline: {exc}")
        initial_sources = {}

    known_signatures = {
        _source_signature(source)
        for source in initial_sources.values()
    }
    registry = _new_registry()

    while True:
        try:
            current_sources = _collect_candidate_sources(db_folder)
        except ValueError as exc:
            # Keep watcher alive when DB_SOURCES_JSON is temporarily malformed.
            print(f"Skipping discovery due to invalid source config: {exc}")
            time.sleep(AUTO_PIPELINE_DEFAULTS["poll_interval_sec"])
            continue

        new_sources = [
            source
            for source in current_sources.values()
            if _source_signature(source) not in known_signatures
        ]

        if new_sources:
            new_names = [source.name for source in new_sources]
            print(f"Detected new database sources: {new_names}")

            for source in new_sources:
                db_agents: dict[str, DatabaseAgent] = {}
                source_sig = _source_signature(source)
                try:
                    db_agents = _create_agent_for_source(registry, source)

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    samples_cid = _run_sampling(db_agents, ipfs, timestamp)

                    field_desc_cid = run_llm_pipeline(
                        ipfs,
                        samples_cid,
                        timestamp,
                        LLM_DESC_CONFIG,
                    )

                    field_descriptions = ipfs.cat_json(field_desc_cid)
                    updated_unified_fields_cid = _unify_fields_with_existing(
                        field_descriptions=field_descriptions,
                        existing_unified_fields_cid=previous_unified_fields_cid,
                        ipfs=ipfs,
                        llm_config=LLM_UNIFY_CONFIG,
                    )

                    _cypher_file, _cypher_list = _run_kg_full(
                        ipfs,
                        updated_unified_fields_cid,
                        db_agents,
                    )

                    previous_unified_fields_cid = updated_unified_fields_cid
                    known_signatures.add(source_sig)
                    print(f"New database processed: {source.name}")
                except Exception as exc:  # noqa: BLE001
                    print(f"Processing failed for {source.name}: {exc}")
                finally:
                    for agent in db_agents.values():
                        agent.close()
        time.sleep(AUTO_PIPELINE_DEFAULTS["poll_interval_sec"])


def run_auto() -> None:
    ipfs = IPFSClient()

    previous_unified_fields_cid = AUTO_PIPELINE_DEFAULTS["previous_unified_fields_cid"]
    if not previous_unified_fields_cid:
        raise RuntimeError("AUTO_PREVIOUS_UNIFIED_FIELDS_CID is empty in .env")

    monitor_and_process_new_database(
        ipfs,
        AUTO_PIPELINE_DEFAULTS["db_folder"],
        previous_unified_fields_cid,
    )


if __name__ == "__main__":
    run_auto()
