from __future__ import annotations

import os
import re
import time
from collections.abc import Mapping
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

_TIMESTAMP_TOKEN_PATTERN = re.compile(r"^[0-9A-Za-z_-]{1,64}$")


def _coerce_timestamp_token(timestamp: object | None) -> str:
    if timestamp is None:
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    if not isinstance(timestamp, str):
        raise RuntimeError("timestamp must be a string")
    token = timestamp.strip()
    if not token:
        raise RuntimeError("timestamp must be a non-empty string")
    if not _TIMESTAMP_TOKEN_PATTERN.fullmatch(token):
        raise RuntimeError("timestamp token contains unsafe characters")
    return token


def _coerce_llm_config(config: object) -> dict[str, str]:
    if config is None:
        return {}
    if not isinstance(config, Mapping):
        raise RuntimeError("llm_config must be a mapping when provided")

    normalized: dict[str, str] = {}
    for key in ("api_key", "base_url", "model_name"):
        value = config.get(key, "")
        if value is None:
            normalized[key] = ""
            continue
        if not isinstance(value, str):
            raise RuntimeError(f"llm_config['{key}'] must be a string")
        normalized[key] = value
    return normalized


def run_llm_pipeline(
    ipfs: IPFSClient,
    samples_cid: str,
    timestamp: str | None = None,
    llm_config: dict[str, str] | None = None,
) -> str:
    """Load sampled fields from IPFS, generate descriptions, then publish descriptions to IPFS."""
    normalized_timestamp = _coerce_timestamp_token(timestamp)
    normalized_llm_config = _coerce_llm_config(llm_config)

    print(f"Fetching sample payload from IPFS, CID={samples_cid}")
    samples = _coerce_sample_records(ipfs.cat_json(samples_cid))

    fd_agent = FieldDescriptionAgent(
        api_key=normalized_llm_config.get("api_key", ""),
        base_url=normalized_llm_config.get("base_url", ""),
        model_name=normalized_llm_config.get("model_name", ""),
    )

    print("Generating field descriptions")
    field_descriptions = [fd_agent.generate_description(item) for item in samples]

    fd_file = f"field_descriptions_{normalized_timestamp}.json"
    save_json(field_descriptions, fd_file)

    field_desc_cid = ipfs.add_json(field_descriptions)
    if not isinstance(field_desc_cid, str):
        raise RuntimeError("ipfs.add_json must return a CID string")
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


def _auto_db_folder() -> str:
    raw = AUTO_PIPELINE_DEFAULTS.get("db_folder")
    if not isinstance(raw, str) or not raw.strip():
        raise RuntimeError("AUTO_DB_FOLDER must be a non-empty string in .env")
    return raw.strip()


def _auto_previous_unified_fields_cid() -> str:
    raw = AUTO_PIPELINE_DEFAULTS.get("previous_unified_fields_cid")
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raise RuntimeError("AUTO_PREVIOUS_UNIFIED_FIELDS_CID must be a string in .env")
    return raw.strip()


def _auto_poll_interval_sec() -> int:
    raw = AUTO_PIPELINE_DEFAULTS.get("poll_interval_sec")
    if isinstance(raw, int):
        return max(1, raw)
    if isinstance(raw, str):
        try:
            return max(1, int(raw.strip()))
        except ValueError as exc:
            raise RuntimeError("AUTO_POLL_INTERVAL_SEC must be an integer in .env") from exc
    raise RuntimeError("AUTO_POLL_INTERVAL_SEC must be an integer in .env")


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

    cid = run_sampling(db_agents, ipfs, timestamp)
    if not isinstance(cid, str):
        raise RuntimeError("run_sampling must return a CID string")
    return cid


def _unify_fields_with_existing(
    *,
    field_descriptions: list[dict[str, object]],
    existing_unified_fields_cid: str,
    ipfs: IPFSClient,
    llm_config: dict[str, str],
) -> str:
    from src.service.semantic_service import unify_fields_with_existing

    cid = unify_fields_with_existing(
        field_descriptions=field_descriptions,
        existing_unified_fields_cid=existing_unified_fields_cid,
        ipfs=ipfs,
        llm_config=llm_config,
    )
    if not isinstance(cid, str):
        raise RuntimeError("unify_fields_with_existing must return a CID string")
    return cid


def _run_kg_full(
    ipfs: IPFSClient,
    unified_fields_cid: str,
    db_agents: dict[str, DatabaseAgent],
) -> tuple[str, list[str]]:
    from src.service.kg_service import run_kg_full

    result = run_kg_full(ipfs, unified_fields_cid, db_agents)
    if not isinstance(result, tuple) or len(result) != 2:
        raise RuntimeError("run_kg_full must return (cypher_file, cypher_list)")
    cypher_file, cypher_list = result
    if not isinstance(cypher_file, str):
        raise RuntimeError("run_kg_full cypher_file must be a string")
    if not isinstance(cypher_list, list):
        raise RuntimeError("run_kg_full cypher_list must be a list")
    return cypher_file, cypher_list


def monitor_and_process_new_database(
    ipfs: IPFSClient,
    db_folder: str,
    previous_unified_fields_cid: str,
) -> None:
    """Incrementally process newly discovered DB sources from config and folder."""
    poll_interval_sec = _auto_poll_interval_sec()
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
            time.sleep(poll_interval_sec)
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
        time.sleep(poll_interval_sec)


def run_auto() -> None:
    ipfs = IPFSClient()

    previous_unified_fields_cid = _auto_previous_unified_fields_cid()
    if not previous_unified_fields_cid:
        raise RuntimeError("AUTO_PREVIOUS_UNIFIED_FIELDS_CID is empty in .env")

    monitor_and_process_new_database(
        ipfs,
        _auto_db_folder(),
        previous_unified_fields_cid,
    )


if __name__ == "__main__":
    run_auto()
