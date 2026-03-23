from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from pprint import pprint
from typing import TYPE_CHECKING, Any

from src.configs.config import (
    AUTO_PIPELINE_DEFAULTS,
    DB_PATHS,
    LLM_DESC_CONFIG,
    LLM_UNIFY_CONFIG,
    PIPELINE_CONFIG,
)
from src.db.plugin_registry import (
    DatabasePluginRegistry,
    DatabaseSource,
    load_db_sources_from_env,
)
from src.kg.kg_agent import KnowledgeGraphAgent
from src.llm.description_agent import FieldDescriptionAgent
from src.llm.semantic import FieldSemanticAgent
from src.pipeline.orchestration_common import (
    attach_db_name_to_domain_unified as _attach_db_name_to_domain_unified,
)
from src.pipeline.orchestration_common import (
    build_sample_artifact as _build_sample_artifact,
)
from src.pipeline.orchestration_common import (
    generate_descriptions_parallel as _generate_descriptions_parallel,
)
from src.pipeline.orchestration_common import (
    safe_db_tag as _safe_db_tag,
)
from src.pipeline.orchestration_common import (
    wrap_single_table_fields_for_cross_domain as _wrap_single_table_fields_for_cross_domain,
)
from src.pipeline.unified_interface import (
    build_db_data_from_field_units,
    extract_field_units_by_source,
    field_units_to_sample_records,
)
from src.storage.ipfs_client import IPFSClient
from src.storage.registry import append_run_record
from src.utils.io import save_json

if TYPE_CHECKING:
    from src.db.database_agent import DatabaseAgent


def _load_runtime_db_sources() -> dict[str, DatabaseSource]:
    try:
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
    except ValueError as exc:
        # Preserve pipeline availability when DB_SOURCES_JSON is temporarily malformed.
        print(f"[warn] invalid DB_SOURCES_JSON, fallback to legacy DB_PATHS: {exc}")
        return {
            name: DatabaseSource(name=name, driver="sqlite", dsn=path, options={})
            for name, path in DB_PATHS.items()
            if name.strip() and path.strip()
        }


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
    # Config sources are authoritative and override same-name auto-discovered files.
    candidates = _discover_sqlite_sources_from_folder(db_folder)
    candidates.update(_load_runtime_db_sources())
    return candidates


def _auto_db_folder() -> str:
    raw = AUTO_PIPELINE_DEFAULTS.get("db_folder")
    if not isinstance(raw, str) or not raw.strip():
        raise RuntimeError("AUTO_DB_FOLDER must be a non-empty string in .env")
    return raw.strip()


def _resolve_sqlite_dsn(dsn: str) -> str:
    path = Path(dsn)
    if not path.is_absolute():
        path = (Path(__file__).resolve().parents[2] / path).resolve()
    return str(path)


def _normalize_source_for_agent(source: DatabaseSource) -> DatabaseSource:
    driver = source.driver.strip().lower()
    if driver != "sqlite":
        return source

    resolved_path = Path(_resolve_sqlite_dsn(source.dsn.strip()))
    if not resolved_path.is_file():
        raise RuntimeError(
            f"Database source '{source.name}' points to missing sqlite file: {resolved_path}"
        )
    try:
        file_size = resolved_path.stat().st_size
    except OSError as exc:
        raise RuntimeError(
            f"Failed to inspect sqlite file for source '{source.name}': {resolved_path}"
        ) from exc
    if file_size <= 0:
        raise RuntimeError(
            f"Database source '{source.name}' points to an empty sqlite file: {resolved_path}"
        )

    return DatabaseSource(
        name=source.name,
        driver=source.driver,
        dsn=str(resolved_path),
        options=dict(source.options),
    )


def _create_db_agents(
    db_sources: dict[str, DatabaseSource],
    registry: DatabasePluginRegistry,
) -> dict[str, DatabaseAgent]:
    db_agents: dict[str, DatabaseAgent] = {}
    try:
        for db_name, source in db_sources.items():
            if not source.dsn.strip():
                raise RuntimeError(f"Database source '{db_name}' has an empty DSN")
            source_for_agent = _normalize_source_for_agent(source)
            try:
                db_agents[db_name] = registry.create_agent(source_for_agent)
            except KeyError as exc:
                supported = ", ".join(registry.supported_drivers()) or "<none>"
                raise RuntimeError(
                    f"Unsupported database driver '{source.driver}' for source '{db_name}'. "
                    f"Supported drivers: {supported}"
                ) from exc
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    f"Failed to create database agent for source '{db_name}': {exc}"
                ) from exc
        return db_agents
    except Exception:
        for agent in db_agents.values():
            agent.close()
        raise


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

        table = item.get("table")
        field = item.get("field")
        if not isinstance(table, str) or not table.strip():
            raise RuntimeError(f"sample item at index {index} missing non-empty table")
        if not isinstance(field, str) or not field.strip():
            raise RuntimeError(f"sample item at index {index} missing non-empty field")
        records.append(item)
    return records


def _persist_run_record(run_record: dict[str, Any], timestamp: str) -> None:
    append_run_record(run_record)
    save_json(run_record, f"run_manifest_{timestamp}.json")


def run_all() -> None:
    db_folder = _auto_db_folder()
    db_sources = _collect_candidate_sources(db_folder)
    if not db_sources:
        raise RuntimeError(
            "No database sources configured. Set DB_SOURCES_JSON/DB_PATHS "
            f"or place SQLite files in '{db_folder}'."
        )

    max_workers = PIPELINE_CONFIG["llm_desc_max_workers"]
    domain_timeout_sec = PIPELINE_CONFIG["llm_desc_domain_timeout_sec"]
    max_fields_per_domain = PIPELINE_CONFIG["run_max_fields_per_domain"]
    domain_field_units = extract_field_units_by_source(
        db_sources,
        max_fields_per_domain=max_fields_per_domain,
    )

    ipfs = IPFSClient()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    run_record: dict[str, Any] = {
        "timestamp": timestamp,
        "status": "running",
        "databases": list(db_sources.keys()),
        "llm_desc_model": LLM_DESC_CONFIG["model_name"],
        "llm_unify_model": LLM_UNIFY_CONFIG["model_name"],
        "domains": [],
    }

    # Keep in-memory aggregate descriptions for downstream unification only.
    all_field_descriptions: list[dict[str, Any]] = []

    try:
        # ---------- Step 1: per-domain sampling + per-domain sample artifacts ----------
        for db_name in db_sources:
            db_tag = _safe_db_tag(db_name)
            print(f"sampling database: {db_name}")
            samples = field_units_to_sample_records(domain_field_units[db_name])

            sample_artifact = _build_sample_artifact(db_name, timestamp, samples)
            sample_filename = f"samples_{db_tag}_{timestamp}.json"
            save_json(sample_artifact, sample_filename)
            sample_cid = ipfs.add_json(sample_artifact)

            run_record["domains"].append(
                {
                    "db_name": db_name,
                    "sample_file": sample_filename,
                    "samples_cid": sample_cid,
                    "sampled_field_count": sample_artifact["summary"]["sampled_field_count"],
                    "total_sample_value_count": sample_artifact["summary"]["total_sample_value_count"],
                }
            )

        # ---------- Step 2: per-domain descriptions + per-domain description artifacts ----------
        fd_agent = FieldDescriptionAgent(
            api_key=LLM_DESC_CONFIG["api_key"],
            base_url=LLM_DESC_CONFIG["base_url"],
            model_name=LLM_DESC_CONFIG["model_name"],
        )
        print(
            f"generating descriptions with workers={max_workers}, "
            f"domain_timeout={domain_timeout_sec}s"
        )

        for domain_entry in run_record["domains"]:
            db_name = domain_entry["db_name"]
            db_tag = _safe_db_tag(db_name)
            sample_cid = domain_entry["samples_cid"]

            sample_artifact = ipfs.cat_json(sample_cid)
            domain_samples = _coerce_sample_records(sample_artifact)
            print(f"describe domain={db_name}, fields={len(domain_samples)}")

            field_descriptions = _generate_descriptions_parallel(
                fd_agent=fd_agent,
                samples=domain_samples,
                max_workers=max_workers,
                domain_timeout_sec=domain_timeout_sec,
            )

            for item in field_descriptions:
                item["db_name"] = db_name

            desc_artifact: dict[str, Any] = {
                "summary": {
                    "db_name": db_name,
                    "timestamp": timestamp,
                    "description_count": len(field_descriptions),
                    "source_samples_cid": sample_cid,
                },
                "field_descriptions": field_descriptions,
            }

            desc_filename = f"field_descriptions_{db_tag}_{timestamp}.json"
            save_json(desc_artifact, desc_filename)
            desc_cid = ipfs.add_json(desc_artifact)

            domain_entry["field_descriptions_file"] = desc_filename
            domain_entry["field_descriptions_cid"] = desc_cid
            domain_entry["description_count"] = len(field_descriptions)

            all_field_descriptions.extend(field_descriptions)

        # ---------- Step 3: two-stage semantic unification ----------
        fs_agent = FieldSemanticAgent(
            api_key=LLM_UNIFY_CONFIG["api_key"],
            base_url=LLM_UNIFY_CONFIG["base_url"],
            model_name=LLM_UNIFY_CONFIG["model_name"],
        )

        domain_level_items: list[dict[str, Any]] = []

        for domain_entry in run_record["domains"]:
            db_name = domain_entry["db_name"]
            desc_cid = domain_entry["field_descriptions_cid"]

            desc_artifact = ipfs.cat_json(desc_cid)
            field_descriptions = desc_artifact.get("field_descriptions", [])

            tables = {item["table"] for item in field_descriptions if item.get("table")}

            # Single-table domains skip within-domain unification.
            if len(tables) <= 1:
                print(f"single-table domain {db_name}, skip within-domain unify")
                domain_unified = _wrap_single_table_fields_for_cross_domain(field_descriptions)
            else:
                print(f"within-domain unify {db_name}, fields={len(field_descriptions)}")
                domain_unified = fs_agent.unify_within_domain(field_descriptions)
                domain_unified = _attach_db_name_to_domain_unified(domain_unified, db_name)

            domain_unified_file = f"domain_unified_{_safe_db_tag(db_name)}_{timestamp}.json"
            save_json(domain_unified, domain_unified_file)
            domain_unified_cid = ipfs.add_json(domain_unified)

            domain_entry["domain_unified_file"] = domain_unified_file
            domain_entry["domain_unified_cid"] = domain_unified_cid
            domain_entry["domain_unified_count"] = len(domain_unified)

            domain_level_items.extend(domain_unified)

        print(f"cross-domain unify candidates={len(domain_level_items)}")
        unified_fields = fs_agent.unify_across_domains(domain_level_items)

        uf_file = f"unified_fields_{timestamp}.json"
        save_json(unified_fields, uf_file)
        unified_fields_cid = ipfs.add_json(unified_fields)
        run_record["unified_fields_file"] = uf_file
        run_record["unified_fields_cid"] = unified_fields_cid
        run_record["unified_field_count"] = len(unified_fields)

        # ---------- Step 4: KG Cypher ----------
        db_data = build_db_data_from_field_units(domain_field_units)
        kg_agent = KnowledgeGraphAgent()

        domain_field_desc_map = {}
        domain_unified_map = {}

        for domain_entry in run_record["domains"]:
            db_name = domain_entry["db_name"]

            desc_artifact = ipfs.cat_json(domain_entry["field_descriptions_cid"])
            domain_field_desc_map[db_name] = desc_artifact.get("field_descriptions", [])

            if domain_entry.get("domain_unified_cid"):
                domain_unified_map[db_name] = ipfs.cat_json(domain_entry["domain_unified_cid"])
            else:
                domain_unified_map[db_name] = []

        cypher_list = kg_agent.generate_cypher(
            run_record=run_record,
            db_data=db_data,
            domain_field_desc_map=domain_field_desc_map,
            domain_unified_map=domain_unified_map,
            unified_fields=unified_fields,
        )

        cypher_file = f"cypher_{timestamp}.json"
        save_json(cypher_list, cypher_file)
        cypher_cid = ipfs.add_json(cypher_list)
        run_record["cypher_file"] = cypher_file
        run_record["cypher_cid"] = cypher_cid
        run_record["cypher_count"] = len(cypher_list)

        # ---------- Step 5: local registry ----------
        run_record["status"] = "completed"
        try:
            _persist_run_record(run_record, timestamp)
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] failed to persist run record: {exc}")

        print("\nrun summary:")
        pprint(run_record)
        print(f"generated cypher count: {len(cypher_list)}")
    except Exception as exc:  # noqa: BLE001
        run_record["status"] = "failed"
        run_record["error"] = str(exc)
        try:
            _persist_run_record(run_record, timestamp)
        except Exception as persist_exc:  # noqa: BLE001
            print(f"[warn] failed to persist failed run record: {persist_exc}")
        raise


def run_pipeline() -> None:
    run_all()


if __name__ == "__main__":
    run_all()
