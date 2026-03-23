from __future__ import annotations

import re
import subprocess
from datetime import datetime
from pathlib import Path
from pprint import pprint
from typing import TYPE_CHECKING, Any

from src.configs.config import (
    DB_PATHS,
    DOMAIN_SHARE_DEFAULTS,
    LLM_DESC_CONFIG,
    LLM_UNIFY_CONFIG,
    PIPELINE_CONFIG,
)
from src.db.plugin_registry import (
    DatabasePluginRegistry,
    DatabaseSource,
    load_db_sources_from_env,
)
from src.db.unified.preflight import run_preflight_checks
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


def _domain_share_required_str(key: str) -> str:
    raw = DOMAIN_SHARE_DEFAULTS.get(key)
    if not isinstance(raw, str) or not raw.strip():
        raise RuntimeError(f"DOMAIN_SHARE_DEFAULTS['{key}'] must be a non-empty string")
    return raw.strip()


def _domain_share_optional_str(key: str) -> str | None:
    raw = DOMAIN_SHARE_DEFAULTS.get(key)
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise RuntimeError(f"DOMAIN_SHARE_DEFAULTS['{key}'] must be a string when provided")
    value = raw.strip()
    if not value:
        return None
    return value


def _domain_share_timeout_sec() -> int:
    raw = DOMAIN_SHARE_DEFAULTS.get("timeout_sec")
    if isinstance(raw, int):
        return max(3, raw)
    if isinstance(raw, str):
        try:
            return max(3, int(raw.strip()))
        except ValueError as exc:
            raise RuntimeError("DOMAIN_SHARE_DEFAULTS['timeout_sec'] must be an integer") from exc
    raise RuntimeError("DOMAIN_SHARE_DEFAULTS['timeout_sec'] must be an integer")


def _pipeline_bool(key: str, default: bool) -> bool:
    raw = PIPELINE_CONFIG.get(key, default)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        return raw != 0
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise RuntimeError(f"PIPELINE_CONFIG['{key}'] must be a bool-like value")


def _pipeline_positive_float(key: str, default: float) -> float:
    raw = PIPELINE_CONFIG.get(key, default)
    if isinstance(raw, int | float):
        value = float(raw)
    elif isinstance(raw, str):
        try:
            value = float(raw.strip())
        except ValueError as exc:
            raise RuntimeError(f"PIPELINE_CONFIG['{key}'] must be a float") from exc
    else:
        raise RuntimeError(f"PIPELINE_CONFIG['{key}'] must be a float")

    if value <= 0:
        raise RuntimeError(f"PIPELINE_CONFIG['{key}'] must be positive")
    return value


def _ensure_ipfs_chain_binary(binary_path: Path, go_norn_root: Path | None) -> None:
    if binary_path.exists():
        if binary_path.is_file():
            return
        raise RuntimeError(f"ipfs-chain path exists but is not a file: {binary_path}")
    if not go_norn_root:
        raise RuntimeError(
            f"ipfs-chain binary not found at {binary_path}, provide GO_NORN_ROOT"
        )
    if not go_norn_root.is_dir():
        raise RuntimeError(f"GO_NORN_ROOT is not a directory: {go_norn_root}")
    binary_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["go", "build", "-o", str(binary_path), "./cmd/ipfs-chain"]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(go_norn_root),
            capture_output=True,
            text=True,
            timeout=180,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("go tool not found while building ipfs-chain") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("building ipfs-chain timed out after 180s") from exc
    if proc.returncode != 0:
        raise RuntimeError(
            "failed to build ipfs-chain\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    if not binary_path.is_file():
        raise RuntimeError(
            f"ipfs-chain build reported success but binary is missing: {binary_path}"
        )


def _put_file_on_chain(
    *,
    ipfs_chain_bin: Path,
    receiver: str,
    key: str,
    file_path: Path,
    rpc_addr: str,
    ipfs_api: str,
    timeout_sec: int,
) -> tuple[str, str]:
    cmd = [
        str(ipfs_chain_bin),
        "put",
        "-receiver",
        receiver,
        "-key",
        key,
        "-file",
        str(file_path),
        "-rpc",
        rpc_addr,
        "-ipfs",
        ipfs_api,
        "-timeout",
        str(timeout_sec),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(3, timeout_sec + 2),
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"ipfs-chain put timed out for key={key} after {timeout_sec}s") from exc

    if proc.returncode != 0:
        raise RuntimeError(
            f"ipfs-chain put failed for key={key}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )

    cid_match = re.search(r"(?im)^\s*cid\s*:\s*(\S+)\s*$", proc.stdout)
    tx_hash_match = re.search(r"(?im)^\s*txhash\s*:\s*(\S+)\s*$", proc.stdout)

    if not cid_match or not tx_hash_match:
        raise RuntimeError(f"failed to parse CID/TxHash from output:\n{proc.stdout}")
    return cid_match.group(1), tx_hash_match.group(1)


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


def _new_registry() -> DatabasePluginRegistry:
    return DatabasePluginRegistry()


def _create_db_agents(
    db_sources: dict[str, DatabaseSource],
    registry: DatabasePluginRegistry,
) -> dict[str, DatabaseAgent]:
    db_agents: dict[str, DatabaseAgent] = {}
    try:
        for db_name, source in db_sources.items():
            try:
                db_agents[db_name] = registry.create_agent(source)
            except KeyError as exc:
                supported = ", ".join(registry.supported_drivers()) or "<none>"
                raise RuntimeError(
                    f"Unsupported database driver '{source.driver}' for source '{db_name}'. "
                    f"Supported drivers: {supported}"
                ) from exc
        return db_agents
    except Exception:
        for agent in db_agents.values():
            agent.close()
        raise


def run_all() -> None:
    db_sources = _load_runtime_db_sources()
    if not db_sources:
        raise RuntimeError("No database sources configured. Set DB_SOURCES_JSON or DB_PATHS.")

    preflight_enabled = _pipeline_bool("run_preflight_enabled", True)
    if preflight_enabled:
        preflight_check_sqlite = _pipeline_bool("run_preflight_check_sqlite_path", False)
        preflight_check_tcp = _pipeline_bool("run_preflight_check_tcp", False)
        preflight_tcp_timeout_sec = _pipeline_positive_float(
            "run_preflight_tcp_timeout_sec",
            2.0,
        )
        run_preflight_checks(
            db_sources,
            check_sqlite_path=preflight_check_sqlite,
            check_tcp=preflight_check_tcp,
            tcp_timeout_sec=preflight_tcp_timeout_sec,
        )
        print(
            "[Preflight] passed "
            f"(sqlite_path={preflight_check_sqlite}, tcp={preflight_check_tcp})"
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

    ipfs_chain_bin = Path(_domain_share_required_str("ipfs_chain_bin"))
    go_norn_root_value = _domain_share_optional_str("go_norn_root")
    go_norn_root = Path(go_norn_root_value) if go_norn_root_value else None
    chain_receiver = _domain_share_required_str("receiver")
    chain_rpc_addr = _domain_share_required_str("rpc_addr")
    chain_ipfs_api = _domain_share_required_str("ipfs_api")
    chain_timeout_sec = _domain_share_timeout_sec()
    _ensure_ipfs_chain_binary(ipfs_chain_bin, go_norn_root)

    run_record: dict[str, Any] = {
        "timestamp": timestamp,
        "mode": "federated_multi_domain_pipeline",
        "databases": list(db_sources.keys()),
        "llm_desc_model": LLM_DESC_CONFIG["model_name"],
        "llm_unify_model": LLM_UNIFY_CONFIG["model_name"],
        "domains": [],
    }

    try:
        # =========================================================
        # Step 1: per-domain sampling -> IPFS -> chain
        # =========================================================
        for db_name in db_sources:
            db_tag = _safe_db_tag(db_name)
            print(f"sampling database: {db_name}")
            samples = field_units_to_sample_records(domain_field_units[db_name])

            sample_artifact = _build_sample_artifact(db_name, timestamp, samples)
            sample_filename = f"samples_{db_tag}_{timestamp}.json"
            sample_saved_path = save_json(sample_artifact, sample_filename)
            sample_file_path = Path(sample_saved_path).resolve()

            sample_cid = ipfs.add_file(str(sample_file_path))
            print(f"[IPFS] sample CID = {sample_cid}")

            sample_chain_key = f"REGISTER_SAMPLE:{db_tag}_{timestamp}"
            sample_chain_cid, sample_tx_hash = _put_file_on_chain(
                ipfs_chain_bin=ipfs_chain_bin,
                receiver=chain_receiver,
                key=sample_chain_key,
                file_path=sample_file_path,
                rpc_addr=chain_rpc_addr,
                ipfs_api=chain_ipfs_api,
                timeout_sec=chain_timeout_sec,
            )
            print(f"[CHAIN] sample TxHash = {sample_tx_hash}")

            run_record["domains"].append(
                {
                    "db_name": db_name,
                    "sample_file": sample_filename,
                    "samples_cid": sample_cid,
                    "sample_chain_key": sample_chain_key,
                    "sample_chain_cid": sample_chain_cid,
                    "sample_tx_hash": sample_tx_hash,
                    "sampled_field_count": sample_artifact["summary"]["sampled_field_count"],
                    "total_sample_value_count": sample_artifact["summary"]["total_sample_value_count"],
                }
            )

        # =========================================================
        # Step 2: per-domain descriptions -> IPFS -> chain
        # =========================================================
        fd_agent = FieldDescriptionAgent(
            api_key=LLM_DESC_CONFIG["api_key"],
            base_url=LLM_DESC_CONFIG["base_url"],
            model_name=LLM_DESC_CONFIG["model_name"],
        )
        print(
            f"generating descriptions with workers={max_workers}, domain_timeout={domain_timeout_sec}s"
        )

        for domain_entry in run_record["domains"]:
            db_name = domain_entry["db_name"]
            db_tag = _safe_db_tag(db_name)
            sample_source_cid = domain_entry.get("sample_chain_cid") or domain_entry["samples_cid"]
            sample_artifact = ipfs.cat_json(sample_source_cid)
            domain_samples = sample_artifact.get("samples", sample_artifact)

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
                    "source_samples_cid": sample_source_cid,
                },
                "field_descriptions": field_descriptions,
            }

            desc_filename = f"field_descriptions_{db_tag}_{timestamp}.json"
            desc_saved_path = save_json(desc_artifact, desc_filename)
            desc_file_path = Path(desc_saved_path).resolve()

            desc_cid = ipfs.add_file(str(desc_file_path))
            print(f"[IPFS] description CID = {desc_cid}")

            desc_chain_key = f"REGISTER_DESCRIPTION:{db_tag}_{timestamp}"
            desc_chain_cid, desc_tx_hash = _put_file_on_chain(
                ipfs_chain_bin=ipfs_chain_bin,
                receiver=chain_receiver,
                key=desc_chain_key,
                file_path=desc_file_path,
                rpc_addr=chain_rpc_addr,
                ipfs_api=chain_ipfs_api,
                timeout_sec=chain_timeout_sec,
            )
            print(f"[CHAIN] description TxHash = {desc_tx_hash}")

            domain_entry["field_descriptions_file"] = desc_filename
            domain_entry["field_descriptions_cid"] = desc_cid
            domain_entry["description_chain_key"] = desc_chain_key
            domain_entry["description_chain_cid"] = desc_chain_cid
            domain_entry["description_tx_hash"] = desc_tx_hash
            domain_entry["description_count"] = len(field_descriptions)

        # =========================================================
        # Step 3: two-stage semantic unification
        # =========================================================
        fs_agent = FieldSemanticAgent(
            api_key=LLM_UNIFY_CONFIG["api_key"],
            base_url=LLM_UNIFY_CONFIG["base_url"],
            model_name=LLM_UNIFY_CONFIG["model_name"],
        )

        domain_level_items: list[dict[str, Any]] = []
        for domain_entry in run_record["domains"]:
            db_name = domain_entry["db_name"]
            desc_source_cid = (
                domain_entry.get("description_chain_cid")
                or domain_entry["field_descriptions_cid"]
            )
            desc_artifact = ipfs.cat_json(desc_source_cid)
            field_descriptions = desc_artifact.get("field_descriptions", [])

            tables = {item["table"] for item in field_descriptions if item.get("table")}
            if len(tables) <= 1:
                print(f"single-table domain {db_name}, skip within-domain unify")
                domain_unified = _wrap_single_table_fields_for_cross_domain(field_descriptions)
            else:
                print(f"within-domain unify {db_name}, fields={len(field_descriptions)}")
                domain_unified = fs_agent.unify_within_domain(field_descriptions)
                domain_unified = _attach_db_name_to_domain_unified(domain_unified, db_name)

            domain_unified_file = f"domain_unified_{_safe_db_tag(db_name)}_{timestamp}.json"
            domain_unified_saved_path = save_json(domain_unified, domain_unified_file)
            domain_unified_file_path = Path(domain_unified_saved_path).resolve()

            domain_unified_cid = ipfs.add_file(str(domain_unified_file_path))

            domain_entry["domain_unified_file"] = domain_unified_file
            domain_entry["domain_unified_cid"] = domain_unified_cid
            domain_entry["domain_unified_count"] = len(domain_unified)
            domain_level_items.extend(domain_unified)

        print(f"cross-domain unify candidates={len(domain_level_items)}")
        unified_fields = fs_agent.unify_across_domains(domain_level_items)

        uf_file = f"unified_fields_{timestamp}.json"
        uf_saved_path = save_json(unified_fields, uf_file)
        uf_file_path = Path(uf_saved_path).resolve()
        unified_fields_cid = ipfs.add_file(str(uf_file_path))
        run_record["unified_fields_file"] = uf_file
        run_record["unified_fields_cid"] = unified_fields_cid
        run_record["unified_field_count"] = len(unified_fields)

        # =========================================================
        # Step 4A: per-domain KG Cypher generation
        # =========================================================
        db_data = build_db_data_from_field_units(domain_field_units)
        kg_agent = KnowledgeGraphAgent()

        domain_field_desc_map: dict[str, list[dict[str, Any]]] = {}
        domain_unified_map: dict[str, list[dict[str, Any]]] = {}

        for domain_entry in run_record["domains"]:
            db_name = domain_entry["db_name"]
            desc_source_cid = (
                domain_entry.get("description_chain_cid")
                or domain_entry["field_descriptions_cid"]
            )
            desc_artifact = ipfs.cat_json(desc_source_cid)
            domain_field_desc_map[db_name] = desc_artifact.get("field_descriptions", [])
            domain_unified_map[db_name] = ipfs.cat_json(domain_entry["domain_unified_cid"])

        total_domain_kg_stmt_count = 0

        for domain_entry in run_record["domains"]:
            db_name = domain_entry["db_name"]
            db_tag = _safe_db_tag(db_name)

            domain_kg_cypher = kg_agent.generate_domain_kg_cypher(
                run_record=run_record,
                db_name=db_name,
                tables_data=db_data.get(db_name, {}),
                field_descs=domain_field_desc_map.get(db_name, []),
                domain_unified=domain_unified_map.get(db_name, []),
            )

            domain_kg_file = f"domain_kg_cypher_{db_tag}_{timestamp}.json"
            domain_kg_saved_path = save_json(domain_kg_cypher, domain_kg_file)
            domain_kg_file_path = Path(domain_kg_saved_path).resolve()

            domain_kg_cid = ipfs.add_file(str(domain_kg_file_path))
            print(f"[IPFS] domain kg CID = {domain_kg_cid}")

            domain_kg_chain_key = f"REGISTER_DOMAIN_KG:{db_tag}_{timestamp}"
            domain_kg_chain_cid, domain_kg_tx_hash = _put_file_on_chain(
                ipfs_chain_bin=ipfs_chain_bin,
                receiver=chain_receiver,
                key=domain_kg_chain_key,
                file_path=domain_kg_file_path,
                rpc_addr=chain_rpc_addr,
                ipfs_api=chain_ipfs_api,
                timeout_sec=chain_timeout_sec,
            )
            print(f"[CHAIN] domain kg TxHash = {domain_kg_tx_hash}")

            domain_entry["domain_kg_file"] = domain_kg_file
            domain_entry["domain_kg_cid"] = domain_kg_cid
            domain_entry["domain_kg_chain_key"] = domain_kg_chain_key
            domain_entry["domain_kg_chain_cid"] = domain_kg_chain_cid
            domain_entry["domain_kg_tx_hash"] = domain_kg_tx_hash
            domain_entry["domain_kg_stmt_count"] = len(domain_kg_cypher)

            total_domain_kg_stmt_count += len(domain_kg_cypher)

        # =========================================================
        # Step 4B: alignment index + alignment cypher
        # =========================================================
        alignment_index = kg_agent.generate_alignment_index(unified_fields)

        alignment_index_file = f"alignment_index_{timestamp}.json"
        alignment_index_saved_path = save_json(alignment_index, alignment_index_file)
        alignment_index_file_path = Path(alignment_index_saved_path).resolve()

        alignment_index_cid = ipfs.add_file(str(alignment_index_file_path))
        alignment_chain_key = f"REGISTER_ALIGNMENT_INDEX:{timestamp}"
        alignment_chain_cid, alignment_tx_hash = _put_file_on_chain(
            ipfs_chain_bin=ipfs_chain_bin,
            receiver=chain_receiver,
            key=alignment_chain_key,
            file_path=alignment_index_file_path,
            rpc_addr=chain_rpc_addr,
            ipfs_api=chain_ipfs_api,
            timeout_sec=chain_timeout_sec,
        )

        run_record["alignment_index_file"] = alignment_index_file
        run_record["alignment_index_cid"] = alignment_index_cid
        run_record["alignment_chain_key"] = alignment_chain_key
        run_record["alignment_chain_cid"] = alignment_chain_cid
        run_record["alignment_tx_hash"] = alignment_tx_hash
        run_record["alignment_count"] = len(alignment_index)

        alignment_cypher = kg_agent.generate_alignment_cypher(
            run_record=run_record,
            db_data=db_data,
            unified_fields=unified_fields,
            alignment_index=alignment_index,
        )

        alignment_cypher_file = f"alignment_cypher_{timestamp}.json"
        alignment_cypher_saved_path = save_json(alignment_cypher, alignment_cypher_file)
        alignment_cypher_file_path = Path(alignment_cypher_saved_path).resolve()

        alignment_cypher_cid = ipfs.add_file(str(alignment_cypher_file_path))
        alignment_cypher_chain_key = f"REGISTER_ALIGNMENT_CYPHER:{timestamp}"
        alignment_cypher_chain_cid, alignment_cypher_tx_hash = _put_file_on_chain(
            ipfs_chain_bin=ipfs_chain_bin,
            receiver=chain_receiver,
            key=alignment_cypher_chain_key,
            file_path=alignment_cypher_file_path,
            rpc_addr=chain_rpc_addr,
            ipfs_api=chain_ipfs_api,
            timeout_sec=chain_timeout_sec,
        )

        run_record["alignment_cypher_file"] = alignment_cypher_file
        run_record["alignment_cypher_cid"] = alignment_cypher_cid
        run_record["alignment_cypher_chain_key"] = alignment_cypher_chain_key
        run_record["alignment_cypher_chain_cid"] = alignment_cypher_chain_cid
        run_record["alignment_cypher_tx_hash"] = alignment_cypher_tx_hash
        run_record["alignment_cypher_count"] = len(alignment_cypher)

        run_record["domain_kg_total_stmt_count"] = total_domain_kg_stmt_count

        # =========================================================
        # Step 5: registry
        # =========================================================
        append_run_record(run_record)

        print("\nrun summary:")
        pprint(run_record)
        print(f"generated domain kg stmt count: {total_domain_kg_stmt_count}")
        print(f"generated alignment stmt count: {len(alignment_cypher)}")
    finally:
        # Unified adapters are stateless; keep explicit finally for structure parity.
        pass


def run_pipeline() -> None:
    run_all()


if __name__ == "__main__":
    run_all()
