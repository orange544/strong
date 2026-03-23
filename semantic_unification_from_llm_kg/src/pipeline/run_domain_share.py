from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.configs.config import (
    DB_PATHS,
    DOMAIN_SHARE_DEFAULTS,
    LLM_DESC_CONFIG,
    PIPELINE_CONFIG,
)
from src.db.database_agent import get_all_fields
from src.db.plugin_registry import (
    DatabasePluginRegistry,
    DatabaseSource,
    load_db_sources_from_env,
)
from src.llm.description_agent import FieldDescriptionAgent
from src.pipeline.orchestration_common import (
    DESCRIPTION_FAILED,
)
from src.pipeline.orchestration_common import (
    generate_descriptions_parallel as _generate_descriptions_parallel,
)
from src.pipeline.orchestration_common import (
    safe_db_tag as _safe_db_tag,
)
from src.pipeline.unified_interface import (
    extract_field_units_by_source,
    field_units_to_sample_records,
)
from src.storage.ipfs_client import IPFSClient
from src.storage.registry import append_run_record
from src.utils.io import save_json

if TYPE_CHECKING:
    from src.db.database_agent import DatabaseAgent


@dataclass
class DomainShareConfig:
    ipfs_chain_bin: Path
    go_norn_root: Path | None
    receiver: str
    rpc_addr: str
    ipfs_api: str
    timeout_sec: int
    strict: bool
    skip_chain: bool
    selected_domains: list[str]
    max_fields_per_domain: int
    mock_llm: bool


def _slugify(value: str) -> str:
    slug = _safe_db_tag(value)
    if slug != "domain":
        return slug[:48]
    short = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
    return f"domain_{short}"


def _quote_sqlite_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _load_runtime_db_sources() -> dict[str, DatabaseSource]:
    return load_db_sources_from_env(legacy_db_paths=DB_PATHS)


def _resolve_sqlite_path(dsn: str) -> Path:
    path = Path(dsn)
    if not path.is_absolute():
        path = (Path(__file__).resolve().parents[2] / path).resolve()
    return path


def _create_agent_for_source(
    registry: DatabasePluginRegistry,
    source: DatabaseSource,
) -> DatabaseAgent:
    if not source.dsn.strip():
        raise RuntimeError(f"Database source '{source.name}' has an empty DSN")

    if source.driver.strip().lower() == "sqlite":
        resolved = _resolve_sqlite_path(source.dsn.strip())
        if not resolved.is_file():
            raise RuntimeError(
                f"Database source '{source.name}' points to missing sqlite file: {resolved}"
            )

    try:
        return registry.create_agent(source)
    except KeyError as exc:
        supported = ", ".join(registry.supported_drivers()) or "<none>"
        raise RuntimeError(
            f"Unsupported database driver '{source.driver}' for source '{source.name}'. "
            f"Supported drivers: {supported}"
        ) from exc


def _sample_fields_for_domain(agent: DatabaseAgent, max_fields: int) -> list[dict[str, Any]]:
    if max_fields <= 0:
        return get_all_fields(agent)

    if not hasattr(agent, "conn") or not callable(getattr(agent, "sample_field", None)):
        return get_all_fields(agent)[:max_fields]

    # Keep original sample_field logic, but stop early for quick debug runs.
    try:
        cursor = agent.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [t[0] for t in cursor.fetchall()]
    except Exception:  # noqa: BLE001
        return get_all_fields(agent)[:max_fields]

    all_samples: list[dict[str, Any]] = []
    sample_field_fn = agent.sample_field
    for table in tables:
        if not isinstance(table, str) or not table:
            continue

        table_ident = _quote_sqlite_identifier(table)
        cursor.execute(f"PRAGMA table_info({table_ident})")
        columns = [c[1] for c in cursor.fetchall()]
        for field in columns:
            sample = sample_field_fn(table, field)
            if not isinstance(sample, dict):
                raise RuntimeError(f"sample_field for {table}.{field} must return an object")
            samples_obj = sample.get("samples", [])
            if not isinstance(samples_obj, list):
                raise RuntimeError(f"sample_field for {table}.{field} returned non-list 'samples'")
            if sample["samples"]:
                all_samples.append(sample)
            if len(all_samples) >= max_fields:
                return all_samples
    return all_samples


def _ensure_ipfs_chain_binary(binary_path: Path, go_norn_root: Path | None) -> None:
    if binary_path.exists():
        if binary_path.is_file():
            return
        raise RuntimeError(f"ipfs-chain path exists but is not a file: {binary_path}")

    if not go_norn_root:
        raise RuntimeError(
            f"ipfs-chain binary not found at {binary_path}, provide --go-norn-root"
        )
    if not go_norn_root.is_dir():
        raise RuntimeError(f"go-norn root is not a directory: {go_norn_root}")

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


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _make_ipfs_client(api_url: str) -> IPFSClient:
    api = api_url.rstrip("/")
    if not api.endswith("/api/v0"):
        api = api + "/api/v0"
    return IPFSClient(api_url=api)


def _mock_generate_description(field_json: dict[str, Any]) -> dict[str, str]:
    field = str(field_json.get("field", "unknown_field"))
    table = str(field_json.get("table", "unknown_table"))
    sample_count = len(field_json.get("samples", []))
    description = f"{field} in {table}, inferred from {sample_count} sample values."
    return {
        "table": table,
        "field": field,
        "description": description,
    }


def _coerce_sample_records(payload: Any) -> list[dict[str, Any]]:
    records_obj: Any = payload
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


def _generate_domain_descriptions(
    *,
    sample_payload: Any,
    mock_llm: bool,
    fd_agent: FieldDescriptionAgent | None,
) -> list[dict[str, Any]]:
    sample_records = _coerce_sample_records(sample_payload)

    class _DescriptionAdapter:
        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            if mock_llm:
                return _mock_generate_description(sample)
            if fd_agent is None:
                raise RuntimeError("FieldDescriptionAgent is not initialized")
            return fd_agent.generate_description(sample)

    return _generate_descriptions_parallel(
        fd_agent=_DescriptionAdapter(),
        samples=sample_records,
        max_workers=PIPELINE_CONFIG["llm_desc_max_workers"],
        domain_timeout_sec=PIPELINE_CONFIG["llm_desc_domain_timeout_sec"],
    )


def run_domain_share(cfg: DomainShareConfig) -> dict[str, Any]:
    db_sources = _load_runtime_db_sources()
    if not db_sources:
        raise RuntimeError("No database sources configured. Set DB_SOURCES_JSON or DB_PATHS.")

    domain_field_units = extract_field_units_by_source(
        db_sources,
        max_fields_per_domain=cfg.max_fields_per_domain,
    )

    if not cfg.skip_chain:
        _ensure_ipfs_chain_binary(cfg.ipfs_chain_bin, cfg.go_norn_root)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ipfs = _make_ipfs_client(cfg.ipfs_api)
    fd_agent = None
    if not cfg.mock_llm:
        fd_agent = FieldDescriptionAgent(
            api_key=LLM_DESC_CONFIG["api_key"],
            base_url=LLM_DESC_CONFIG["base_url"],
            model_name=LLM_DESC_CONFIG["model_name"],
        )

    manifest: dict[str, Any] = {
        "timestamp": timestamp,
        "mode": "domain_sample_description_to_ipfs_and_chain",
        "llm_model": LLM_DESC_CONFIG["model_name"],
        "domains": [],
    }

    selected = set(cfg.selected_domains)
    domain_items = list(db_sources.items())
    if selected:
        domain_items = [(k, v) for k, v in domain_items if k in selected]
        if not domain_items:
            raise RuntimeError(
                f"No domains matched --domain values: {sorted(selected)}. "
                f"Available: {sorted(db_sources.keys())}"
            )

    strict_error: Exception | None = None
    for domain_name, source in domain_items:
        domain_slug = _slugify(domain_name)
        run_id = f"{domain_slug}_{timestamp}"

        item: dict[str, Any] = {
            "domain": domain_name,
            "domain_slug": domain_slug,
            "db_path": source.dsn,
            "db_driver": source.driver,
            "status": "started",
            "failed_stage": "",
            "error_context": {},
            "sample_chain_key": "",
            "sample_chain_cid": "",
            "sample_tx_hash": "",
            "description_chain_key": "",
            "description_chain_cid": "",
            "description_tx_hash": "",
        }
        print(f"\n[Domain] {domain_name} [{source.driver}] ({source.dsn})")

        try:
            # 1) Sample by domain.
            samples = field_units_to_sample_records(domain_field_units[domain_name])

            samples_file = Path(
                save_json(samples, f"samples_{domain_slug}_{timestamp}.json")
            )
            item["samples_file"] = str(samples_file.resolve())
            item["samples_count"] = len(samples)
            item["samples_sha256"] = _file_sha256(samples_file.resolve())

            # 2) Upload sample file to IPFS.
            sample_cid = ipfs.add_file(str(samples_file.resolve()))
            item["samples_cid"] = sample_cid
            print(f"[IPFS] sample CID = {sample_cid}")

            # 3) Put sample CID on chain.
            sample_chain_cid = ""
            sample_tx_hash = ""
            sample_key = f"REGISTER_SAMPLE:{run_id}"
            item["sample_chain_key"] = sample_key
            if not cfg.skip_chain:
                try:
                    sample_chain_cid, sample_tx_hash = _put_file_on_chain(
                        ipfs_chain_bin=cfg.ipfs_chain_bin,
                        receiver=cfg.receiver,
                        key=sample_key,
                        file_path=samples_file.resolve(),
                        rpc_addr=cfg.rpc_addr,
                        ipfs_api=cfg.ipfs_api,
                        timeout_sec=cfg.timeout_sec,
                    )
                except Exception as exc:  # noqa: BLE001
                    item["failed_stage"] = "sample_chain"
                    item["error_context"] = {
                        "stage": "sample_chain",
                        "chain_key": sample_key,
                        "file_path": str(samples_file.resolve()),
                        "timeout_sec": cfg.timeout_sec,
                    }
                    raise RuntimeError(
                        f"sample chain registration failed for domain '{domain_name}'"
                    ) from exc
                item["sample_chain_cid"] = sample_chain_cid
                item["sample_tx_hash"] = sample_tx_hash
                print(f"[Chain] sample TxHash = {sample_tx_hash}")

            sample_source_cid = sample_chain_cid or sample_cid

            # 4) Domain LLM reads its own sample from IPFS and generates descriptions.
            samples_from_ipfs = ipfs.cat_json(sample_source_cid)
            field_descriptions = _generate_domain_descriptions(
                sample_payload=samples_from_ipfs,
                mock_llm=cfg.mock_llm,
                fd_agent=fd_agent,
            )
            for d in field_descriptions:
                d["db_name"] = domain_name
            item["description_failed_count"] = sum(
                1 for d in field_descriptions if d.get("description") == DESCRIPTION_FAILED
            )

            desc_file = Path(
                save_json(
                    field_descriptions,
                    f"field_descriptions_{domain_slug}_{timestamp}.json",
                )
            )
            item["field_descriptions_file"] = str(desc_file.resolve())
            item["field_descriptions_count"] = len(field_descriptions)
            item["field_descriptions_sha256"] = _file_sha256(desc_file.resolve())

            # 5) Upload descriptions to IPFS.
            desc_cid = ipfs.add_file(str(desc_file.resolve()))
            item["field_descriptions_cid"] = desc_cid
            print(f"[IPFS] field_descriptions CID = {desc_cid}")

            # 6) Put descriptions CID on chain.
            desc_key = f"REGISTER_DESCRIPTION:{run_id}"
            item["description_chain_key"] = desc_key
            if not cfg.skip_chain:
                try:
                    desc_chain_cid, desc_tx_hash = _put_file_on_chain(
                        ipfs_chain_bin=cfg.ipfs_chain_bin,
                        receiver=cfg.receiver,
                        key=desc_key,
                        file_path=desc_file.resolve(),
                        rpc_addr=cfg.rpc_addr,
                        ipfs_api=cfg.ipfs_api,
                        timeout_sec=cfg.timeout_sec,
                    )
                except Exception as exc:  # noqa: BLE001
                    item["failed_stage"] = "description_chain"
                    item["error_context"] = {
                        "stage": "description_chain",
                        "chain_key": desc_key,
                        "file_path": str(desc_file.resolve()),
                        "timeout_sec": cfg.timeout_sec,
                    }
                    raise RuntimeError(
                        f"description chain registration failed for domain '{domain_name}'"
                    ) from exc
                item["description_chain_cid"] = desc_chain_cid
                item["description_tx_hash"] = desc_tx_hash
                print(f"[Chain] description TxHash = {desc_tx_hash}")

            item["status"] = "completed"
        except Exception as exc:  # noqa: BLE001
            item["status"] = "failed"
            item["error"] = str(exc)
            print(f"[Domain] failed: {exc}")
            if cfg.strict:
                strict_error = exc
        finally:
            manifest["domains"].append(item)

        if strict_error is not None:
            break

    append_run_record(manifest)
    save_json(manifest, f"domain_share_manifest_{timestamp}.json")
    if strict_error is not None:
        raise strict_error
    return manifest


def parse_args() -> DomainShareConfig:
    parser = argparse.ArgumentParser(
        description=(
            "Per-domain workflow: sample DB -> upload sample to IPFS -> CID on chain -> "
            "LLM generates per-domain field_descriptions -> upload descriptions to IPFS -> CID on chain."
        )
    )
    parser.add_argument(
        "--ipfs-chain-bin",
        type=str,
        default=DOMAIN_SHARE_DEFAULTS["ipfs_chain_bin"],
    )
    parser.add_argument("--go-norn-root", type=str, default=DOMAIN_SHARE_DEFAULTS["go_norn_root"])
    parser.add_argument(
        "--receiver",
        type=str,
        default=DOMAIN_SHARE_DEFAULTS["receiver"],
    )
    parser.add_argument("--rpc", type=str, default=DOMAIN_SHARE_DEFAULTS["rpc_addr"])
    parser.add_argument("--ipfs-api", type=str, default=DOMAIN_SHARE_DEFAULTS["ipfs_api"])
    parser.add_argument("--timeout", type=int, default=DOMAIN_SHARE_DEFAULTS["timeout_sec"])
    parser.add_argument("--strict", action="store_true")
    parser.add_argument(
        "--domain",
        action="append",
        default=[],
        help="Run only selected domain name(s) from configured DB sources.",
    )
    parser.add_argument(
        "--max-fields-per-domain",
        type=int,
        default=0,
        help="Limit sampled fields per domain for quick testing (0 means no limit).",
    )
    parser.add_argument(
        "--skip-chain",
        action="store_true",
        help="Skip chain registration (IPFS + LLM only).",
    )
    parser.add_argument(
        "--mock-llm",
        action="store_true",
        help="Use local mock descriptions instead of calling real LLM (for pipeline debugging).",
    )
    args = parser.parse_args()

    go_norn_root = Path(args.go_norn_root) if args.go_norn_root else None
    return DomainShareConfig(
        ipfs_chain_bin=Path(args.ipfs_chain_bin),
        go_norn_root=go_norn_root,
        receiver=args.receiver,
        rpc_addr=args.rpc,
        ipfs_api=args.ipfs_api,
        timeout_sec=max(3, args.timeout),
        strict=args.strict,
        skip_chain=args.skip_chain,
        selected_domains=args.domain,
        max_fields_per_domain=max(0, args.max_fields_per_domain),
        mock_llm=args.mock_llm,
    )


def main() -> None:
    cfg = parse_args()
    result = run_domain_share(cfg)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
