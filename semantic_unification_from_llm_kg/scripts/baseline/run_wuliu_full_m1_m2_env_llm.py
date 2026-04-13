from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.configs.config import LLM_DESC_CONFIG, LLM_UNIFY_CONFIG
from src.db.plugin_registry import DatabaseSource
from src.distributed.contracts.models import BatchRequest
from src.distributed.local_agent_service import LocalDomainAgentService
from src.distributed.orchestrator_service import GlobalOrchestratorService
from src.storage.ipfs_client import IPFSClient


@dataclass(frozen=True)
class RunConfig:
    max_fields_per_source: int
    poll_timeout_sec: float
    poll_interval_sec: float


def _save_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _now_token() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _build_sources() -> tuple[DatabaseSource, DatabaseSource]:
    source_m1 = DatabaseSource(
        name="m1_mysql",
        driver="mysql",
        dsn="mysql://root:123456@127.0.0.1:3306/wuliu_m1_mysql_db",
        options={},
    )
    source_m2 = DatabaseSource(
        name="m2_mysql",
        driver="mysql",
        dsn="mysql://root:123456@127.0.0.1:3306/wuliu_m2_mysql_db",
        options={},
    )
    return source_m1, source_m2


def _llm_snapshot() -> dict[str, object]:
    return {
        "desc_base_url": LLM_DESC_CONFIG.get("base_url", ""),
        "desc_model": LLM_DESC_CONFIG.get("model_name", ""),
        "desc_api_key_present": bool(LLM_DESC_CONFIG.get("api_key", "")),
        "unify_base_url": LLM_UNIFY_CONFIG.get("base_url", ""),
        "unify_model": LLM_UNIFY_CONFIG.get("model_name", ""),
        "unify_api_key_present": bool(LLM_UNIFY_CONFIG.get("api_key", "")),
    }


def _export_artifacts(
    *,
    report_dir: Path,
    manifest_dict: dict[str, Any],
    ipfs: IPFSClient,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    exported_domain: list[dict[str, object]] = []
    exported_global: list[dict[str, object]] = []

    node_manifests_obj = manifest_dict.get("node_manifests", [])
    if isinstance(node_manifests_obj, list):
        for node_manifest in node_manifests_obj:
            if not isinstance(node_manifest, dict):
                continue
            domain_manifests_obj = node_manifest.get("domain_manifests", [])
            if not isinstance(domain_manifests_obj, list):
                continue
            for domain_manifest in domain_manifests_obj:
                if not isinstance(domain_manifest, dict):
                    continue
                domain_id_obj = domain_manifest.get("domain_id")
                domain_id = domain_id_obj if isinstance(domain_id_obj, str) else ""
                artifacts_obj = domain_manifest.get("artifacts", [])
                if not isinstance(artifacts_obj, list):
                    continue
                for artifact in artifacts_obj:
                    if not isinstance(artifact, dict):
                        continue
                    artifact_type_obj = artifact.get("type")
                    cid_obj = artifact.get("cid")
                    if not isinstance(artifact_type_obj, str) or not isinstance(cid_obj, str):
                        continue
                    artifact_type = artifact_type_obj.strip()
                    cid = cid_obj.strip()
                    if not artifact_type or not cid:
                        continue

                    payload = ipfs.cat_json(cid)
                    file_name = f"{domain_id}__{artifact_type}.json"
                    _save_json(report_dir / file_name, payload)
                    exported_domain.append(
                        {
                            "domain_id": domain_id,
                            "type": artifact_type,
                            "cid": cid,
                            "record_count": artifact.get("record_count", 0),
                            "sha256": artifact.get("sha256", ""),
                            "file": file_name,
                        }
                    )

    artifacts_obj = manifest_dict.get("artifacts", [])
    if isinstance(artifacts_obj, list):
        for artifact in artifacts_obj:
            if not isinstance(artifact, dict):
                continue
            artifact_type_obj = artifact.get("type")
            cid_obj = artifact.get("cid")
            if not isinstance(artifact_type_obj, str) or not isinstance(cid_obj, str):
                continue
            artifact_type = artifact_type_obj.strip()
            cid = cid_obj.strip()
            if not artifact_type or not cid:
                continue

            payload = ipfs.cat_json(cid)
            file_name = f"global__{artifact_type}.json"
            _save_json(report_dir / file_name, payload)
            exported_global.append(
                {
                    "type": artifact_type,
                    "cid": cid,
                    "record_count": artifact.get("record_count", 0),
                    "sha256": artifact.get("sha256", ""),
                    "file": file_name,
                }
            )

    return exported_domain, exported_global


def run(config: RunConfig) -> dict[str, object]:
    run_id = f"wuliu_full_m1m2_envllm_{_now_token()}"
    report_dir = Path("tmp") / "report" / run_id
    report_dir.mkdir(parents=True, exist_ok=True)

    source_m1, source_m2 = _build_sources()
    agent_m1 = LocalDomainAgentService(
        node_id="node_m1",
        source_loader=lambda: {"m1_mysql": source_m1},
    )
    agent_m2 = LocalDomainAgentService(
        node_id="node_m2",
        source_loader=lambda: {"m2_mysql": source_m2},
    )

    orchestrator = GlobalOrchestratorService(enable_chain_registration=False)
    orchestrator.register_domain_client("m1_mysql", agent_m1)
    orchestrator.register_domain_client("m2_mysql", agent_m2)

    request = BatchRequest(
        run_id=run_id,
        domain_ids=("m1_mysql", "m2_mysql"),
        max_fields_per_source=config.max_fields_per_source,
        mock_llm=False,
        share_mode="include_samples",
        poll_interval_sec=config.poll_interval_sec,
        poll_timeout_sec=config.poll_timeout_sec,
    )
    batch_id = orchestrator.create_batch(request)

    status_history: list[dict[str, object]] = []
    start_time = time.time()
    while True:
        status = orchestrator.get_batch_status(batch_id)
        status_history.append({"ts": datetime.now().isoformat(), **status})
        if status["status"] in {"succeeded", "failed"}:
            break
        if time.time() - start_time > config.poll_timeout_sec + 30:
            raise RuntimeError("runner polling timeout reached")
        time.sleep(1.0)

    _save_json(report_dir / "batch_status_history.json", status_history)
    _save_json(report_dir / "batch_status.json", status)

    summary: dict[str, object] = {
        "run_id": run_id,
        "batch_id": batch_id,
        "status": status["status"],
        "report_dir": str(report_dir),
        "request": asdict(config),
        "mock_llm": False,
        "share_mode": "include_samples",
        "llm_config_snapshot": _llm_snapshot(),
        "domain_ids": ["m1_mysql", "m2_mysql"],
    }

    if status["status"] != "succeeded":
        summary["error_message"] = status.get("error_message", "")
        _save_json(report_dir / "run_error.json", summary)
        _save_json(report_dir / "run_summary.json", summary)
        return summary

    manifest = orchestrator.get_batch_manifest(batch_id)
    manifest_dict = manifest.to_dict()
    _save_json(report_dir / "global_manifest.json", manifest_dict)

    ipfs = IPFSClient()
    exported_domain, exported_global = _export_artifacts(
        report_dir=report_dir,
        manifest_dict=manifest_dict,
        ipfs=ipfs,
    )

    summary["domain_artifact_count"] = len(exported_domain)
    summary["global_artifact_count"] = len(exported_global)
    summary["domain_artifacts"] = exported_domain
    summary["global_artifacts"] = exported_global
    _save_json(report_dir / "run_summary.json", summary)
    return summary


def parse_args() -> RunConfig:
    parser = argparse.ArgumentParser(
        description="Run full pipeline test for wuliu m1_mysql + m2_mysql with LLM strictly from .env",
    )
    parser.add_argument(
        "--max-fields-per-source",
        type=int,
        default=0,
        help="0 means no field limit.",
    )
    parser.add_argument(
        "--poll-timeout-sec",
        type=float,
        default=900.0,
        help="Distributed batch polling timeout.",
    )
    parser.add_argument(
        "--poll-interval-sec",
        type=float,
        default=0.5,
        help="Polling interval in seconds.",
    )
    args = parser.parse_args()
    return RunConfig(
        max_fields_per_source=max(0, args.max_fields_per_source),
        poll_timeout_sec=max(10.0, args.poll_timeout_sec),
        poll_interval_sec=max(0.1, args.poll_interval_sec),
    )


def main() -> None:
    cfg = parse_args()
    result = run(cfg)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
