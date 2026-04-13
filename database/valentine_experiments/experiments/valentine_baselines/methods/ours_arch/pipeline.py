from __future__ import annotations

import hashlib
import importlib
import os
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ...adapters.common import MethodPrediction, PairSample, prediction_row
from ...utils.io import read_yaml, write_json
from .dataset_csv_sampler import build_pair_csv_sampling_context


_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _as_int(value: object, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _as_bool(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _safe_stem(text: str) -> str:
    out = []
    for ch in text:
        if ch.isalnum() or ch == "_":
            out.append(ch)
        else:
            out.append("_")
    token = "".join(out).strip("_")
    return token or "run"


def _stable_seed(base_seed: int, *parts: str) -> int:
    token = "::".join([str(base_seed), *parts])
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _is_loopback_base_url(base_url: str) -> bool:
    url = base_url.strip()
    if not url:
        return False
    try:
        host = (urlparse(url).hostname or "").strip().lower()
    except Exception:
        return False
    return host in _LOOPBACK_HOSTS or host.startswith("127.")


def _merge_no_proxy(existing: str, additions: list[str]) -> str:
    merged: list[str] = []
    seen: set[str] = set()
    for raw in [*existing.split(","), *additions]:
        item = raw.strip()
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    return ",".join(merged)


def _ensure_local_desc_bypass_proxy(base_url: str) -> None:
    if not _is_loopback_base_url(base_url):
        return
    additions = ["127.0.0.1", "localhost", "::1"]
    for key in ("NO_PROXY", "no_proxy"):
        os.environ[key] = _merge_no_proxy(os.getenv(key, ""), additions)


@dataclass(frozen=True)
class OursArchConfig:
    method_name: str
    semantic_project_root: Path
    temp_db_root: Path
    intermediate_root: Path
    write_intermediate: bool
    preserve_temp_db: bool
    seed: int
    max_fields_per_domain: int
    llm_desc_max_workers: int
    llm_desc_domain_timeout_sec: int
    unify_desc_max_chars: int
    unify_retry_on_timeout: bool
    unify_retry_count: int
    unify_retry_desc_max_chars: int


def load_ours_arch_config(path: Path) -> dict[str, Any]:
    return read_yaml(path)


def resolve_ours_arch_config(payload: dict[str, Any]) -> OursArchConfig:
    method_cfg_obj = payload.get("method", {})
    method_cfg = method_cfg_obj if isinstance(method_cfg_obj, dict) else {}
    runtime_cfg_obj = payload.get("runtime", {})
    runtime_cfg = runtime_cfg_obj if isinstance(runtime_cfg_obj, dict) else {}

    semantic_root = Path(
        str(
            runtime_cfg.get(
                "semantic_project_root",
                "D:/Program Files/BISHE/program/semantic_unification_from_llm_kg",
            )
        )
    )
    temp_db_root = Path(
        str(
            runtime_cfg.get(
                "temp_db_root",
                "D:/Program Files/BISHE/program/database/valentine_experiments/experiments/valentine_baselines/outputs/tmp_sqlite",
            )
        )
    )
    intermediate_root = Path(
        str(
            runtime_cfg.get(
                "intermediate_root",
                "D:/Program Files/BISHE/program/database/valentine_experiments/experiments/valentine_baselines/outputs/intermediate/ours_arch",
            )
        )
    )

    return OursArchConfig(
        method_name=str(method_cfg.get("name", "ours_arch")).strip() or "ours_arch",
        semantic_project_root=semantic_root,
        temp_db_root=temp_db_root,
        intermediate_root=intermediate_root,
        write_intermediate=_as_bool(runtime_cfg.get("write_intermediate", True), True),
        preserve_temp_db=_as_bool(runtime_cfg.get("preserve_temp_db", False), False),
        seed=_as_int(runtime_cfg.get("seed", 20260409), 20260409),
        max_fields_per_domain=max(0, _as_int(runtime_cfg.get("max_fields_per_domain", 0), 0)),
        llm_desc_max_workers=max(1, _as_int(runtime_cfg.get("llm_desc_max_workers", 1), 1)),
        llm_desc_domain_timeout_sec=max(30, _as_int(runtime_cfg.get("llm_desc_domain_timeout_sec", 900), 900)),
        unify_desc_max_chars=max(32, _as_int(runtime_cfg.get("unify_desc_max_chars", 220), 220)),
        unify_retry_on_timeout=_as_bool(runtime_cfg.get("unify_retry_on_timeout", True), True),
        unify_retry_count=max(0, _as_int(runtime_cfg.get("unify_retry_count", 1), 1)),
        unify_retry_desc_max_chars=max(
            24,
            _as_int(runtime_cfg.get("unify_retry_desc_max_chars", 96), 96),
        ),
    )


@dataclass(frozen=True)
class OursArchEvalConfig:
    one_to_one: bool
    top_k: int | None
    top_percent: float | None


def resolve_eval_config(payload: dict[str, Any]) -> OursArchEvalConfig:
    eval_cfg_obj = payload.get("evaluation", {})
    eval_cfg = eval_cfg_obj if isinstance(eval_cfg_obj, dict) else {}

    top_k_raw = eval_cfg.get("top_k")
    top_percent_raw = eval_cfg.get("top_percent")
    top_k = None if top_k_raw in (None, "", "null") else _as_int(top_k_raw, 0)
    top_percent = None
    if top_percent_raw not in (None, "", "null"):
        try:
            top_percent = float(top_percent_raw)
        except Exception:
            top_percent = None
    return OursArchEvalConfig(
        one_to_one=_as_bool(eval_cfg.get("one_to_one", True), True),
        top_k=top_k,
        top_percent=top_percent,
    )


@dataclass(frozen=True)
class OursArchRunnerDefaults:
    default_datasets: list[str]


def resolve_runner_defaults(payload: dict[str, Any]) -> OursArchRunnerDefaults:
    runner_cfg_obj = payload.get("runner", {})
    runner_cfg = runner_cfg_obj if isinstance(runner_cfg_obj, dict) else {}
    items_obj = runner_cfg.get("default_datasets", [])
    if not isinstance(items_obj, list):
        items_obj = []
    datasets = [str(item).strip() for item in items_obj if str(item).strip()]
    return OursArchRunnerDefaults(default_datasets=datasets)


@dataclass(frozen=True)
class _SemanticModules:
    FieldDescriptionAgent: Any
    FieldSemanticAgent: Any
    KnowledgeGraphAgent: Any
    generate_descriptions_parallel: Any
    wrap_single_table_fields_for_cross_domain: Any
    attach_db_name_to_domain_unified: Any
    llm_desc_config: dict[str, Any]
    llm_unify_config: dict[str, Any]
    db_sample_ratio: float
    db_sample_min: int
    db_sample_max: int


def _load_semantic_modules(semantic_project_root: Path) -> _SemanticModules:
    project_root = semantic_project_root.resolve()
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

    description_agent_mod = importlib.import_module("src.llm.description_agent")
    semantic_agent_mod = importlib.import_module("src.llm.semantic")
    orchestration_common = importlib.import_module("src.pipeline.orchestration_common")
    kg_mod = importlib.import_module("src.kg.kg_agent")
    config_mod = importlib.import_module("src.configs.config")
    llm_desc_config = dict(getattr(config_mod, "LLM_DESC_CONFIG"))
    llm_unify_config = dict(getattr(config_mod, "LLM_UNIFY_CONFIG"))
    _ensure_local_desc_bypass_proxy(str(llm_desc_config.get("base_url", "")))

    return _SemanticModules(
        FieldDescriptionAgent=getattr(description_agent_mod, "FieldDescriptionAgent"),
        FieldSemanticAgent=getattr(semantic_agent_mod, "FieldSemanticAgent"),
        KnowledgeGraphAgent=getattr(kg_mod, "KnowledgeGraphAgent"),
        generate_descriptions_parallel=getattr(orchestration_common, "generate_descriptions_parallel"),
        wrap_single_table_fields_for_cross_domain=getattr(
            orchestration_common,
            "wrap_single_table_fields_for_cross_domain",
        ),
        attach_db_name_to_domain_unified=getattr(
            orchestration_common,
            "attach_db_name_to_domain_unified",
        ),
        llm_desc_config=llm_desc_config,
        llm_unify_config=llm_unify_config,
        db_sample_ratio=float(getattr(config_mod, "DB_SAMPLE_RATIO", 0.02)),
        db_sample_min=int(getattr(config_mod, "DB_SAMPLE_MIN", 10)),
        db_sample_max=int(getattr(config_mod, "DB_SAMPLE_MAX", 20)),
    )


def _to_predictions(
    *,
    pair: PairSample,
    alignment_index: list[dict[str, Any]],
    source_domain_id: str,
    target_domain_id: str,
) -> list[dict[str, Any]]:
    dedupe: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for rel in alignment_index:
        src_db = str(rel.get("source_domain", ""))
        src_table = str(rel.get("source_table", ""))
        src_field = str(rel.get("source_field", ""))
        tgt_db = str(rel.get("target_domain", ""))
        tgt_table = str(rel.get("target_table", ""))
        tgt_field = str(rel.get("target_field", ""))
        raw_score = rel.get("score", 1.0)
        try:
            score = float(raw_score)
        except Exception:
            score = 1.0

        if src_db == source_domain_id and tgt_db == target_domain_id:
            left_table, left_field = src_table, src_field
            right_table, right_field = tgt_table, tgt_field
        elif src_db == target_domain_id and tgt_db == source_domain_id:
            left_table, left_field = tgt_table, tgt_field
            right_table, right_field = src_table, src_field
        else:
            continue

        normalized_left_table = left_table.strip() or pair.source_table_name
        normalized_right_table = right_table.strip() or pair.target_table_name
        key = (
            normalized_left_table,
            left_field.strip(),
            normalized_right_table,
            right_field.strip(),
        )
        if not key[1] or not key[3]:
            continue
        row = prediction_row(key[0], key[1], key[2], key[3], score)
        if key not in dedupe or float(dedupe[key]["score"]) < score:
            dedupe[key] = row

    rows = list(dedupe.values())
    rows.sort(key=lambda item: float(item["score"]), reverse=True)
    return rows


def _write_intermediate(
    *,
    config: OursArchConfig,
    pair: PairSample,
    payload: dict[str, Any],
) -> Path:
    pair_dir = (
        config.intermediate_root
        / _safe_stem(pair.dataset_name)
        / _safe_stem(pair.pair_id)
    )
    pair_dir.mkdir(parents=True, exist_ok=True)
    out_file = pair_dir / "trace.json"
    write_json(out_file, payload)
    return out_file


def _compact_text(text: str, *, max_chars: int) -> str:
    normalized = " ".join(str(text).split())
    if not normalized:
        return ""
    # Keep one concise sentence-like fragment for unify prompt size control.
    for sep in (". ", "; ", "。", "\n"):
        if sep in normalized:
            normalized = normalized.split(sep, 1)[0].strip()
            break
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max(1, max_chars - 1)].rstrip() + "…"


def _fallback_description(item: dict[str, Any]) -> str:
    canonical = str(item.get("canonical_name", "")).strip()
    if canonical:
        return canonical
    field = str(item.get("field", "")).strip()
    if field:
        return field
    fields_obj = item.get("fields", [])
    if isinstance(fields_obj, list) and fields_obj:
        return str(fields_obj[0]).strip()
    return "semantic field"


def _compact_domain_items_for_unify(
    domain_items: list[dict[str, Any]],
    *,
    max_chars: int,
) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for item in domain_items:
        copied = dict(item)
        raw_desc = str(copied.get("description", "")).strip()
        if not raw_desc or raw_desc.lower() == "generation_failed":
            raw_desc = _fallback_description(copied)
        copied["description"] = _compact_text(raw_desc, max_chars=max_chars)
        compacted.append(copied)
    return compacted


def _unify_across_domains_with_retry(
    *,
    fs_agent: Any,
    domain_items: list[dict[str, Any]],
    desc_max_chars: int,
    retry_on_timeout: bool,
    retry_count: int,
    retry_desc_max_chars: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    attempts = 0
    used_retry = False
    last_desc_chars = desc_max_chars
    compact_items = _compact_domain_items_for_unify(domain_items, max_chars=desc_max_chars)

    while True:
        attempts += 1
        try:
            result = fs_agent.unify_across_domains(compact_items) if compact_items else []
            return result, {
                "attempts": attempts,
                "used_retry": used_retry,
                "desc_max_chars": last_desc_chars,
            }
        except Exception as exc:  # noqa: BLE001
            timeout_like = "timed out" in str(exc).lower()
            can_retry = retry_on_timeout and timeout_like and attempts <= retry_count
            if not can_retry:
                raise
            used_retry = True
            last_desc_chars = max(24, retry_desc_max_chars)
            compact_items = _compact_domain_items_for_unify(domain_items, max_chars=last_desc_chars)


def run_pair_with_main_architecture(
    *,
    pair: PairSample,
    config: OursArchConfig,
) -> MethodPrediction:
    start = time.perf_counter()
    try:
        modules = _load_semantic_modules(config.semantic_project_root)
        pair_seed = _stable_seed(config.seed, pair.dataset_name, pair.pair_id)

        context = build_pair_csv_sampling_context(
            pair=pair,
            seed=pair_seed,
            sample_ratio=modules.db_sample_ratio,
            sample_min=max(0, modules.db_sample_min),
            sample_max=max(0, modules.db_sample_max),
            max_fields_per_domain=config.max_fields_per_domain,
        )
        load_sec = time.perf_counter() - start

        infer_begin = time.perf_counter()
        domain_samples: dict[str, list[dict[str, Any]]] = {
            context.source_domain_id: context.source_samples,
            context.target_domain_id: context.target_samples,
        }

        fd_agent = modules.FieldDescriptionAgent(
            api_key=str(modules.llm_desc_config.get("api_key", "")),
            base_url=str(modules.llm_desc_config.get("base_url", "")),
            model_name=str(modules.llm_desc_config.get("model_name", "")),
        )
        domain_descriptions: dict[str, list[dict[str, Any]]] = {}
        for domain_id, samples in domain_samples.items():
            field_descriptions = modules.generate_descriptions_parallel(
                fd_agent=fd_agent,
                samples=samples,
                max_workers=config.llm_desc_max_workers,
                domain_timeout_sec=config.llm_desc_domain_timeout_sec,
            )
            for item in field_descriptions:
                item["db_name"] = domain_id
                item["description"] = _compact_text(
                    str(item.get("description", "")),
                    max_chars=config.unify_desc_max_chars,
                )
            domain_descriptions[domain_id] = field_descriptions

        fs_agent = modules.FieldSemanticAgent(
            api_key=str(modules.llm_unify_config.get("api_key", "")),
            base_url=str(modules.llm_unify_config.get("base_url", "")),
            model_name=str(modules.llm_unify_config.get("model_name", "")),
        )
        domain_unified_map: dict[str, list[dict[str, Any]]] = {}
        domain_level_items: list[dict[str, Any]] = []
        for domain_id, descriptions in domain_descriptions.items():
            tables = {str(item.get("table", "")).strip() for item in descriptions if item.get("table")}
            if len(tables) <= 1:
                domain_unified = modules.wrap_single_table_fields_for_cross_domain(descriptions)
            else:
                domain_unified = modules.attach_db_name_to_domain_unified(
                    fs_agent.unify_within_domain(descriptions),
                    domain_id,
                )
            domain_unified_map[domain_id] = domain_unified
            domain_level_items.extend(domain_unified)

        unified_fields, unify_runtime = _unify_across_domains_with_retry(
            fs_agent=fs_agent,
            domain_items=domain_level_items,
            desc_max_chars=config.unify_desc_max_chars,
            retry_on_timeout=config.unify_retry_on_timeout,
            retry_count=config.unify_retry_count,
            retry_desc_max_chars=config.unify_retry_desc_max_chars,
        )
        kg_agent = modules.KnowledgeGraphAgent()
        alignment_index = kg_agent.generate_alignment_index(unified_fields)

        predictions = _to_predictions(
            pair=pair,
            alignment_index=alignment_index,
            source_domain_id=context.source_domain_id,
            target_domain_id=context.target_domain_id,
        )
        infer_sec = time.perf_counter() - infer_begin

        metadata: dict[str, Any] = {
            "seed": pair_seed,
            "source_domain_id": context.source_domain_id,
            "target_domain_id": context.target_domain_id,
            "csv": {
                "source_csv": str(pair.source_path),
                "target_csv": str(pair.target_path),
                "source_rows": context.source_rows,
                "source_columns": context.source_columns,
                "target_rows": context.target_rows,
                "target_columns": context.target_columns,
                "source_has_duplicate_header_row": context.source_has_duplicate_header_row,
                "target_has_duplicate_header_row": context.target_has_duplicate_header_row,
            },
            "sampling": {
                "sample_ratio": modules.db_sample_ratio,
                "sample_min": modules.db_sample_min,
                "sample_max": modules.db_sample_max,
                "max_fields_per_domain": config.max_fields_per_domain,
            },
            "unify_runtime": unify_runtime,
            "llm_desc_model": str(modules.llm_desc_config.get("model_name", "")),
            "llm_unify_model": str(modules.llm_unify_config.get("model_name", "")),
            "sample_count_by_domain": {key: len(value) for key, value in domain_samples.items()},
            "description_count_by_domain": {key: len(value) for key, value in domain_descriptions.items()},
            "domain_unified_count_by_domain": {key: len(value) for key, value in domain_unified_map.items()},
            "cross_domain_unified_count": len(unified_fields),
            "alignment_count": len(alignment_index),
            "prediction_count": len(predictions),
        }

        if config.write_intermediate:
            trace_file = _write_intermediate(
                config=config,
                pair=pair,
                payload={
                    "pair": {
                        "dataset": pair.dataset_name,
                        "pair_id": pair.pair_id,
                        "source_csv": str(pair.source_path),
                        "target_csv": str(pair.target_path),
                        "ground_truth": str(pair.ground_truth_path),
                    },
                    "metadata": metadata,
                    "domain_samples": domain_samples,
                    "domain_descriptions": domain_descriptions,
                    "domain_unified_map": domain_unified_map,
                    "cross_domain_unified": unified_fields,
                    "alignment_index": alignment_index,
                    "predictions": predictions,
                },
            )
            metadata["trace_file"] = str(trace_file)

        return MethodPrediction(
            method=config.method_name,
            status="ok",
            predictions=predictions,
            supports_ranking=False,
            runtime_sec=time.perf_counter() - start,
            data_loading_sec=load_sec,
            inference_sec=infer_sec,
            metadata=metadata,
        )
    except Exception as exc:  # noqa: BLE001
        return MethodPrediction(
            method=config.method_name,
            status="error",
            runtime_sec=time.perf_counter() - start,
            error_message=f"{type(exc).__name__}: {exc}",
            metadata={"traceback": traceback.format_exc()},
        )
