from __future__ import annotations

import os
import time
import traceback
from pathlib import Path
from typing import Any

from ..methods.ours.ours_aggregator import aggregate_column_matches
from ..methods.ours.ours_describer import describe_sampled_columns
from ..methods.ours.ours_llm_describer import (
    LLMColumnDescriptionAgent,
    LLMRuntimeConfig,
    describe_sampled_columns_with_llm,
)
from ..methods.ours.ours_sampler import sample_table_columns
from ..utils.io import write_json
from ..utils.pandas_utils import read_csv_safe
from .common import MethodPrediction, PairSample


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists() or not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = raw_value.strip()
        if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
            value = value[1:-1]
        values[key] = value
    return values


def _first_non_empty(*values: str) -> str:
    for value in values:
        text = str(value).strip()
        if text:
            return text
    return ""


def _resolve_llm_runtime(description_cfg: dict[str, Any]) -> LLMRuntimeConfig:
    llm_cfg_obj = description_cfg.get("llm", {})
    llm_cfg = llm_cfg_obj if isinstance(llm_cfg_obj, dict) else {}

    dotenv_values: dict[str, str] = {}
    dotenv_path_raw = str(llm_cfg.get("dotenv_path", "")).strip()
    if dotenv_path_raw:
        dotenv_values = _read_dotenv(Path(dotenv_path_raw))

    def from_env_or_dotenv(key: str) -> str:
        env_value = str(os.getenv(key, "")).strip()
        if env_value:
            return env_value
        return str(dotenv_values.get(key, "")).strip()

    api_key = _first_non_empty(
        str(llm_cfg.get("api_key", "")),
        from_env_or_dotenv("LLM_DESC_API_KEY"),
        from_env_or_dotenv("LLM_API_KEY"),
    )
    base_url = _first_non_empty(
        str(llm_cfg.get("base_url", "")),
        from_env_or_dotenv("LLM_DESC_BASE_URL"),
        from_env_or_dotenv("LLM_BASE_URL"),
        "http://127.0.0.1:1234/v1",
    )
    model_name = _first_non_empty(
        str(llm_cfg.get("model_name", "")),
        from_env_or_dotenv("LLM_DESC_MODEL_NAME"),
        from_env_or_dotenv("LLM_MODEL_NAME"),
        "qwen3.5-4b",
    )

    timeout_default = _as_float(
        _first_non_empty(
            str(llm_cfg.get("timeout_sec", "")),
            from_env_or_dotenv("LLM_REQUEST_TIMEOUT_SEC"),
            "45",
        ),
        45.0,
    )
    max_retries_default = _as_int(
        _first_non_empty(
            str(llm_cfg.get("max_retries", "")),
            from_env_or_dotenv("LLM_MAX_RETRIES"),
            "1",
        ),
        1,
    )
    max_workers = _as_int(llm_cfg.get("max_workers", 1), 1)
    domain_timeout_sec = _as_int(llm_cfg.get("domain_timeout_sec", 900), 900)

    timeout_sec: float | None = timeout_default if timeout_default > 0 else None
    return LLMRuntimeConfig(
        api_key=api_key,
        base_url=base_url,
        model_name=model_name,
        temperature=max(0.0, _as_float(llm_cfg.get("temperature", 0.2), 0.2)),
        timeout_sec=timeout_sec,
        max_retries=max(0, max_retries_default),
        max_workers=max(1, max_workers),
        domain_timeout_sec=max(1, domain_timeout_sec),
    )


def _write_intermediate_artifacts(
    *,
    output_root: Path,
    pair: PairSample,
    source_samples: list[dict[str, Any]],
    target_samples: list[dict[str, Any]],
    source_desc: list[dict[str, Any]],
    target_desc: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
) -> Path:
    pair_dir = output_root / pair.dataset_name / pair.pair_id
    write_json(pair_dir / "samples_source.json", source_samples)
    write_json(pair_dir / "samples_target.json", target_samples)
    write_json(pair_dir / "descriptions_source.json", source_desc)
    write_json(pair_dir / "descriptions_target.json", target_desc)
    write_json(pair_dir / "predictions.json", predictions)
    return pair_dir


def run(pair: PairSample, method_cfg: dict[str, Any]) -> MethodPrediction:
    start = time.perf_counter()
    try:
        enabled = _as_bool(method_cfg.get("enabled", True), True)
        if not enabled:
            return MethodPrediction(
                method=str(method_cfg.get("name", "ours_sda")),
                status="not_available",
                runtime_sec=time.perf_counter() - start,
                error_message="ours_sda adapter is disabled in config.",
            )

        load_begin = time.perf_counter()
        source_df = read_csv_safe(pair.source_path)
        target_df = read_csv_safe(pair.target_path)
        load_sec = time.perf_counter() - load_begin

        infer_begin = time.perf_counter()

        seed = _as_int(method_cfg.get("seed", 20260409), 20260409)
        sampling_cfg = method_cfg.get("sampling", {})
        description_cfg = method_cfg.get("description", {})
        aggregation_cfg = method_cfg.get("aggregation", {})

        source_samples = sample_table_columns(
            source_df,
            pair.source_table_name,
            seed=seed,
            max_samples_per_column=_as_int(sampling_cfg.get("max_samples_per_column", 32), 32),
            min_non_empty_samples=_as_int(sampling_cfg.get("min_non_empty_samples", 0), 0),
            distinct_values=_as_bool(sampling_cfg.get("distinct_values", True), True),
            shuffle_values=_as_bool(sampling_cfg.get("shuffle_values", True), True),
        )
        target_samples = sample_table_columns(
            target_df,
            pair.target_table_name,
            seed=seed + 1,
            max_samples_per_column=_as_int(sampling_cfg.get("max_samples_per_column", 32), 32),
            min_non_empty_samples=_as_int(sampling_cfg.get("min_non_empty_samples", 0), 0),
            distinct_values=_as_bool(sampling_cfg.get("distinct_values", True), True),
            shuffle_values=_as_bool(sampling_cfg.get("shuffle_values", True), True),
        )

        description_mode = str(description_cfg.get("mode", "llm")).strip().lower()
        max_keywords = _as_int(description_cfg.get("max_keywords", 8), 8)
        if description_mode == "rule":
            source_desc = describe_sampled_columns(source_samples, max_keywords=max(1, max_keywords))
            target_desc = describe_sampled_columns(target_samples, max_keywords=max(1, max_keywords))
        elif description_mode == "llm":
            llm_runtime = _resolve_llm_runtime(description_cfg)
            llm_agent = LLMColumnDescriptionAgent(llm_runtime)
            source_desc = describe_sampled_columns_with_llm(
                sampled_columns=source_samples,
                agent=llm_agent,
                max_workers=llm_runtime.max_workers,
                domain_timeout_sec=llm_runtime.domain_timeout_sec,
            )
            target_desc = describe_sampled_columns_with_llm(
                sampled_columns=target_samples,
                agent=llm_agent,
                max_workers=llm_runtime.max_workers,
                domain_timeout_sec=llm_runtime.domain_timeout_sec,
            )
            fail_open_to_rule = _as_bool(description_cfg.get("fail_open_to_rule", False), False)
            source_failed = all(
                str(item.get("description", "")).strip() == "generation_failed"
                for item in source_desc
            )
            target_failed = all(
                str(item.get("description", "")).strip() == "generation_failed"
                for item in target_desc
            )
            if fail_open_to_rule and source_failed and target_failed:
                source_desc = describe_sampled_columns(source_samples, max_keywords=max(1, max_keywords))
                target_desc = describe_sampled_columns(target_samples, max_keywords=max(1, max_keywords))
                description_mode = "llm_fallback_rule"
        else:
            raise RuntimeError(f"Unsupported description mode: {description_mode}")

        weights = aggregation_cfg.get("weights", {})
        predictions = aggregate_column_matches(
            source_samples=source_samples,
            target_samples=target_samples,
            source_descriptions=source_desc,
            target_descriptions=target_desc,
            weights={
                "name": float(weights.get("name", 0.45)),
                "values": float(weights.get("values", 0.30)),
                "description": float(weights.get("description", 0.20)),
                "type": float(weights.get("type", 0.05)),
            },
            min_score=float(aggregation_cfg.get("min_score", 0.0)),
            top_k_per_source_column=(
                None
                if aggregation_cfg.get("top_k_per_source_column", None) is None
                else _as_int(aggregation_cfg.get("top_k_per_source_column"), 0)
            ),
        )

        infer_sec = time.perf_counter() - infer_begin
        metadata: dict[str, Any] = {
            "rows": {"source": int(len(source_df)), "target": int(len(target_df))},
            "columns": {"source": int(len(source_df.columns)), "target": int(len(target_df.columns))},
            "sampled_columns": {"source": len(source_samples), "target": len(target_samples)},
            "prediction_count": len(predictions),
            "seed": seed,
            "description_mode": description_mode,
        }
        llm_models = {
            str(item.get("llm_model", "")).strip()
            for item in [*source_desc, *target_desc]
            if str(item.get("llm_model", "")).strip()
        }
        if llm_models:
            metadata["llm_models"] = sorted(llm_models)

        outputs_cfg = method_cfg.get("outputs", {})
        write_intermediate = _as_bool(outputs_cfg.get("write_intermediate", False), False)
        if write_intermediate:
            output_root = Path(
                str(
                    outputs_cfg.get(
                        "intermediate_root",
                        "outputs/intermediate/ours_sda",
                    )
                )
            )
            pair_dir = _write_intermediate_artifacts(
                output_root=output_root,
                pair=pair,
                source_samples=source_samples,
                target_samples=target_samples,
                source_desc=source_desc,
                target_desc=target_desc,
                predictions=predictions,
            )
            metadata["intermediate_dir"] = str(pair_dir)

        return MethodPrediction(
            method=str(method_cfg.get("name", "ours_sda")),
            status="ok",
            predictions=predictions,
            supports_ranking=True,
            runtime_sec=time.perf_counter() - start,
            data_loading_sec=load_sec,
            inference_sec=infer_sec,
            metadata=metadata,
        )
    except Exception as exc:  # pragma: no cover - runtime dependent
        return MethodPrediction(
            method=str(method_cfg.get("name", "ours_sda")),
            status="error",
            runtime_sec=time.perf_counter() - start,
            error_message=f"{type(exc).__name__}: {exc}",
            metadata={"traceback": traceback.format_exc()},
        )
