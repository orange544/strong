from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from ..adapters import isresmat_adapter, unicorn_adapter, valentine_coma, valentine_simflooding
from ..adapters.common import MethodPrediction, PairSample, discover_valentine_pairs, load_ground_truth_pairs
from ..evaluation.metrics import evaluate_predictions
from ..utils.io import read_yaml


def load_configs(config_dir: Path) -> dict[str, Any]:
    default_cfg = read_yaml(config_dir / "default.yaml")
    datasets_cfg = read_yaml(config_dir / "datasets.yaml")
    methods_cfg = read_yaml(config_dir / "methods.yaml")
    return {
        "default": default_cfg,
        "datasets": datasets_cfg,
        "methods": methods_cfg,
    }


def build_single_pair(
    *,
    dataset_name: str,
    pair_id: str,
    source: Path,
    target: Path,
    ground_truth: Path,
) -> PairSample:
    return PairSample(
        dataset_name=dataset_name,
        pair_id=pair_id,
        source_path=source,
        target_path=target,
        ground_truth_path=ground_truth,
        source_table_name=source.stem,
        target_table_name=target.stem,
        relatedness=None,
    )


def load_dataset_pairs(config: dict[str, Any], dataset_name: str, limit: int | None = None) -> list[PairSample]:
    datasets_cfg = config["datasets"]
    dataset_root = Path(str(datasets_cfg.get("dataset_root", "")))
    pairs = discover_valentine_pairs(dataset_root, dataset_name)
    if limit is not None:
        return pairs[: max(0, limit)]
    return pairs


def run_method(method_name: str, pair: PairSample, config: dict[str, Any]) -> MethodPrediction:
    methods_cfg = config["methods"].get("methods", {})
    method_cfg = methods_cfg.get(method_name, {})
    if method_name == "coma":
        return valentine_coma.run(pair, method_cfg)
    if method_name == "simflooding":
        return valentine_simflooding.run(pair, method_cfg)
    if method_name == "isresmat":
        return isresmat_adapter.run(pair, method_cfg)
    if method_name == "unicorn":
        return unicorn_adapter.run(pair, method_cfg)
    return MethodPrediction(
        method=method_name,
        status="not_available",
        error_message=f"unknown method: {method_name}",
    )


def evaluate_pair_result(
    *,
    pair: PairSample,
    method_prediction: MethodPrediction,
    one_to_one: bool,
    top_k: int | None,
    top_percent: float | None,
) -> dict[str, Any]:
    gt = load_ground_truth_pairs(pair.ground_truth_path)
    for item in gt:
        if item.get("source_table") == "__unknown__":
            item["source_table"] = pair.source_table_name
        if item.get("target_table") == "__unknown__":
            item["target_table"] = pair.target_table_name

    if method_prediction.status != "ok":
        return {
            "method": method_prediction.method,
            "dataset": pair.dataset_name,
            "pair_id": pair.pair_id,
            "precision": None,
            "recall": None,
            "f1": None,
            "mrr": None,
            "recall_at_gt": None,
            "runtime_sec": method_prediction.runtime_sec,
            "data_loading_sec": method_prediction.data_loading_sec,
            "inference_sec": method_prediction.inference_sec,
            "status": method_prediction.status,
            "error_message": method_prediction.error_message,
            "supports_ranking": method_prediction.supports_ranking,
            "notes": "",
        }

    metrics = evaluate_predictions(
        predictions=method_prediction.predictions,
        ground_truth=gt,
        one_to_one=one_to_one,
        supports_ranking=method_prediction.supports_ranking,
        top_k=top_k,
        top_percent=top_percent,
    )
    return {
        "method": method_prediction.method,
        "dataset": pair.dataset_name,
        "pair_id": pair.pair_id,
        "precision": metrics["Precision"],
        "recall": metrics["Recall"],
        "f1": metrics["F1"],
        "mrr": metrics["MRR"],
        "recall_at_gt": metrics["RecallAtGT"],
        "runtime_sec": method_prediction.runtime_sec,
        "data_loading_sec": method_prediction.data_loading_sec,
        "inference_sec": method_prediction.inference_sec,
        "status": method_prediction.status,
        "error_message": method_prediction.error_message,
        "supports_ranking": method_prediction.supports_ranking,
        "notes": "",
    }
