from __future__ import annotations

import math
from typing import Any

from .ranking_metrics import iter_unique_pairs, mean_reciprocal_rank, recall_at_ground_truth_size


PairKey = tuple[str, str, str, str]


def _pair_key(
    source_table: str,
    source_column: str,
    target_table: str,
    target_column: str,
) -> PairKey:
    return (
        source_table.strip(),
        source_column.strip(),
        target_table.strip(),
        target_column.strip(),
    )


def _prediction_to_pair(pred: dict[str, Any]) -> PairKey:
    return _pair_key(
        str(pred.get("source_table", "")),
        str(pred.get("source_column", "")),
        str(pred.get("target_table", "")),
        str(pred.get("target_column", "")),
    )


def _score_of(pred: dict[str, Any]) -> float:
    raw = pred.get("score", 1.0)
    try:
        return float(raw)
    except Exception:
        return 1.0


def _apply_one_to_one(sorted_predictions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    used_source: set[tuple[str, str]] = set()
    used_target: set[tuple[str, str]] = set()
    selected: list[dict[str, Any]] = []
    for pred in sorted_predictions:
        src = (str(pred.get("source_table", "")), str(pred.get("source_column", "")))
        tgt = (str(pred.get("target_table", "")), str(pred.get("target_column", "")))
        if src in used_source or tgt in used_target:
            continue
        used_source.add(src)
        used_target.add(tgt)
        selected.append(pred)
    return selected


def evaluate_predictions(
    *,
    predictions: list[dict[str, Any]],
    ground_truth: list[dict[str, str]],
    one_to_one: bool = True,
    supports_ranking: bool = False,
    top_k: int | None = None,
    top_percent: float | None = None,
) -> dict[str, Any]:
    gt_set: set[PairKey] = {
        _pair_key(
            str(item.get("source_table", "")),
            str(item.get("source_column", "")),
            str(item.get("target_table", "")),
            str(item.get("target_column", "")),
        )
        for item in ground_truth
    }

    dedupe: dict[PairKey, dict[str, Any]] = {}
    for pred in predictions:
        key = _prediction_to_pair(pred)
        score = _score_of(pred)
        if key not in dedupe or score > _score_of(dedupe[key]):
            dedupe[key] = {
                "source_table": key[0],
                "source_column": key[1],
                "target_table": key[2],
                "target_column": key[3],
                "score": score,
            }

    ranked_predictions = sorted(dedupe.values(), key=_score_of, reverse=True)
    if top_percent is not None:
        percent = max(0.0, min(100.0, float(top_percent)))
        keep = int(math.ceil(len(ranked_predictions) * percent / 100.0))
        ranked_predictions = ranked_predictions[:keep]
    if top_k is not None:
        ranked_predictions = ranked_predictions[: max(0, int(top_k))]

    selected_predictions = _apply_one_to_one(ranked_predictions) if one_to_one else ranked_predictions
    pred_set: set[PairKey] = {_prediction_to_pair(pred) for pred in selected_predictions}

    tp = len(pred_set & gt_set)
    fp = len(pred_set - gt_set)
    fn = len(gt_set - pred_set)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0

    ranked_pairs = iter_unique_pairs(_prediction_to_pair(pred) for pred in ranked_predictions)
    metrics: dict[str, Any] = {
        "Precision": precision,
        "Recall": recall,
        "F1": f1,
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "num_predictions_after_filter": len(pred_set),
        "num_ground_truth": len(gt_set),
        "supports_ranking": supports_ranking,
        "MRR": None,
        "RecallAtGT": None,
    }

    if supports_ranking and ranked_pairs:
        metrics["MRR"] = mean_reciprocal_rank(ranked_pairs, gt_set)
        metrics["RecallAtGT"] = recall_at_ground_truth_size(ranked_pairs, gt_set)
    return metrics

