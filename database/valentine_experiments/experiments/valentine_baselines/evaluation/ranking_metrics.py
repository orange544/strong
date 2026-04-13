from __future__ import annotations

from typing import Iterable


def recall_at_ground_truth_size(
    ranked_pairs: list[tuple[str, str, str, str]],
    ground_truth: set[tuple[str, str, str, str]],
) -> float:
    if not ground_truth:
        return 0.0
    top_n = ranked_pairs[: len(ground_truth)]
    tp = sum(1 for pair in top_n if pair in ground_truth)
    return tp / len(ground_truth)


def mean_reciprocal_rank(
    ranked_pairs: list[tuple[str, str, str, str]],
    ground_truth: set[tuple[str, str, str, str]],
) -> float:
    if not ground_truth:
        return 0.0

    rank_by_pair: dict[tuple[str, str, str, str], int] = {}
    for idx, pair in enumerate(ranked_pairs, start=1):
        if pair not in rank_by_pair:
            rank_by_pair[pair] = idx

    rr_values: list[float] = []
    for gt in ground_truth:
        rank = rank_by_pair.get(gt)
        rr_values.append(0.0 if rank is None else (1.0 / rank))
    return sum(rr_values) / len(rr_values)


def top_k_recall(
    ranked_pairs: list[tuple[str, str, str, str]],
    ground_truth: set[tuple[str, str, str, str]],
    k: int,
) -> float:
    if not ground_truth:
        return 0.0
    k = max(0, int(k))
    top_k = ranked_pairs[:k]
    tp = sum(1 for pair in top_k if pair in ground_truth)
    return tp / len(ground_truth)


def iter_unique_pairs(
    ranked_pairs: Iterable[tuple[str, str, str, str]],
) -> list[tuple[str, str, str, str]]:
    seen: set[tuple[str, str, str, str]] = set()
    out: list[tuple[str, str, str, str]] = []
    for pair in ranked_pairs:
        if pair in seen:
            continue
        seen.add(pair)
        out.append(pair)
    return out

