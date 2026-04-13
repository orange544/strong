from __future__ import annotations

import re
from typing import Any

from ...adapters.common import prediction_row

_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")


def _tokenize(text: str) -> set[str]:
    return {token.lower() for token in _TOKEN_PATTERN.findall(text) if token}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def _type_similarity(left_type: str, right_type: str) -> float:
    left = left_type.strip().lower()
    right = right_type.strip().lower()
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    if "numeric" in {left, right} and "mixed" in {left, right}:
        return 0.5
    if "text" in {left, right} and "mixed" in {left, right}:
        return 0.5
    return 0.0


def _build_desc_index(descriptions: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in descriptions:
        table_name = str(row.get("table_name", ""))
        column_name = str(row.get("column_name", ""))
        index[(table_name, column_name)] = row
    return index


def _tokens_from_values(values: list[str], *, token_budget: int = 80) -> set[str]:
    out: list[str] = []
    for value in values:
        for token in _TOKEN_PATTERN.findall(value):
            out.append(token.lower())
            if len(out) >= token_budget:
                return set(out)
    return set(out)


def _tokens_from_description(row: dict[str, Any]) -> set[str]:
    tags = [str(item) for item in row.get("tags", [])]
    keywords = [str(item) for item in row.get("keywords", [])]
    description = str(row.get("description", ""))
    return set(tags) | set(keywords) | _tokenize(description)


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for key in ("name", "values", "description", "type"):
        raw = float(weights.get(key, 0.0))
        normalized[key] = max(0.0, raw)
    total = sum(normalized.values())
    if total <= 0.0:
        return {"name": 1.0, "values": 0.0, "description": 0.0, "type": 0.0}
    return {key: value / total for key, value in normalized.items()}


def aggregate_column_matches(
    *,
    source_samples: list[dict[str, Any]],
    target_samples: list[dict[str, Any]],
    source_descriptions: list[dict[str, Any]],
    target_descriptions: list[dict[str, Any]],
    weights: dict[str, float],
    min_score: float,
    top_k_per_source_column: int | None,
) -> list[dict[str, Any]]:
    normalized_weights = _normalize_weights(weights)
    source_desc_index = _build_desc_index(source_descriptions)
    target_desc_index = _build_desc_index(target_descriptions)

    scored: list[dict[str, Any]] = []
    for source in source_samples:
        src_table = str(source.get("table_name", ""))
        src_col = str(source.get("column_name", ""))
        src_key = (src_table, src_col)
        src_desc = source_desc_index.get(src_key, {})
        src_name_tokens = _tokenize(src_col)
        src_value_tokens = _tokens_from_values([str(v) for v in source.get("sample_values", [])])
        src_desc_tokens = _tokens_from_description(src_desc)
        src_type = str(src_desc.get("value_type", "unknown"))

        local_scores: list[dict[str, Any]] = []
        for target in target_samples:
            tgt_table = str(target.get("table_name", ""))
            tgt_col = str(target.get("column_name", ""))
            tgt_key = (tgt_table, tgt_col)
            tgt_desc = target_desc_index.get(tgt_key, {})
            tgt_name_tokens = _tokenize(tgt_col)
            tgt_value_tokens = _tokens_from_values([str(v) for v in target.get("sample_values", [])])
            tgt_desc_tokens = _tokens_from_description(tgt_desc)
            tgt_type = str(tgt_desc.get("value_type", "unknown"))

            name_score = _jaccard(src_name_tokens, tgt_name_tokens)
            value_score = _jaccard(src_value_tokens, tgt_value_tokens)
            desc_score = _jaccard(src_desc_tokens, tgt_desc_tokens)
            type_score = _type_similarity(src_type, tgt_type)

            score = (
                normalized_weights["name"] * name_score
                + normalized_weights["values"] * value_score
                + normalized_weights["description"] * desc_score
                + normalized_weights["type"] * type_score
            )
            if score < min_score:
                continue
            local_scores.append(
                prediction_row(
                    src_table,
                    src_col,
                    tgt_table,
                    tgt_col,
                    score,
                )
            )

        local_scores.sort(key=lambda item: float(item["score"]), reverse=True)
        if top_k_per_source_column is not None and top_k_per_source_column >= 0:
            local_scores = local_scores[:top_k_per_source_column]
        scored.extend(local_scores)

    scored.sort(key=lambda item: float(item["score"]), reverse=True)
    return scored

