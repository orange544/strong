from __future__ import annotations

import hashlib
import random
from typing import Any

import pandas as pd

_EMPTY_TOKENS = {"", "nan", "none", "null", "na", "n/a"}


def _normalize_value(raw: object) -> str | None:
    text = str(raw).strip()
    if text.lower() in _EMPTY_TOKENS:
        return None
    return text


def _dedupe_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _stable_seed(base_seed: int, *parts: str) -> int:
    token = "::".join([str(base_seed), *parts])
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def sample_table_columns(
    df: pd.DataFrame,
    table_name: str,
    *,
    seed: int,
    max_samples_per_column: int,
    min_non_empty_samples: int,
    distinct_values: bool,
    shuffle_values: bool,
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    max_keep = max(0, int(max_samples_per_column))
    min_keep = max(0, int(min_non_empty_samples))

    for index, raw_column_name in enumerate(df.columns):
        column_name = str(raw_column_name).strip() or f"column_{index + 1}"
        series = df[raw_column_name]
        normalized = [_normalize_value(value) for value in series.tolist()]
        values = [value for value in normalized if value is not None]

        if distinct_values:
            values = _dedupe_keep_order(values)

        if shuffle_values and values:
            rng = random.Random(_stable_seed(seed, table_name, column_name))
            rng.shuffle(values)

        if max_keep > 0:
            sampled_values = values[:max_keep]
        else:
            sampled_values = values

        samples.append(
            {
                "table_name": table_name,
                "column_name": column_name,
                "dtype": str(series.dtype),
                "non_empty_count": len(values),
                "unique_non_empty_count": len(_dedupe_keep_order(values)),
                "sampled_count": len(sampled_values),
                "min_non_empty_samples": min_keep,
                "insufficient_samples": len(sampled_values) < min_keep,
                "sample_values": sampled_values,
            }
        )
    return samples

