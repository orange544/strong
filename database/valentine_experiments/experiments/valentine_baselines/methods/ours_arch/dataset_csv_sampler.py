from __future__ import annotations

import hashlib
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ...adapters.common import PairSample
from ...utils.pandas_utils import drop_duplicate_header_row

_EMPTY_TOKENS = {"", "nan", "none", "null", "na", "n/a"}
_FIELD_SAMPLE_SCHEMA_VERSION = "field-sample/1.0"
_PIPELINE_TARGET_VERSION = "2026.03.v1"


def _safe_token(text: str) -> str:
    token = re.sub(r"[^0-9A-Za-z_]+", "_", text).strip("_")
    return token or "pair"


def _stable_seed(base_seed: int, *parts: str) -> int:
    token = "::".join([str(base_seed), *parts])
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _normalize_columns(columns: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    normalized: list[str] = []
    for index, raw in enumerate(columns):
        base = str(raw).strip() or f"column_{index + 1}"
        count = seen.get(base, 0)
        seen[base] = count + 1
        if count == 0:
            normalized.append(base)
        else:
            normalized.append(f"{base}_{count + 1}")
    return normalized


def _read_csv_with_header_dedupe_flag(path: Path) -> tuple[pd.DataFrame, bool]:
    try:
        raw_df = pd.read_csv(path)
    except UnicodeDecodeError:
        raw_df = pd.read_csv(path, encoding="latin-1")

    has_duplicate_header_row = False
    if not raw_df.empty:
        first_row = [str(value).strip() for value in raw_df.iloc[0].tolist()]
        header = [str(column).strip() for column in raw_df.columns.tolist()]
        has_duplicate_header_row = first_row == header

    cleaned_df = drop_duplicate_header_row(raw_df).copy()
    cleaned_df.columns = _normalize_columns([str(col) for col in cleaned_df.columns])
    return cleaned_df, has_duplicate_header_row


def _normalize_sample_value(raw: object) -> str | None:
    text = str(raw).strip()
    if text.lower() in _EMPTY_TOKENS:
        return None
    return text


def _resolve_sample_count(
    *,
    non_empty_count: int,
    sample_ratio: float,
    sample_min: int,
    sample_max: int,
) -> int:
    if non_empty_count <= 0:
        return 0

    ratio_count = 0
    if sample_ratio > 0:
        ratio_count = int(round(non_empty_count * sample_ratio))
    target = max(sample_min, ratio_count)
    if target <= 0:
        target = min(non_empty_count, 1)
    if sample_max > 0:
        target = min(target, sample_max)
    return min(non_empty_count, target)


def _sample_values(values: list[str], *, sample_count: int, seed: int) -> list[str]:
    if sample_count <= 0 or not values:
        return []
    if sample_count >= len(values):
        return list(values)

    indexes = list(range(len(values)))
    rng = random.Random(seed)
    rng.shuffle(indexes)
    chosen = indexes[:sample_count]
    return [values[index] for index in chosen]


def _build_domain_samples(
    *,
    dataframe: pd.DataFrame,
    domain_id: str,
    table_name: str,
    seed: int,
    sample_ratio: float,
    sample_min: int,
    sample_max: int,
    max_fields_per_domain: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    column_names = [str(column).strip() for column in dataframe.columns.tolist()]

    for index, column_name in enumerate(column_names):
        if max_fields_per_domain > 0 and index >= max_fields_per_domain:
            break

        series = dataframe.iloc[:, index]
        normalized_values = [_normalize_sample_value(value) for value in series.tolist()]
        non_empty_values = [value for value in normalized_values if value is not None]
        sample_count = _resolve_sample_count(
            non_empty_count=len(non_empty_values),
            sample_ratio=sample_ratio,
            sample_min=sample_min,
            sample_max=sample_max,
        )
        sampled_values = _sample_values(
            non_empty_values,
            sample_count=sample_count,
            seed=_stable_seed(seed, domain_id, table_name, column_name),
        )
        logical_type = str(series.dtype)
        field_ref = f"{domain_id}.{table_name}.{column_name}"
        records.append(
            {
                "schema_version": _FIELD_SAMPLE_SCHEMA_VERSION,
                "pipeline_target_version": _PIPELINE_TARGET_VERSION,
                "source_name": domain_id,
                "database_type": "csv",
                "container_name": table_name,
                "field_path": column_name,
                "original_field": column_name,
                "field_origin": "column",
                "logical_type": logical_type,
                "samples": sampled_values,
                # Compatibility keys used by the main-architecture description agent.
                "db_name": domain_id,
                "table": table_name,
                "field": column_name,
                "type": logical_type,
                "field_ref": field_ref,
            }
        )

    return records


@dataclass(frozen=True)
class PairCSVSamplingContext:
    source_domain_id: str
    target_domain_id: str
    source_rows: int
    source_columns: int
    target_rows: int
    target_columns: int
    source_has_duplicate_header_row: bool
    target_has_duplicate_header_row: bool
    source_samples: list[dict[str, Any]]
    target_samples: list[dict[str, Any]]


def build_pair_csv_sampling_context(
    *,
    pair: PairSample,
    seed: int,
    sample_ratio: float,
    sample_min: int,
    sample_max: int,
    max_fields_per_domain: int,
) -> PairCSVSamplingContext:
    dataset_token = _safe_token(pair.dataset_name)
    pair_token = _safe_token(pair.pair_id)
    source_domain_id = f"{dataset_token}__{pair_token}__source"
    target_domain_id = f"{dataset_token}__{pair_token}__target"

    source_df, source_has_duplicate_header_row = _read_csv_with_header_dedupe_flag(pair.source_path)
    target_df, target_has_duplicate_header_row = _read_csv_with_header_dedupe_flag(pair.target_path)

    source_samples = _build_domain_samples(
        dataframe=source_df,
        domain_id=source_domain_id,
        table_name=pair.source_table_name,
        seed=seed,
        sample_ratio=sample_ratio,
        sample_min=sample_min,
        sample_max=sample_max,
        max_fields_per_domain=max_fields_per_domain,
    )
    target_samples = _build_domain_samples(
        dataframe=target_df,
        domain_id=target_domain_id,
        table_name=pair.target_table_name,
        seed=seed,
        sample_ratio=sample_ratio,
        sample_min=sample_min,
        sample_max=sample_max,
        max_fields_per_domain=max_fields_per_domain,
    )

    return PairCSVSamplingContext(
        source_domain_id=source_domain_id,
        target_domain_id=target_domain_id,
        source_rows=int(len(source_df)),
        source_columns=int(len(source_df.columns)),
        target_rows=int(len(target_df)),
        target_columns=int(len(target_df.columns)),
        source_has_duplicate_header_row=source_has_duplicate_header_row,
        target_has_duplicate_header_row=target_has_duplicate_header_row,
        source_samples=source_samples,
        target_samples=target_samples,
    )
