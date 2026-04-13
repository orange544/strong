from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..utils.io import read_json


@dataclass
class PairSample:
    dataset_name: str
    pair_id: str
    source_path: Path
    target_path: Path
    ground_truth_path: Path
    source_table_name: str
    target_table_name: str
    relatedness: str | None = None


@dataclass
class MethodPrediction:
    method: str
    status: str
    predictions: list[dict[str, Any]] = field(default_factory=list)
    supports_ranking: bool = False
    runtime_sec: float = 0.0
    data_loading_sec: float = 0.0
    inference_sec: float = 0.0
    error_message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def prediction_row(
    source_table: str,
    source_column: str,
    target_table: str,
    target_column: str,
    score: float,
) -> dict[str, Any]:
    return {
        "source_table": source_table,
        "source_column": source_column,
        "target_table": target_table,
        "target_column": target_column,
        "score": float(score),
    }


def normalize_valentine_matches(matches: dict[tuple[tuple[str, str], tuple[str, str]], float]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, score in matches.items():
        left, right = key
        rows.append(prediction_row(left[0], left[1], right[0], right[1], float(score)))
    rows.sort(key=lambda item: item["score"], reverse=True)
    return rows


def load_ground_truth_pairs(path: Path) -> list[dict[str, str]]:
    payload = read_json(path)
    if isinstance(payload, list):
        rows: list[dict[str, str]] = []
        for item in payload:
            if isinstance(item, list | tuple) and len(item) == 2:
                left = str(item[0])
                right = str(item[1])
                rows.append(
                    {
                        "source_table": "__unknown__",
                        "source_column": left,
                        "target_table": "__unknown__",
                        "target_column": right,
                    }
                )
            elif isinstance(item, dict):
                rows.append(
                    {
                        "source_table": str(item.get("source_table", "")),
                        "source_column": str(item.get("source_column", "")),
                        "target_table": str(item.get("target_table", "")),
                        "target_column": str(item.get("target_column", "")),
                    }
                )
            else:
                raise ValueError(f"invalid ground truth item: {item!r}")
        return rows

    if isinstance(payload, dict):
        matches = payload.get("matches")
        if not isinstance(matches, list):
            raise ValueError(f"invalid mapping json, missing matches list: {path}")
        rows = []
        for item in matches:
            if not isinstance(item, dict):
                raise ValueError(f"invalid mapping item: {item!r}")
            rows.append(
                {
                    "source_table": str(item["source_table"]),
                    "source_column": str(item["source_column"]),
                    "target_table": str(item["target_table"]),
                    "target_column": str(item["target_column"]),
                }
            )
        return rows

    raise ValueError(f"unsupported ground truth payload type: {type(payload)!r}")


def discover_valentine_pairs(dataset_root: Path, dataset_name: str) -> list[PairSample]:
    target_root = dataset_root / dataset_name
    if not target_root.exists():
        return []

    pairs: list[PairSample] = []
    for directory in sorted([p for p in target_root.rglob("*") if p.is_dir()]):
        src = sorted(directory.glob("*_source.csv"))
        tgt = sorted(directory.glob("*_target.csv"))
        mp = sorted(directory.glob("*_mapping.json"))
        if not src or not tgt or not mp:
            continue

        rel_parts = directory.relative_to(target_root).parts
        relatedness = rel_parts[0] if len(rel_parts) > 1 else None
        pair_id = directory.name
        source_table_name = src[0].stem
        target_table_name = tgt[0].stem
        pairs.append(
            PairSample(
                dataset_name=dataset_name,
                pair_id=pair_id,
                source_path=src[0],
                target_path=tgt[0],
                ground_truth_path=mp[0],
                source_table_name=source_table_name,
                target_table_name=target_table_name,
                relatedness=relatedness,
            )
        )
    return pairs
