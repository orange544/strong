from __future__ import annotations

import argparse
import json
import os
import tempfile
import traceback
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

import pandas as pd


def _default_magellan_root(script_path: Path) -> Path:
    return (
        script_path.parent
        / "data"
        / "valentine_datasets_magellan_20260401"
        / "Valentine-datasets"
        / "Magellan"
    )


def _patch_temp_mkdtemp(base_dir: Path) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)

    def _patched_mkdtemp(*args: Any, **kwargs: Any) -> str:
        candidate = base_dir / f"tmp_{uuid.uuid4().hex[:12]}"
        os.mkdir(candidate, 0o777)
        return str(candidate)

    tempfile.mkdtemp = _patched_mkdtemp  # type: ignore[assignment]


def _nltk_preflight() -> dict[str, Any]:
    checks = {
        "punkt_tab": ["tokenizers/punkt_tab", "tokenizers/punkt_tab.zip"],
        "punkt": ["tokenizers/punkt", "tokenizers/punkt.zip"],
        "wordnet": ["corpora/wordnet", "corpora/wordnet.zip"],
        "stopwords": ["corpora/stopwords", "corpora/stopwords.zip"],
    }
    missing: list[str] = []
    details: dict[str, str] = {}
    try:
        import nltk
    except Exception as exc:  # pragma: no cover - environment dependent
        return {
            "ok": False,
            "missing": list(checks.keys()),
            "details": {"nltk": f"import failed: {exc!r}"},
        }

    for package_name, resource_paths in checks.items():
        found = False
        for resource_path in resource_paths:
            try:
                nltk.data.find(resource_path)
                found = True
                break
            except LookupError:
                continue
        if found:
            details[package_name] = "ok"
        else:
            missing.append(package_name)
            details[package_name] = "missing"

    return {"ok": len(missing) == 0, "missing": missing, "details": details}


def _drop_duplicate_header_row(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    first_row = [str(value).strip() for value in df.iloc[0].tolist()]
    columns = [str(col).strip() for col in df.columns.tolist()]
    if first_row == columns:
        return df.iloc[1:].reset_index(drop=True)
    return df


def _load_ground_truth_from_mapping(mapping_file: Path) -> list[tuple[str, str]]:
    payload = json.loads(mapping_file.read_text(encoding="utf-8"))
    matches_obj = payload.get("matches", [])
    if not isinstance(matches_obj, list):
        raise ValueError(f"invalid mapping file (missing matches list): {mapping_file}")

    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for idx, item in enumerate(matches_obj):
        if not isinstance(item, dict):
            raise ValueError(f"mapping item #{idx} is not an object")
        left_obj = item.get("source_column")
        right_obj = item.get("target_column")
        if not isinstance(left_obj, str) or not isinstance(right_obj, str):
            raise ValueError(f"mapping item #{idx} missing source_column/target_column")
        pair = (left_obj, right_obj)
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return pairs


def _discover_tasks(magellan_root: Path) -> list[dict[str, Any]]:
    if not magellan_root.exists():
        raise FileNotFoundError(f"magellan root not found: {magellan_root}")

    tasks: list[dict[str, Any]] = []
    for task_dir in sorted([item for item in magellan_root.iterdir() if item.is_dir()]):
        source_candidates = sorted(task_dir.glob("*_source.csv"))
        target_candidates = sorted(task_dir.glob("*_target.csv"))
        mapping_candidates = sorted(task_dir.glob("*_mapping.json"))
        if not source_candidates or not target_candidates or not mapping_candidates:
            continue
        tasks.append(
            {
                "task_name": task_dir.name,
                "source_file": source_candidates[0],
                "target_file": target_candidates[0],
                "mapping_file": mapping_candidates[0],
            }
        )
    return tasks


def _run_one(
    *,
    valentine_match_fn: Any,
    matcher_name: str,
    matcher: Any,
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    ground_truth: list[tuple[str, str]],
) -> dict[str, Any]:
    try:
        matches = valentine_match_fn(source_df, target_df, matcher)
        metrics_raw = matches.get_metrics(ground_truth)
        metrics = {key: float(value) for key, value in metrics_raw.items()}
        return {
            "status": "ok",
            "matcher": matcher_name,
            "num_matches": len(matches),
            "metrics": metrics,
        }
    except Exception as exc:  # pragma: no cover - runtime dependent
        return {
            "status": "error",
            "matcher": matcher_name,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }


def _aggregate(task_results: list[dict[str, Any]]) -> dict[str, Any]:
    collector: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "ok_tasks": 0,
            "error_tasks": 0,
            "not_supported_tasks": 0,
            "task_metrics": [],
            "errors": [],
        }
    )

    for task in task_results:
        task_name = str(task["task_name"])
        for result in task["results"]:
            matcher_name = str(result["matcher"])
            bucket = collector[matcher_name]
            status = result["status"]

            if status == "ok":
                bucket["ok_tasks"] += 1
                bucket["task_metrics"].append(
                    {
                        "task_name": task_name,
                        "Precision": result["metrics"].get("Precision"),
                        "Recall": result["metrics"].get("Recall"),
                        "F1Score": result["metrics"].get("F1Score"),
                    }
                )
            elif status == "not_supported":
                bucket["not_supported_tasks"] += 1
                reason_obj = result.get("reason", "")
                bucket["errors"].append(
                    {
                        "task_name": task_name,
                        "reason": reason_obj if isinstance(reason_obj, str) else str(reason_obj),
                    }
                )
            else:
                bucket["error_tasks"] += 1
                error_obj = result.get("error", "")
                bucket["errors"].append(
                    {
                        "task_name": task_name,
                        "error_type": result.get("error_type", ""),
                        "error": error_obj if isinstance(error_obj, str) else str(error_obj),
                    }
                )

    summary: dict[str, Any] = {}
    for matcher_name, payload in collector.items():
        metrics_items = payload["task_metrics"]
        avg_metrics: dict[str, float] = {}
        if metrics_items:
            avg_metrics = {
                "Precision": mean(item["Precision"] for item in metrics_items if item["Precision"] is not None),
                "Recall": mean(item["Recall"] for item in metrics_items if item["Recall"] is not None),
                "F1Score": mean(item["F1Score"] for item in metrics_items if item["F1Score"] is not None),
            }

        summary[matcher_name] = {
            "ok_tasks": payload["ok_tasks"],
            "error_tasks": payload["error_tasks"],
            "not_supported_tasks": payload["not_supported_tasks"],
            "avg_metrics": avg_metrics,
            "task_metrics": metrics_items,
            "errors": payload["errors"],
        }
    return summary


def main() -> None:
    script_path = Path(__file__).resolve()

    parser = argparse.ArgumentParser(
        description="Run baseline methods on Magellan tasks with unified reporting."
    )
    parser.add_argument(
        "--magellan-root",
        type=Path,
        default=_default_magellan_root(script_path),
        help="Root directory containing Magellan task subfolders.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=script_path.parent / "results",
        help="Directory to write JSON reports.",
    )
    parser.add_argument(
        "--nltk-data",
        type=Path,
        default=script_path.parent.parent / "nltk_data",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=Path.home() / ".codex" / "memories" / "valentine_temp",
    )
    parser.add_argument(
        "--no-temp-patch",
        action="store_true",
        help="Disable mkdtemp patch workaround.",
    )
    args = parser.parse_args()

    os.environ["NLTK_DATA"] = str(args.nltk_data)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if not args.no_temp_patch:
        _patch_temp_mkdtemp(args.temp_dir)

    tasks = _discover_tasks(args.magellan_root)
    if not tasks:
        raise RuntimeError(f"no valid Magellan tasks discovered under: {args.magellan_root}")

    from valentine import valentine_match
    from valentine.algorithms import Coma, SimilarityFlooding

    runnable_matchers: list[tuple[str, Any]] = [
        ("Coma", Coma()),
        ("SimilarityFlooding", SimilarityFlooding()),
    ]
    unsupported_matchers = {
        "ISResMat": "Not included in valentine 0.4.1. Requires custom implementation.",
        "Unicorn": "Not included in valentine 0.4.1. Requires external project/model setup.",
        "Magneto": "Not included in valentine 0.4.1. Requires retriever + LLM reranker pipeline.",
    }

    task_results: list[dict[str, Any]] = []
    for task in tasks:
        task_name = str(task["task_name"])
        source_file = Path(task["source_file"])
        target_file = Path(task["target_file"])
        mapping_file = Path(task["mapping_file"])

        source_df = _drop_duplicate_header_row(pd.read_csv(source_file))
        target_df = _drop_duplicate_header_row(pd.read_csv(target_file))
        ground_truth = _load_ground_truth_from_mapping(mapping_file)

        results = [
            _run_one(
                valentine_match_fn=valentine_match,
                matcher_name=name,
                matcher=matcher,
                source_df=source_df,
                target_df=target_df,
                ground_truth=ground_truth,
            )
            for name, matcher in runnable_matchers
        ]
        for name, reason in unsupported_matchers.items():
            results.append(
                {
                    "status": "not_supported",
                    "matcher": name,
                    "reason": reason,
                }
            )

        task_results.append(
            {
                "task_name": task_name,
                "source_file": str(source_file),
                "target_file": str(target_file),
                "mapping_file": str(mapping_file),
                "source_rows": int(len(source_df)),
                "target_rows": int(len(target_df)),
                "source_columns": list(source_df.columns),
                "target_columns": list(target_df.columns),
                "ground_truth_size": len(ground_truth),
                "results": results,
            }
        )

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "dataset": "Magellan",
        "task_count": len(task_results),
        "magellan_root": str(args.magellan_root),
        "preflight": {
            "nltk": _nltk_preflight(),
            "temp_patch_enabled": not args.no_temp_patch,
            "temp_dir": str(args.temp_dir),
            "nltk_data": str(args.nltk_data),
        },
        "tasks": task_results,
        "summary": _aggregate(task_results),
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = args.output_dir / f"valentine_magellan_baseline_{stamp}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[magellan] result saved: {out_path}")
    for matcher_name, item in report["summary"].items():
        avg_metrics = item.get("avg_metrics", {})
        if avg_metrics:
            print(
                f"- {matcher_name:<20} "
                f"OK={item['ok_tasks']} ERR={item['error_tasks']} NS={item['not_supported_tasks']} "
                f"P={avg_metrics.get('Precision', 0.0):.4f} "
                f"R={avg_metrics.get('Recall', 0.0):.4f} "
                f"F1={avg_metrics.get('F1Score', 0.0):.4f}"
            )
        else:
            print(
                f"- {matcher_name:<20} "
                f"OK={item['ok_tasks']} ERR={item['error_tasks']} NS={item['not_supported_tasks']}"
            )


if __name__ == "__main__":
    main()

