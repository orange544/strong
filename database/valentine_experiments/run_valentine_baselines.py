from __future__ import annotations

import argparse
import json
import os
import tempfile
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


def _default_paths(script_path: Path) -> tuple[Path, Path, Path]:
    data_dir = script_path.parent / "data"
    source = data_dir / "authors1.csv"
    target = data_dir / "authors2.csv"
    gt = data_dir / "ground_truth_authors.json"
    return source, target, gt


def _load_ground_truth(path: Path) -> list[tuple[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"ground truth must be a JSON list: {path}")

    ground_truth: list[tuple[str, str]] = []
    for idx, item in enumerate(payload):
        if not isinstance(item, list | tuple) or len(item) != 2:
            raise ValueError(f"ground truth item #{idx} is invalid, expected [source_col, target_col]")
        left, right = item
        if not isinstance(left, str) or not isinstance(right, str):
            raise ValueError(f"ground truth item #{idx} must contain strings")
        ground_truth.append((left, right))
    return ground_truth


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


def _run_one(
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


def main() -> None:
    script_path = Path(__file__).resolve()
    default_source, default_target, default_gt = _default_paths(script_path)

    parser = argparse.ArgumentParser(
        description="Run Valentine baseline matchers and collect diagnostics."
    )
    parser.add_argument("--source", type=Path, default=default_source)
    parser.add_argument("--target", type=Path, default=default_target)
    parser.add_argument("--ground-truth", type=Path, default=default_gt)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=script_path.parent / "results",
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
        help="Disable mkdtemp patch workaround (not recommended on this Windows setup).",
    )
    args = parser.parse_args()

    os.environ["NLTK_DATA"] = str(args.nltk_data)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if not args.no_temp_patch:
        _patch_temp_mkdtemp(args.temp_dir)

    # Delay Valentine imports until environment variables are configured.
    from valentine import valentine_match
    from valentine.algorithms import (
        Coma,
        Cupid,
        DistributionBased,
        JaccardDistanceMatcher,
        SimilarityFlooding,
    )

    source_df = pd.read_csv(args.source)
    target_df = pd.read_csv(args.target)
    ground_truth = _load_ground_truth(args.ground_truth)

    builtin_matchers: list[tuple[str, Any]] = [
        ("Coma", Coma()),
        ("SimilarityFlooding", SimilarityFlooding()),
        ("Cupid", Cupid()),
        ("DistributionBased", DistributionBased()),
        ("JaccardDistanceMatcher", JaccardDistanceMatcher()),
    ]
    unsupported_matchers = {
        "ISResMat": "Not included in valentine 0.4.1. Requires custom implementation.",
        "Unicorn": "Not included in valentine 0.4.1. Requires external project/model setup.",
        "Magneto": "Not included in valentine 0.4.1. Requires retriever + LLM reranker pipeline.",
    }

    run_results = [
        _run_one(valentine_match, name, matcher, source_df, target_df, ground_truth)
        for name, matcher in builtin_matchers
    ]
    for name, reason in unsupported_matchers.items():
        run_results.append(
            {
                "status": "not_supported",
                "matcher": name,
                "reason": reason,
            }
        )

    preflight = {
        "source": str(args.source),
        "target": str(args.target),
        "ground_truth": str(args.ground_truth),
        "rows": {"source": int(len(source_df)), "target": int(len(target_df))},
        "columns": {"source": list(source_df.columns), "target": list(target_df.columns)},
        "nltk": _nltk_preflight(),
        "temp_patch_enabled": not args.no_temp_patch,
        "temp_dir": str(args.temp_dir),
        "nltk_data": str(args.nltk_data),
    }

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "preflight": preflight,
        "results": run_results,
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = args.output_dir / f"valentine_baseline_{stamp}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[valentine] result saved: {out_path}")
    for item in run_results:
        name = item["matcher"]
        status = item["status"]
        if status == "ok":
            f1 = item["metrics"].get("F1Score", 0.0)
            p = item["metrics"].get("Precision", 0.0)
            r = item["metrics"].get("Recall", 0.0)
            print(f"- {name:<22} OK  P={p:.4f} R={r:.4f} F1={f1:.4f}")
        elif status == "not_supported":
            print(f"- {name:<22} NOT_SUPPORTED  {item['reason']}")
        else:
            print(f"- {name:<22} ERROR  {item.get('error_type')}: {item.get('error')}")


if __name__ == "__main__":
    main()
