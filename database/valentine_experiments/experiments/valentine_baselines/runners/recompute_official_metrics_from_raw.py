from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

from valentine.algorithms.matcher_results import MatcherResults


DEFAULT_METHODS = ("coma", "simflooding")
DEFAULT_DATASETS = ("Magellan", "OpenData", "ChEMBL", "TPC-DI", "Wikidata")


@dataclass
class PairRun:
    file_path: Path
    payload: dict[str, Any]


def _now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_ground_truth_columns(path: Path) -> list[tuple[str, str]]:
    payload = _read_json(path)
    if isinstance(payload, dict):
        matches = payload.get("matches", [])
        out: list[tuple[str, str]] = []
        if isinstance(matches, list):
            for item in matches:
                if not isinstance(item, dict):
                    continue
                out.append(
                    (
                        str(item.get("source_column", "")),
                        str(item.get("target_column", "")),
                    )
                )
        return out
    if isinstance(payload, list):
        out = []
        for item in payload:
            if isinstance(item, list | tuple) and len(item) == 2:
                out.append((str(item[0]), str(item[1])))
        return out
    return []


def _build_matcher_results(predictions: list[dict[str, Any]]) -> MatcherResults:
    dedupe: dict[tuple[tuple[str, str], tuple[str, str]], float] = {}
    for pred in predictions:
        key = (
            (str(pred.get("source_table", "")), str(pred.get("source_column", ""))),
            (str(pred.get("target_table", "")), str(pred.get("target_column", ""))),
        )
        try:
            score = float(pred.get("score", 0.0))
        except Exception:
            score = 0.0
        if key not in dedupe or score > dedupe[key]:
            dedupe[key] = score
    return MatcherResults(dedupe)


def _discover_latest_pair_runs(raw_dir: Path, method: str, dataset: str) -> dict[str, PairRun]:
    base = raw_dir / method / dataset
    if not base.exists():
        return {}

    latest: dict[str, PairRun] = {}
    for file_path in base.glob("*.json"):
        try:
            payload = _read_json(file_path)
        except Exception:
            continue
        pair = payload.get("pair", {})
        pair_id = str(pair.get("pair_id", "")).strip()
        if not pair_id:
            continue
        prev = latest.get(pair_id)
        if prev is None or file_path.stat().st_mtime > prev.file_path.stat().st_mtime:
            latest[pair_id] = PairRun(file_path=file_path, payload=payload)
    return latest


def _metric_value(metrics: dict[str, Any], key: str) -> float | None:
    raw = metrics.get(key)
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _precision_top_n(metrics: dict[str, Any]) -> float | None:
    for key, value in metrics.items():
        if key.startswith("PrecisionTop") and key.endswith("Percent"):
            try:
                return float(value)
            except Exception:
                return None
    return None


def _mean(values: list[float]) -> float | None:
    return mean(values) if values else None


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    module_root = script_dir.parent
    parser = argparse.ArgumentParser(
        description=(
            "Recompute COMA/SimFlooding metrics with Valentine official metric logic "
            "from existing raw predictions, without overwriting prior metrics."
        )
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=module_root / "outputs" / "raw_predictions",
        help="Raw prediction root directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=module_root / "outputs" / f"recomputed_official_{_now_stamp()}",
        help="Output directory for recomputed official metrics.",
    )
    parser.add_argument(
        "--methods",
        nargs="+",
        default=list(DEFAULT_METHODS),
        help="Methods to recompute.",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=list(DEFAULT_DATASETS),
        help="Datasets to recompute.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_dir: Path = args.raw_dir
    output_dir: Path = args.output_dir
    methods = [m.strip() for m in args.methods if str(m).strip()]
    datasets = [d.strip() for d in args.datasets if str(d).strip()]

    per_pair_root = output_dir / "metrics" / "per_pair"
    summary_root = output_dir / "metrics"

    summary_rows: list[dict[str, Any]] = []
    run_manifest: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "raw_dir": str(raw_dir),
        "output_dir": str(output_dir),
        "methods": methods,
        "datasets": datasets,
        "results": [],
    }

    for method in methods:
        for dataset in datasets:
            pair_runs = _discover_latest_pair_runs(raw_dir, method, dataset)
            if not pair_runs:
                print(f"[skip] no raw predictions for method={method} dataset={dataset}")
                continue

            per_pair_rows: list[dict[str, Any]] = []
            for pair_id, pair_run in sorted(pair_runs.items()):
                payload = pair_run.payload
                pair_info = payload.get("pair", {})
                pred_info = payload.get("method_prediction", {})
                status = str(pred_info.get("status", "")).strip() or "error"
                runtime_sec = float(pred_info.get("runtime_sec", 0.0) or 0.0)
                data_loading_sec = float(pred_info.get("data_loading_sec", 0.0) or 0.0)
                inference_sec = float(pred_info.get("inference_sec", 0.0) or 0.0)
                error_message = str(pred_info.get("error_message", "") or "")
                supports_ranking = bool(pred_info.get("supports_ranking", False))

                precision = None
                recall = None
                f1 = None
                precision_top10 = None
                recall_at_gt = None

                if status == "ok":
                    predictions = pred_info.get("predictions", [])
                    if isinstance(predictions, list):
                        gt_path = Path(str(pair_info.get("ground_truth", "") or ""))
                        gt_cols = _load_ground_truth_columns(gt_path) if gt_path.exists() else []
                        matches = _build_matcher_results(predictions)
                        metrics = matches.get_metrics(gt_cols)
                        precision = _metric_value(metrics, "Precision")
                        recall = _metric_value(metrics, "Recall")
                        f1 = _metric_value(metrics, "F1Score")
                        precision_top10 = _precision_top_n(metrics)
                        recall_at_gt = _metric_value(metrics, "RecallAtSizeofGroundTruth")

                per_pair_rows.append(
                    {
                        "method": method,
                        "dataset": dataset,
                        "pair_id": pair_id,
                        "precision": precision,
                        "recall": recall,
                        "f1": f1,
                        "precision_top10": precision_top10,
                        "recall_at_gt": recall_at_gt,
                        "runtime_sec": runtime_sec,
                        "data_loading_sec": data_loading_sec,
                        "inference_sec": inference_sec,
                        "status": status,
                        "error_message": error_message,
                        "supports_ranking": supports_ranking,
                        "source_raw_file": str(pair_run.file_path),
                    }
                )

            per_pair_file = per_pair_root / f"{method}_{dataset}_official.csv"
            _write_csv(
                per_pair_file,
                per_pair_rows,
                [
                    "method",
                    "dataset",
                    "pair_id",
                    "precision",
                    "recall",
                    "f1",
                    "precision_top10",
                    "recall_at_gt",
                    "runtime_sec",
                    "data_loading_sec",
                    "inference_sec",
                    "status",
                    "error_message",
                    "supports_ranking",
                    "source_raw_file",
                ],
            )

            ok_rows = [r for r in per_pair_rows if r["status"] == "ok"]
            p_vals = [float(r["precision"]) for r in ok_rows if r["precision"] is not None]
            r_vals = [float(r["recall"]) for r in ok_rows if r["recall"] is not None]
            f1_vals = [float(r["f1"]) for r in ok_rows if r["f1"] is not None]
            pt_vals = [float(r["precision_top10"]) for r in ok_rows if r["precision_top10"] is not None]
            rag_vals = [float(r["recall_at_gt"]) for r in ok_rows if r["recall_at_gt"] is not None]
            rt_vals = [float(r["runtime_sec"]) for r in ok_rows]

            summary = {
                "method": method,
                "dataset": dataset,
                "precision": _mean(p_vals),
                "recall": _mean(r_vals),
                "f1": _mean(f1_vals),
                "precision_top10": _mean(pt_vals),
                "recall_at_gt": _mean(rag_vals),
                "runtime_sec": _mean(rt_vals),
                "ok_pairs": len(ok_rows),
                "total_pairs": len(per_pair_rows),
                "status": "ok" if ok_rows else "failed",
                "per_pair_file": str(per_pair_file),
            }
            summary_rows.append(summary)
            run_manifest["results"].append(summary)

            summary_file = summary_root / f"{method}_{dataset}_official_summary.csv"
            _write_csv(summary_file, [summary], list(summary.keys()))
            print(
                f"[done] method={method} dataset={dataset} "
                f"ok_pairs={summary['ok_pairs']}/{summary['total_pairs']}"
            )

    if summary_rows:
        combined_file = summary_root / "official_coma_simflooding_5datasets_recomputed.csv"
        _write_csv(
            combined_file,
            summary_rows,
            [
                "method",
                "dataset",
                "precision",
                "recall",
                "f1",
                "precision_top10",
                "recall_at_gt",
                "runtime_sec",
                "ok_pairs",
                "total_pairs",
                "status",
                "per_pair_file",
            ],
        )
        run_manifest["combined_summary_file"] = str(combined_file)
        print(f"[done] combined summary: {combined_file}")

    manifest_file = output_dir / "run_manifest.json"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(json.dumps(run_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] manifest: {manifest_file}")


if __name__ == "__main__":
    main()
