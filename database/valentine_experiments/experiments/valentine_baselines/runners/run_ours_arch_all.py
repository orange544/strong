from __future__ import annotations

import argparse
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[3]))
    from experiments.valentine_baselines.evaluation.aggregator import (  # type: ignore
        aggregate_method_dataset,
    )
    from experiments.valentine_baselines.methods.ours_arch.pipeline import (  # type: ignore
        load_ours_arch_config,
        resolve_eval_config,
        resolve_ours_arch_config,
        resolve_runner_defaults,
        run_pair_with_main_architecture,
    )
    from experiments.valentine_baselines.runners.common import (  # type: ignore
        evaluate_pair_result,
        load_configs,
        load_dataset_pairs,
    )
    from experiments.valentine_baselines.utils.io import now_stamp, write_csv_rows, write_json  # type: ignore
    from experiments.valentine_baselines.utils.logging_utils import setup_logger  # type: ignore
else:
    from ..evaluation.aggregator import aggregate_method_dataset
    from ..methods.ours_arch.pipeline import (
        load_ours_arch_config,
        resolve_eval_config,
        resolve_ours_arch_config,
        resolve_runner_defaults,
        run_pair_with_main_architecture,
    )
    from ..utils.io import now_stamp, write_csv_rows, write_json
    from ..utils.logging_utils import setup_logger
    from .common import evaluate_pair_result, load_configs, load_dataset_pairs


def _overall_summary(per_pair_rows: list[dict[str, Any]]) -> dict[str, Any]:
    ok_rows = [row for row in per_pair_rows if row.get("status") == "ok"]
    if not ok_rows:
        return {
            "precision": None,
            "recall": None,
            "f1": None,
            "runtime_sec": None,
            "ok_pairs": 0,
            "total_pairs": len(per_pair_rows),
            "status": "failed",
        }

    def _mean(key: str) -> float | None:
        values = [float(row[key]) for row in ok_rows if row.get(key) not in (None, "")]
        return statistics.mean(values) if values else None

    return {
        "precision": _mean("precision"),
        "recall": _mean("recall"),
        "f1": _mean("f1"),
        "runtime_sec": _mean("runtime_sec"),
        "ok_pairs": len(ok_rows),
        "total_pairs": len(per_pair_rows),
        "status": "ok",
    }


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    module_root = script_dir.parent

    parser = argparse.ArgumentParser(
        description="Run main architecture on all selected Valentine datasets."
    )
    parser.add_argument("--datasets", nargs="+", default=None)
    parser.add_argument("--pair-limit", type=int, default=None)
    parser.add_argument("--config-dir", type=Path, default=module_root / "configs")
    parser.add_argument("--ours-config", type=Path, default=module_root / "configs" / "ours_arch.yaml")
    parser.add_argument("--output-dir", type=Path, default=module_root / "outputs")
    parser.add_argument("--run-name", type=str, default="ours_arch_all")
    parser.add_argument("--many-to-many", action="store_true", help="Disable one-to-one filtering.")
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--top-percent", type=float, default=None)
    args = parser.parse_args()

    stamp = now_stamp()
    run_output = args.output_dir / f"{args.run_name}_{stamp}"
    logger = setup_logger(
        "run_ours_arch_all",
        log_file=run_output / "logs" / f"run_ours_arch_all_{stamp}.log",
    )

    base_config = load_configs(args.config_dir)
    ours_payload = load_ours_arch_config(args.ours_config)
    runtime_cfg = resolve_ours_arch_config(ours_payload)
    eval_cfg = resolve_eval_config(ours_payload)
    defaults = resolve_runner_defaults(ours_payload)
    datasets = args.datasets or defaults.default_datasets or list(
        base_config["default"].get("default_datasets", ["Magellan"])
    )
    if not datasets:
        logger.error("No datasets selected.")
        return

    one_to_one = not args.many_to_many if args.many_to_many else eval_cfg.one_to_one
    top_k = args.top_k if args.top_k is not None else eval_cfg.top_k
    top_percent = args.top_percent if args.top_percent is not None else eval_cfg.top_percent

    per_pair_rows: list[dict[str, Any]] = []
    per_dataset_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    logger.info("Start methods=%s datasets=%s", runtime_cfg.method_name, datasets)
    for dataset_name in datasets:
        pairs = load_dataset_pairs(base_config, dataset_name, limit=args.pair_limit)
        logger.info("Dataset=%s pairs=%s", dataset_name, len(pairs))
        if not pairs:
            logger.warning("No pairs for dataset=%s", dataset_name)
            continue
        for idx, pair in enumerate(pairs, start=1):
            logger.info("  Pair %s/%s: %s", idx, len(pairs), pair.pair_id)
            method_pred = run_pair_with_main_architecture(pair=pair, config=runtime_cfg)
            row = evaluate_pair_result(
                pair=pair,
                method_prediction=method_pred,
                one_to_one=one_to_one,
                top_k=top_k,
                top_percent=top_percent,
            )
            per_pair_rows.append(row)
            per_dataset_rows[dataset_name].append(row)

            raw_out = (
                run_output
                / "raw_predictions"
                / runtime_cfg.method_name
                / dataset_name
                / f"{pair.pair_id}_{stamp}.json"
            )
            write_json(
                raw_out,
                {
                    "pair": {
                        "dataset": pair.dataset_name,
                        "pair_id": pair.pair_id,
                        "source": str(pair.source_path),
                        "target": str(pair.target_path),
                        "ground_truth": str(pair.ground_truth_path),
                        "relatedness": pair.relatedness,
                    },
                    "method_prediction": {
                        "method": method_pred.method,
                        "status": method_pred.status,
                        "supports_ranking": method_pred.supports_ranking,
                        "runtime_sec": method_pred.runtime_sec,
                        "data_loading_sec": method_pred.data_loading_sec,
                        "inference_sec": method_pred.inference_sec,
                        "error_message": method_pred.error_message,
                        "metadata": method_pred.metadata,
                        "predictions": method_pred.predictions,
                    },
                    "metrics": row,
                },
            )

    if not per_pair_rows:
        logger.error("No experiment rows produced.")
        return

    per_pair_path = run_output / "metrics" / "per_pair" / "ours_arch_all_datasets.csv"
    write_csv_rows(per_pair_path, per_pair_rows, fieldnames=list(per_pair_rows[0].keys()))

    for dataset_name, rows in per_dataset_rows.items():
        dataset_path = run_output / "metrics" / "per_pair" / f"ours_arch_{dataset_name}.csv"
        write_csv_rows(dataset_path, rows, fieldnames=list(rows[0].keys()))

    summary_rows = aggregate_method_dataset(per_pair_rows)
    summary_dataset_path = run_output / "metrics" / "summary_ours_arch_by_dataset.csv"
    write_csv_rows(summary_dataset_path, summary_rows, fieldnames=list(summary_rows[0].keys()))

    overall = _overall_summary(per_pair_rows)
    overall_row = {
        "method": runtime_cfg.method_name,
        "dataset": "ALL",
        "precision": overall["precision"],
        "recall": overall["recall"],
        "f1": overall["f1"],
        "runtime_sec": overall["runtime_sec"],
        "ok_pairs": overall["ok_pairs"],
        "total_pairs": overall["total_pairs"],
        "status": overall["status"],
        "notes": "",
    }
    summary_all_path = run_output / "metrics" / "summary_ours_arch_all.csv"
    write_csv_rows(summary_all_path, [overall_row], fieldnames=list(overall_row.keys()))

    manifest_path = run_output / "run_manifest.json"
    write_json(
        manifest_path,
        {
            "stamp": stamp,
            "run_output": str(run_output),
            "method": runtime_cfg.method_name,
            "datasets": datasets,
            "pair_limit": args.pair_limit,
            "one_to_one": one_to_one,
            "top_k": top_k,
            "top_percent": top_percent,
            "files": {
                "per_pair_all": str(per_pair_path),
                "summary_by_dataset": str(summary_dataset_path),
                "summary_all": str(summary_all_path),
            },
        },
    )

    logger.info("Done. per_pair=%s", per_pair_path)
    logger.info("Done. summary_by_dataset=%s", summary_dataset_path)
    logger.info("Done. summary_all=%s", summary_all_path)
    logger.info("Done. manifest=%s", manifest_path)
    print(f"[ours-arch-all] per_pair_csv={per_pair_path}")
    print(f"[ours-arch-all] summary_by_dataset_csv={summary_dataset_path}")
    print(f"[ours-arch-all] summary_all_csv={summary_all_path}")
    print(f"[ours-arch-all] manifest={manifest_path}")


if __name__ == "__main__":
    main()

