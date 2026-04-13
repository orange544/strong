from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[3]))
    from experiments.valentine_baselines.adapters import ours_sda_adapter  # type: ignore
    from experiments.valentine_baselines.evaluation.aggregator import (  # type: ignore
        aggregate_method_dataset,
    )
    from experiments.valentine_baselines.runners.common import (  # type: ignore
        evaluate_pair_result,
        load_configs,
        load_dataset_pairs,
    )
    from experiments.valentine_baselines.utils.io import (  # type: ignore
        now_stamp,
        read_yaml,
        write_csv_rows,
        write_json,
    )
    from experiments.valentine_baselines.utils.logging_utils import setup_logger  # type: ignore
else:
    from ..adapters import ours_sda_adapter
    from ..evaluation.aggregator import aggregate_method_dataset
    from ..utils.io import now_stamp, read_yaml, write_csv_rows, write_json
    from ..utils.logging_utils import setup_logger
    from .common import evaluate_pair_result, load_configs, load_dataset_pairs


def _load_ours_method_cfg(path: Path) -> dict[str, Any]:
    payload = read_yaml(path)
    method_cfg = payload.get("method", {})
    if not isinstance(method_cfg, dict):
        raise ValueError(f"invalid ours method config in {path}")
    return payload


def _resolve_eval_setting(
    *,
    args_value: Any,
    config_value: Any,
) -> Any:
    if args_value is not None:
        return args_value
    return config_value


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    module_root = script_dir.parent

    parser = argparse.ArgumentParser(
        description="Run custom Sample-Describe-Aggregate method on all selected Valentine datasets."
    )
    parser.add_argument("--datasets", nargs="+", default=None)
    parser.add_argument("--pair-limit", type=int, default=None)
    parser.add_argument("--config-dir", type=Path, default=module_root / "configs")
    parser.add_argument("--ours-config", type=Path, default=module_root / "configs" / "ours_sda.yaml")
    parser.add_argument("--output-dir", type=Path, default=module_root / "outputs")
    parser.add_argument("--run-name", type=str, default="ours_sda")
    parser.add_argument("--many-to-many", action="store_true", help="Disable one-to-one filtering.")
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--top-percent", type=float, default=None)
    args = parser.parse_args()

    stamp = now_stamp()
    run_output = args.output_dir / f"{args.run_name}_{stamp}"
    logger = setup_logger(
        "run_ours_all_datasets",
        log_file=run_output / "logs" / f"run_ours_all_datasets_{stamp}.log",
    )

    base_config = load_configs(args.config_dir)
    ours_payload = _load_ours_method_cfg(args.ours_config)
    ours_method_cfg = dict(ours_payload.get("method", {}))
    eval_cfg = ours_payload.get("evaluation", {})
    runner_cfg = ours_payload.get("runner", {})
    if not isinstance(eval_cfg, dict):
        eval_cfg = {}
    if not isinstance(runner_cfg, dict):
        runner_cfg = {}

    datasets = args.datasets
    if datasets is None:
        default_datasets = runner_cfg.get("default_datasets", [])
        if isinstance(default_datasets, list) and default_datasets:
            datasets = [str(item) for item in default_datasets]
        else:
            datasets = list(base_config["default"].get("default_datasets", ["Magellan"]))

    if not datasets:
        logger.error("No datasets provided.")
        return

    one_to_one = not args.many_to_many if args.many_to_many else bool(eval_cfg.get("one_to_one", True))
    top_k = _resolve_eval_setting(args_value=args.top_k, config_value=eval_cfg.get("top_k"))
    top_percent = _resolve_eval_setting(args_value=args.top_percent, config_value=eval_cfg.get("top_percent"))

    per_pair_rows: list[dict[str, Any]] = []
    per_dataset_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    logger.info("Run start stamp=%s datasets=%s", stamp, datasets)
    for dataset_name in datasets:
        pairs = load_dataset_pairs(base_config, dataset_name, limit=args.pair_limit)
        logger.info("Dataset=%s pairs=%s", dataset_name, len(pairs))
        if not pairs:
            logger.warning("No pairs discovered for dataset=%s", dataset_name)
            continue

        for idx, pair in enumerate(pairs, start=1):
            logger.info("  Pair %s/%s: %s", idx, len(pairs), pair.pair_id)
            method_pred = ours_sda_adapter.run(pair, ours_method_cfg)
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
                / str(row["method"])
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

    all_per_pair_path = run_output / "metrics" / "per_pair" / "ours_sda_all_datasets.csv"
    write_csv_rows(all_per_pair_path, per_pair_rows, fieldnames=list(per_pair_rows[0].keys()))

    for dataset_name, rows in per_dataset_rows.items():
        dataset_path = run_output / "metrics" / "per_pair" / f"ours_sda_{dataset_name}.csv"
        write_csv_rows(dataset_path, rows, fieldnames=list(rows[0].keys()))

    summary_rows = aggregate_method_dataset(per_pair_rows)
    summary_path = run_output / "metrics" / "summary_ours_sda.csv"
    write_csv_rows(summary_path, summary_rows, fieldnames=list(summary_rows[0].keys()))

    manifest_path = run_output / "run_manifest.json"
    write_json(
        manifest_path,
        {
            "stamp": stamp,
            "run_output": str(run_output),
            "datasets": datasets,
            "pair_limit": args.pair_limit,
            "one_to_one": one_to_one,
            "top_k": top_k,
            "top_percent": top_percent,
            "files": {
                "per_pair_all": str(all_per_pair_path),
                "summary": str(summary_path),
            },
        },
    )

    logger.info("Done. per_pair=%s", all_per_pair_path)
    logger.info("Done. summary=%s", summary_path)
    logger.info("Done. manifest=%s", manifest_path)
    print(f"[ours] per_pair_csv={all_per_pair_path}")
    print(f"[ours] summary_csv={summary_path}")
    print(f"[ours] manifest={manifest_path}")


if __name__ == "__main__":
    main()

