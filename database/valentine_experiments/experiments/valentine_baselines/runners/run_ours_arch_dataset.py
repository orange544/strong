from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[3]))
    from experiments.valentine_baselines.evaluation.aggregator import (  # type: ignore
        aggregate_method_dataset,
    )
    from experiments.valentine_baselines.methods.ours_arch.pipeline import (  # type: ignore
        load_ours_arch_config,
        resolve_eval_config,
        resolve_ours_arch_config,
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
        run_pair_with_main_architecture,
    )
    from ..utils.io import now_stamp, write_csv_rows, write_json
    from ..utils.logging_utils import setup_logger
    from .common import evaluate_pair_result, load_configs, load_dataset_pairs


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    module_root = script_dir.parent

    parser = argparse.ArgumentParser(
        description="Run main architecture on one Valentine dataset."
    )
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--config-dir", type=Path, default=module_root / "configs")
    parser.add_argument("--ours-config", type=Path, default=module_root / "configs" / "ours_arch.yaml")
    parser.add_argument("--output-dir", type=Path, default=module_root / "outputs")
    parser.add_argument("--run-name", type=str, default="ours_arch_dataset")
    parser.add_argument("--many-to-many", action="store_true", help="Disable one-to-one filtering.")
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--top-percent", type=float, default=None)
    args = parser.parse_args()

    stamp = now_stamp()
    run_output = args.output_dir / f"{args.run_name}_{stamp}"
    logger = setup_logger(
        "run_ours_arch_dataset",
        log_file=run_output / "logs" / f"run_ours_arch_dataset_{args.dataset}_{stamp}.log",
    )

    base_config = load_configs(args.config_dir)
    ours_payload = load_ours_arch_config(args.ours_config)
    runtime_cfg = resolve_ours_arch_config(ours_payload)
    eval_cfg = resolve_eval_config(ours_payload)
    pairs = load_dataset_pairs(base_config, args.dataset, limit=args.limit)
    if not pairs:
        logger.error("No pair samples found for dataset=%s", args.dataset)
        return

    one_to_one = not args.many_to_many if args.many_to_many else eval_cfg.one_to_one
    top_k = args.top_k if args.top_k is not None else eval_cfg.top_k
    top_percent = args.top_percent if args.top_percent is not None else eval_cfg.top_percent

    logger.info("Start dataset=%s pairs=%s", args.dataset, len(pairs))
    rows = []
    for idx, pair in enumerate(pairs, start=1):
        logger.info("Pair %s/%s: %s", idx, len(pairs), pair.pair_id)
        method_pred = run_pair_with_main_architecture(pair=pair, config=runtime_cfg)
        row = evaluate_pair_result(
            pair=pair,
            method_prediction=method_pred,
            one_to_one=one_to_one,
            top_k=top_k,
            top_percent=top_percent,
        )
        rows.append(row)

        raw_out = (
            run_output
            / "raw_predictions"
            / runtime_cfg.method_name
            / args.dataset
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

    per_pair_path = (
        run_output
        / "metrics"
        / "per_pair"
        / f"{runtime_cfg.method_name}_{args.dataset}.csv"
    )
    write_csv_rows(per_pair_path, rows, fieldnames=list(rows[0].keys()))
    logger.info("Per-pair metrics written: %s", per_pair_path)

    summary_rows = aggregate_method_dataset(rows)
    summary_path = (
        run_output
        / "metrics"
        / f"{runtime_cfg.method_name}_{args.dataset}_summary.csv"
    )
    write_csv_rows(summary_path, summary_rows, fieldnames=list(summary_rows[0].keys()))
    logger.info("Summary metrics written: %s", summary_path)

    print(f"[ours-arch-dataset] per_pair={per_pair_path}")
    print(f"[ours-arch-dataset] summary={summary_path}")


if __name__ == "__main__":
    main()

