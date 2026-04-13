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
    from experiments.valentine_baselines.evaluation.formatter import summary_markdown  # type: ignore
    from experiments.valentine_baselines.runners.common import (  # type: ignore
        evaluate_pair_result,
        load_configs,
        load_dataset_pairs,
        run_method,
    )
    from experiments.valentine_baselines.utils.io import (  # type: ignore
        now_stamp,
        read_yaml,
        write_csv_rows,
        write_json,
        write_text,
    )
    from experiments.valentine_baselines.utils.logging_utils import setup_logger  # type: ignore
else:
    from ..adapters import ours_sda_adapter
    from ..evaluation.aggregator import aggregate_method_dataset
    from ..evaluation.formatter import summary_markdown
    from ..utils.io import now_stamp, read_yaml, write_csv_rows, write_json, write_text
    from ..utils.logging_utils import setup_logger
    from .common import evaluate_pair_result, load_configs, load_dataset_pairs, run_method


def _run_one_method(
    *,
    method_name: str,
    pair: Any,
    shared_config: dict[str, Any],
    ours_method_cfg: dict[str, Any],
) -> Any:
    if method_name == "ours_sda":
        return ours_sda_adapter.run(pair, ours_method_cfg)
    return run_method(method_name, pair, shared_config)


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    module_root = script_dir.parent

    parser = argparse.ArgumentParser(description="Run baseline methods plus custom ours_sda method.")
    parser.add_argument("--methods", nargs="+", default=None)
    parser.add_argument("--datasets", nargs="+", default=None)
    parser.add_argument("--pair-limit", type=int, default=None)
    parser.add_argument("--config-dir", type=Path, default=module_root / "configs")
    parser.add_argument("--ours-config", type=Path, default=module_root / "configs" / "ours_sda.yaml")
    parser.add_argument("--output-dir", type=Path, default=module_root / "outputs")
    parser.add_argument("--run-name", type=str, default="compare_plus_ours")
    parser.add_argument("--many-to-many", action="store_true", help="Disable one-to-one filtering.")
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--top-percent", type=float, default=None)
    args = parser.parse_args()

    stamp = now_stamp()
    run_output = args.output_dir / f"{args.run_name}_{stamp}"
    logger = setup_logger(
        "run_compare_plus_ours",
        log_file=run_output / "logs" / f"run_compare_plus_ours_{stamp}.log",
    )

    config = load_configs(args.config_dir)
    ours_payload = read_yaml(args.ours_config)
    ours_method_cfg = ours_payload.get("method", {})
    if not isinstance(ours_method_cfg, dict):
        raise ValueError(f"invalid ours config: {args.ours_config}")

    default_methods = list(config["default"].get("default_methods", ["coma", "simflooding"]))
    methods = args.methods or [*default_methods, "ours_sda"]
    ordered_methods: list[str] = []
    seen: set[str] = set()
    for method in methods:
        key = str(method).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered_methods.append(key)
    methods = ordered_methods

    datasets = args.datasets or list(config["default"].get("default_datasets", ["Magellan"]))
    if not datasets:
        logger.error("No datasets provided.")
        return

    default_eval = config["default"].get("evaluation", {})
    if not isinstance(default_eval, dict):
        default_eval = {}
    one_to_one = not args.many_to_many if args.many_to_many else bool(default_eval.get("one_to_one", True))
    top_k = args.top_k if args.top_k is not None else default_eval.get("top_k")
    top_percent = args.top_percent if args.top_percent is not None else default_eval.get("top_percent")

    per_pair_rows: list[dict[str, Any]] = []
    logger.info("Run start stamp=%s methods=%s datasets=%s", stamp, methods, datasets)
    for dataset_name in datasets:
        pairs = load_dataset_pairs(config, dataset_name, limit=args.pair_limit)
        logger.info("Dataset=%s pairs=%s", dataset_name, len(pairs))
        if not pairs:
            logger.warning("No pairs found for dataset=%s", dataset_name)
            continue

        for method_name in methods:
            logger.info("Method=%s dataset=%s", method_name, dataset_name)
            for idx, pair in enumerate(pairs, start=1):
                logger.info("  Pair %s/%s: %s", idx, len(pairs), pair.pair_id)
                method_pred = _run_one_method(
                    method_name=method_name,
                    pair=pair,
                    shared_config=config,
                    ours_method_cfg=ours_method_cfg,
                )
                row = evaluate_pair_result(
                    pair=pair,
                    method_prediction=method_pred,
                    one_to_one=one_to_one,
                    top_k=top_k,
                    top_percent=top_percent,
                )
                per_pair_rows.append(row)

                raw_out = (
                    run_output
                    / "raw_predictions"
                    / method_name
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

    per_pair_path = run_output / "metrics" / "per_pair" / "all_methods_plus_ours.csv"
    write_csv_rows(per_pair_path, per_pair_rows, fieldnames=list(per_pair_rows[0].keys()))

    summary_rows = aggregate_method_dataset(per_pair_rows)
    summary_csv = run_output / "metrics" / "summary_all_methods_plus_ours.csv"
    write_csv_rows(summary_csv, summary_rows, fieldnames=list(summary_rows[0].keys()))
    summary_md = run_output / "metrics" / "summary_all_methods_plus_ours.md"
    write_text(summary_md, summary_markdown(summary_rows))

    by_method: dict[str, int] = defaultdict(int)
    for row in per_pair_rows:
        by_method[str(row.get("method", ""))] += 1

    manifest = run_output / "run_manifest.json"
    write_json(
        manifest,
        {
            "stamp": stamp,
            "run_output": str(run_output),
            "methods": methods,
            "datasets": datasets,
            "pair_limit": args.pair_limit,
            "one_to_one": one_to_one,
            "top_k": top_k,
            "top_percent": top_percent,
            "method_rows": dict(by_method),
            "files": {
                "per_pair_csv": str(per_pair_path),
                "summary_csv": str(summary_csv),
                "summary_md": str(summary_md),
            },
        },
    )

    logger.info("Done. per_pair=%s", per_pair_path)
    logger.info("Done. summary=%s", summary_csv)
    logger.info("Done. manifest=%s", manifest)
    print(f"[compare+ours] per_pair_csv={per_pair_path}")
    print(f"[compare+ours] summary_csv={summary_csv}")
    print(f"[compare+ours] manifest={manifest}")


if __name__ == "__main__":
    main()

