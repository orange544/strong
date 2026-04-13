from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[3]))
    from experiments.valentine_baselines.runners.common import (  # type: ignore
        build_single_pair,
        evaluate_pair_result,
        load_configs,
        run_method,
    )
    from experiments.valentine_baselines.utils.io import now_stamp, write_csv_rows, write_json  # type: ignore
else:
    from .common import build_single_pair, evaluate_pair_result, load_configs, run_method
    from ..utils.io import now_stamp, write_csv_rows, write_json


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    module_root = script_dir.parent
    parser = argparse.ArgumentParser(description="Run one method on one pair sample.")
    parser.add_argument("--method", required=True, choices=["coma", "simflooding", "isresmat", "unicorn"])
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--ground-truth", type=Path, required=True)
    parser.add_argument("--dataset", default="Custom")
    parser.add_argument("--pair-id", default="single_pair")
    parser.add_argument("--config-dir", type=Path, default=module_root / "configs")
    parser.add_argument("--output-dir", type=Path, default=module_root / "outputs")
    parser.add_argument("--many-to-many", action="store_true", help="Disable one-to-one filtering.")
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--top-percent", type=float, default=None)
    args = parser.parse_args()

    config = load_configs(args.config_dir)
    pair = build_single_pair(
        dataset_name=args.dataset,
        pair_id=args.pair_id,
        source=args.source,
        target=args.target,
        ground_truth=args.ground_truth,
    )

    method_pred = run_method(args.method, pair, config)
    per_pair = evaluate_pair_result(
        pair=pair,
        method_prediction=method_pred,
        one_to_one=not args.many_to_many,
        top_k=args.top_k,
        top_percent=args.top_percent,
    )

    raw_out = (
        args.output_dir
        / "raw_predictions"
        / args.method
        / args.dataset
        / f"{args.pair_id}_{now_stamp()}.json"
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
            "metrics": per_pair,
        },
    )

    per_pair_file = args.output_dir / "metrics" / "per_pair" / f"{args.method}_{args.dataset}_single.csv"
    write_csv_rows(per_pair_file, [per_pair], fieldnames=list(per_pair.keys()))

    print(f"[single] method={args.method} dataset={args.dataset} pair={args.pair_id}")
    print(f"[single] status={per_pair['status']} f1={per_pair['f1']} runtime={per_pair['runtime_sec']}")
    print(f"[single] raw_output={raw_out}")
    print(f"[single] per_pair_csv={per_pair_file}")


if __name__ == "__main__":
    main()
