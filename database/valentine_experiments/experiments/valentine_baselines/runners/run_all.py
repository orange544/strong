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
    from experiments.valentine_baselines.evaluation.formatter import (  # type: ignore
        summary_latex,
        summary_markdown,
    )
    from experiments.valentine_baselines.runners.common import (  # type: ignore
        evaluate_pair_result,
        load_configs,
        load_dataset_pairs,
        run_method,
    )
    from experiments.valentine_baselines.utils.io import (  # type: ignore
        now_stamp,
        write_csv_rows,
        write_json,
        write_text,
    )
    from experiments.valentine_baselines.utils.logging_utils import setup_logger  # type: ignore
else:
    from ..evaluation.aggregator import aggregate_method_dataset
    from ..evaluation.formatter import summary_latex, summary_markdown
    from ..utils.io import now_stamp, write_csv_rows, write_json, write_text
    from ..utils.logging_utils import setup_logger
    from .common import evaluate_pair_result, load_configs, load_dataset_pairs, run_method


def _plot_bar(
    path: Path,
    title: str,
    x_labels: list[str],
    values: list[float],
    y_label: str,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(x_labels, values)
    ax.set_title(title)
    ax.set_xlabel("Method")
    ax.set_ylabel(y_label)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _build_report(
    *,
    stamp: str,
    methods: list[str],
    datasets: list[str],
    per_pair_rows: list[dict[str, Any]],
    summary_rows: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    lines.append("# Experiment Report")
    lines.append("")
    lines.append(f"- Run timestamp: `{stamp}`")
    lines.append(f"- Methods: `{', '.join(methods)}`")
    lines.append(f"- Datasets: `{', '.join(datasets)}`")
    lines.append(f"- Total pair runs: `{len(per_pair_rows)}`")
    lines.append("")
    lines.append("## Reproduction Status")
    lines.append("")
    status_count: dict[str, int] = defaultdict(int)
    for row in per_pair_rows:
        status_count[str(row.get("status", "unknown"))] += 1
    for key, value in sorted(status_count.items()):
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Main Results")
    lines.append("")
    lines.append(summary_markdown(summary_rows))

    fail_rows = [row for row in per_pair_rows if row.get("status") != "ok"]
    lines.append("## Failed / Not Available Items")
    lines.append("")
    if not fail_rows:
        lines.append("- None")
    else:
        for row in fail_rows[:50]:
            lines.append(
                "- `{method}` / `{dataset}` / `{pair}` -> `{status}`: {err}".format(
                    method=row.get("method", ""),
                    dataset=row.get("dataset", ""),
                    pair=row.get("pair_id", ""),
                    status=row.get("status", ""),
                    err=row.get("error_message", ""),
                )
            )
    lines.append("")
    lines.append("## Fairness And Limitations")
    lines.append("")
    lines.append("- COMA and SimFlooding use native Valentine interfaces.")
    lines.append("- ISResMat and Unicorn are integrated through external adapters.")
    lines.append("- If a method does not provide ranked scores, MRR is not computed.")
    lines.append("- Any unavailable method is marked explicitly; no metric is fabricated.")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    module_root = script_dir.parent

    parser = argparse.ArgumentParser(description="Run all configured baseline methods.")
    parser.add_argument(
        "--methods",
        nargs="+",
        default=None,
        help="Methods to run: coma simflooding isresmat unicorn",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=None,
        help="Datasets to run: Magellan OpenData ...",
    )
    parser.add_argument("--pair-limit", type=int, default=None)
    parser.add_argument("--config-dir", type=Path, default=module_root / "configs")
    parser.add_argument("--output-dir", type=Path, default=module_root / "outputs")
    parser.add_argument("--many-to-many", action="store_true", help="Disable one-to-one filtering.")
    parser.add_argument("--top-k", type=int, default=None)
    parser.add_argument("--top-percent", type=float, default=None)
    args = parser.parse_args()

    config = load_configs(args.config_dir)
    default_cfg = config["default"]
    methods = args.methods or list(default_cfg.get("default_methods", ["coma", "simflooding"]))
    datasets = args.datasets or list(default_cfg.get("default_datasets", ["Magellan"]))
    stamp = now_stamp()

    logger = setup_logger(
        "run_all",
        log_file=args.output_dir / "logs" / f"run_all_{stamp}.log",
    )
    logger.info("Run start stamp=%s methods=%s datasets=%s", stamp, methods, datasets)

    per_pair_rows: list[dict[str, Any]] = []
    for dataset_name in datasets:
        pairs = load_dataset_pairs(config, dataset_name, limit=args.pair_limit)
        logger.info("Dataset=%s pairs=%s", dataset_name, len(pairs))
        if not pairs:
            logger.warning("No pairs discovered for dataset=%s", dataset_name)
            continue

        for method_name in methods:
            logger.info("Method=%s on dataset=%s", method_name, dataset_name)
            for idx, pair in enumerate(pairs, start=1):
                logger.info("  Pair %s/%s: %s", idx, len(pairs), pair.pair_id)
                method_pred = run_method(method_name, pair, config)
                row = evaluate_pair_result(
                    pair=pair,
                    method_prediction=method_pred,
                    one_to_one=not args.many_to_many,
                    top_k=args.top_k,
                    top_percent=args.top_percent,
                )
                per_pair_rows.append(row)

                raw_out = (
                    args.output_dir
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

    all_per_pair_path = args.output_dir / "metrics" / "per_pair" / f"all_methods_{stamp}.csv"
    write_csv_rows(all_per_pair_path, per_pair_rows, fieldnames=list(per_pair_rows[0].keys()))

    summary_rows = aggregate_method_dataset(per_pair_rows)
    summary_all_path = args.output_dir / "metrics" / "summary_all_methods.csv"
    write_csv_rows(summary_all_path, summary_rows, fieldnames=list(summary_rows[0].keys()))

    md_table_path = args.output_dir / "metrics" / "summary_all_methods.md"
    tex_table_path = args.output_dir / "metrics" / "summary_all_methods.tex"
    write_text(md_table_path, summary_markdown(summary_rows))
    write_text(tex_table_path, summary_latex(summary_rows))

    by_method: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in summary_rows:
        by_method[str(row["method"])].append(row)

    f1_methods = []
    f1_values = []
    runtime_values = []
    mrr_values = []
    mrr_methods = []
    for method, items in sorted(by_method.items()):
        f1_list = [float(x["f1"]) for x in items if x.get("f1") is not None]
        rt_list = [float(x["runtime_sec"]) for x in items if x.get("runtime_sec") is not None]
        if f1_list:
            f1_methods.append(method)
            f1_values.append(statistics.mean(f1_list))
        if rt_list:
            runtime_values.append(statistics.mean(rt_list))
        else:
            runtime_values.append(0.0)
        mrr_list = [float(x["mrr"]) for x in items if x.get("mrr") is not None]
        if mrr_list:
            mrr_methods.append(method)
            mrr_values.append(statistics.mean(mrr_list))

    if f1_methods:
        _plot_bar(
            args.output_dir / "figures" / "f1_by_method.png",
            "F1 By Method",
            f1_methods,
            f1_values,
            "F1",
        )
        _plot_bar(
            args.output_dir / "figures" / "runtime_by_method.png",
            "Runtime By Method",
            f1_methods,
            runtime_values[: len(f1_methods)],
            "Runtime (sec)",
        )
    if mrr_methods:
        _plot_bar(
            args.output_dir / "figures" / "mrr_by_method.png",
            "MRR By Method",
            mrr_methods,
            mrr_values,
            "MRR",
        )

    report_text = _build_report(
        stamp=stamp,
        methods=methods,
        datasets=datasets,
        per_pair_rows=per_pair_rows,
        summary_rows=summary_rows,
    )
    report_path = module_root / "reports" / "experiment_report.md"
    write_text(report_path, report_text)

    logger.info("All done. per_pair=%s", all_per_pair_path)
    logger.info("Summary CSV=%s", summary_all_path)
    logger.info("Markdown table=%s", md_table_path)
    logger.info("LaTeX table=%s", tex_table_path)
    logger.info("Report=%s", report_path)
    print(f"[all] per_pair_csv={all_per_pair_path}")
    print(f"[all] summary_csv={summary_all_path}")
    print(f"[all] report={report_path}")


if __name__ == "__main__":
    main()
