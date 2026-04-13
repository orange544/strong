from __future__ import annotations

import ast
import time
import traceback
from pathlib import Path
from typing import Any

from .common import MethodPrediction, PairSample, prediction_row
from ..utils.io import read_json
from ..utils.subprocess_utils import run_subprocess


def _parse_match_key(key_text: str) -> tuple[tuple[str, str], tuple[str, str]] | None:
    try:
        parsed = ast.literal_eval(key_text)
    except Exception:
        return None

    if isinstance(parsed, tuple) and len(parsed) == 2:
        left, right = parsed
        if isinstance(left, tuple) and isinstance(right, tuple) and len(left) == 2 and len(right) == 2:
            return (str(left[0]), str(left[1])), (str(right[0]), str(right[1]))

    if isinstance(parsed, frozenset) and len(parsed) == 2:
        items = list(parsed)
        if all(isinstance(item, tuple) and len(item) == 2 for item in items):
            a = (str(items[0][0]), str(items[0][1]))
            b = (str(items[1][0]), str(items[1][1]))
            return a, b

    return None


def _table_tail(name: str) -> str:
    text = name.strip().replace("\\", "/")
    if not text:
        return ""
    return text.rsplit("/", 1)[-1]


def _is_source_side(table_name: str, source_aliases: set[str], target_aliases: set[str]) -> bool:
    tail = _table_tail(table_name).lower()
    if tail in source_aliases:
        return True
    if tail in target_aliases:
        return False
    if tail.endswith("_source"):
        return True
    if tail.endswith("_target"):
        return False
    return True


def _prediction_file_mode(pair: PairSample, prediction_file: Path) -> MethodPrediction:
    payload = read_json(prediction_file)
    if not isinstance(payload, list):
        raise ValueError("prediction_file must be a JSON list")

    rows = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        rows.append(
            prediction_row(
                str(item["source_table"]),
                str(item["source_column"]),
                str(item["target_table"]),
                str(item["target_column"]),
                float(item.get("score", 1.0)),
            )
        )
    rows.sort(key=lambda x: x["score"], reverse=True)
    return MethodPrediction(
        method="isresmat",
        status="ok",
        predictions=rows,
        supports_ranking=True,
        metadata={
            "mode": "prediction_file",
            "prediction_file": str(prediction_file),
            "pair_id": pair.pair_id,
        },
    )


def run(pair: PairSample, method_cfg: dict[str, Any]) -> MethodPrediction:
    start = time.perf_counter()

    try:
        prediction_file = str(method_cfg.get("prediction_file", "")).strip()
        if prediction_file:
            pred = _prediction_file_mode(pair, Path(prediction_file))
            pred.runtime_sec = time.perf_counter() - start
            pred.inference_sec = pred.runtime_sec
            return pred

        enabled = bool(method_cfg.get("enabled", False))
        if not enabled:
            return MethodPrediction(
                method="isresmat",
                status="not_available",
                runtime_sec=time.perf_counter() - start,
                error_message=(
                    "ISResMat adapter is disabled. Set methods.isresmat.enabled=true and configure repo/runtime."
                ),
            )

        repo_path = Path(str(method_cfg.get("repo_path", "")))
        if not repo_path.exists():
            return MethodPrediction(
                method="isresmat",
                status="not_available",
                runtime_sec=time.perf_counter() - start,
                error_message=f"ISResMat repo path not found: {repo_path}",
            )

        python_exec = str(method_cfg.get("python_exec", "python"))
        store_matches = 1 if bool(method_cfg.get("store_matches", True)) else 0
        comment = str(method_cfg.get("comment", "codex_isresmat"))
        timeout_sec = int(method_cfg.get("timeout_sec", 7200))

        dataset_name = f"{pair.dataset_name}/{pair.pair_id}"
        cmd = [
            python_exec,
            "-m",
            "isresmat",
            "--n-trn-cols",
            str(int(method_cfg.get("n_trn_cols", 200))),
            "--batch-size",
            str(int(method_cfg.get("batch_size", 1))),
            "--num-workers",
            str(int(method_cfg.get("num_workers", 0))),
            "--frag-height",
            str(int(method_cfg.get("frag_height", 6))),
            "--frag-width",
            str(int(method_cfg.get("frag_width", 12))),
            "--learning-rate",
            str(float(method_cfg.get("learning_rate", 3e-5))),
            "--col-name-prob",
            str(float(method_cfg.get("col_name_prob", 0))),
            "--process-mode",
            str(int(method_cfg.get("process_mode", 0))),
            "--store-matches",
            str(store_matches),
            "--comment",
            comment,
            "--dataset-name",
            dataset_name,
            "--orig-file-src",
            str(pair.source_path.resolve()),
            "--orig-file-tgt",
            str(pair.target_path.resolve()),
            "--orig-file-golden-matches",
            str(pair.ground_truth_path.resolve()),
        ]
        extra_args = method_cfg.get("extra_args", [])
        if isinstance(extra_args, list):
            cmd.extend([str(item) for item in extra_args])

        infer_begin = time.perf_counter()
        proc = run_subprocess(cmd, cwd=repo_path, timeout_sec=timeout_sec)
        infer_sec = time.perf_counter() - infer_begin
        if int(proc["returncode"]) != 0:
            return MethodPrediction(
                method="isresmat",
                status="error",
                runtime_sec=time.perf_counter() - start,
                inference_sec=infer_sec,
                error_message=f"ISResMat subprocess failed with code {proc['returncode']}",
                metadata=proc,
            )

        out_file = repo_path / "data" / "output" / comment / f"{dataset_name.replace('/', '-')}.json"
        if not out_file.exists():
            return MethodPrediction(
                method="isresmat",
                status="error",
                runtime_sec=time.perf_counter() - start,
                inference_sec=infer_sec,
                error_message=f"ISResMat output file not found: {out_file}",
                metadata=proc,
            )

        payload = read_json(out_file)
        raw_matches = payload.get("matches")
        if not isinstance(raw_matches, dict):
            return MethodPrediction(
                method="isresmat",
                status="not_available",
                runtime_sec=time.perf_counter() - start,
                inference_sec=infer_sec,
                error_message=(
                    "ISResMat output has no ranked matches. Enable --store-matches=1 for unified evaluation."
                ),
                metadata={"output_file": str(out_file)},
            )

        source_aliases = {
            pair.source_table_name.lower(),
            pair.source_path.stem.lower(),
            _table_tail(pair.source_table_name).lower(),
        }
        target_aliases = {
            pair.target_table_name.lower(),
            pair.target_path.stem.lower(),
            _table_tail(pair.target_table_name).lower(),
        }

        rows = []
        for key, score in raw_matches.items():
            parsed = _parse_match_key(str(key))
            if parsed is None:
                continue
            left, right = parsed
            left_table, left_col = left
            right_table, right_col = right
            if _is_source_side(left_table, source_aliases, target_aliases):
                src_col, tgt_col = left_col, right_col
            else:
                src_col, tgt_col = right_col, left_col
            rows.append(
                prediction_row(
                    pair.source_table_name,
                    src_col,
                    pair.target_table_name,
                    tgt_col,
                    float(score),
                )
            )
        rows.sort(key=lambda x: x["score"], reverse=True)
        return MethodPrediction(
            method="isresmat",
            status="ok",
            predictions=rows,
            supports_ranking=True,
            runtime_sec=time.perf_counter() - start,
            inference_sec=infer_sec,
            metadata={"output_file": str(out_file), "subprocess": proc},
        )
    except Exception as exc:  # pragma: no cover - runtime dependent
        return MethodPrediction(
            method="isresmat",
            status="error",
            runtime_sec=time.perf_counter() - start,
            error_message=f"{type(exc).__name__}: {exc}",
            metadata={"traceback": traceback.format_exc()},
        )
