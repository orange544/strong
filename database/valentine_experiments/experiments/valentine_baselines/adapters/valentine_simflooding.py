from __future__ import annotations

import time
import traceback
from typing import Any

from .common import MethodPrediction, PairSample, normalize_valentine_matches
from ..utils.env_utils import apply_windows_wmi_import_workaround
from ..utils.pandas_utils import read_csv_safe


def run(pair: PairSample, method_cfg: dict[str, Any]) -> MethodPrediction:
    start = time.perf_counter()
    try:
        load_begin = time.perf_counter()
        source_df = read_csv_safe(pair.source_path)
        target_df = read_csv_safe(pair.target_path)
        load_sec = time.perf_counter() - load_begin

        infer_begin = time.perf_counter()
        apply_windows_wmi_import_workaround()
        from valentine import valentine_match
        from valentine.algorithms import SimilarityFlooding

        matcher = SimilarityFlooding(
            coeff_policy=str(method_cfg.get("coeff_policy", "inverse_average")),
            formula=str(method_cfg.get("formula", "formula_c")),
        )
        matches = valentine_match(
            source_df,
            target_df,
            matcher,
            pair.source_table_name,
            pair.target_table_name,
        )
        predictions = normalize_valentine_matches(matches)
        infer_sec = time.perf_counter() - infer_begin
        return MethodPrediction(
            method="simflooding",
            status="ok",
            predictions=predictions,
            supports_ranking=True,
            runtime_sec=time.perf_counter() - start,
            data_loading_sec=load_sec,
            inference_sec=infer_sec,
            metadata={
                "rows": {"source": int(len(source_df)), "target": int(len(target_df))},
                "columns": {
                    "source": list(source_df.columns),
                    "target": list(target_df.columns),
                },
            },
        )
    except Exception as exc:  # pragma: no cover - runtime dependent
        return MethodPrediction(
            method="simflooding",
            status="error",
            runtime_sec=time.perf_counter() - start,
            error_message=f"{type(exc).__name__}: {exc}",
            metadata={"traceback": traceback.format_exc()},
        )
