from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any


def aggregate_method_dataset(per_pair_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in per_pair_rows:
        key = (str(row.get("method", "")), str(row.get("dataset", "")))
        buckets[key].append(row)

    summary: list[dict[str, Any]] = []
    for (method, dataset), rows in sorted(buckets.items()):
        ok_rows = [r for r in rows if r.get("status") == "ok"]
        status = "ok" if ok_rows else "failed"
        precision = mean(float(r.get("precision", 0.0)) for r in ok_rows) if ok_rows else None
        recall = mean(float(r.get("recall", 0.0)) for r in ok_rows) if ok_rows else None
        f1 = mean(float(r.get("f1", 0.0)) for r in ok_rows) if ok_rows else None
        runtime_sec = mean(float(r.get("runtime_sec", 0.0)) for r in ok_rows) if ok_rows else None

        mrr_values = [float(r["mrr"]) for r in ok_rows if r.get("mrr") not in (None, "")]
        rag_values = [float(r["recall_at_gt"]) for r in ok_rows if r.get("recall_at_gt") not in (None, "")]
        mrr = mean(mrr_values) if mrr_values else None
        recall_at_gt = mean(rag_values) if rag_values else None

        fail_notes = [str(r.get("error_message", "")) for r in rows if r.get("status") != "ok"]
        notes = "; ".join([n for n in fail_notes if n][:3])
        summary.append(
            {
                "method": method,
                "dataset": dataset,
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "mrr": mrr,
                "recall_at_gt": recall_at_gt,
                "runtime_sec": runtime_sec,
                "ok_pairs": len(ok_rows),
                "total_pairs": len(rows),
                "status": status,
                "notes": notes,
            }
        )
    return summary

