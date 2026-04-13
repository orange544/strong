from __future__ import annotations

from typing import Any


def format_float(value: Any, digits: int = 4) -> str:
    if value is None or value == "":
        return ""
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def summary_markdown(rows: list[dict[str, Any]]) -> str:
    header = "| Method | Dataset | P | R | F1 | Runtime(s) | Status | Notes |\n"
    header += "|---|---|---:|---:|---:|---:|---|---|\n"
    body = []
    for row in rows:
        body.append(
            "| {method} | {dataset} | {p} | {r} | {f1} | {rt} | {status} | {notes} |".format(
                method=row.get("method", ""),
                dataset=row.get("dataset", ""),
                p=format_float(row.get("precision")),
                r=format_float(row.get("recall")),
                f1=format_float(row.get("f1")),
                rt=format_float(row.get("runtime_sec"), digits=2),
                status=row.get("status", ""),
                notes=row.get("notes", ""),
            )
        )
    return header + "\n".join(body) + ("\n" if body else "")


def summary_latex(rows: list[dict[str, Any]]) -> str:
    lines = [
        "\\begin{tabular}{l l r r r r l}",
        "\\hline",
        "Method & Dataset & P & R & F1 & Runtime(s) & Status \\\\",
        "\\hline",
    ]
    for row in rows:
        lines.append(
            "{method} & {dataset} & {p} & {r} & {f1} & {rt} & {status} \\\\".format(
                method=str(row.get("method", "")).replace("_", "\\_"),
                dataset=str(row.get("dataset", "")).replace("_", "\\_"),
                p=format_float(row.get("precision")),
                r=format_float(row.get("recall")),
                f1=format_float(row.get("f1")),
                rt=format_float(row.get("runtime_sec"), digits=2),
                status=str(row.get("status", "")).replace("_", "\\_"),
            )
        )
    lines.extend(["\\hline", "\\end{tabular}"])
    return "\n".join(lines) + "\n"

