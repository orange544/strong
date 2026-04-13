from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from ...adapters.common import PairSample
from ...utils.pandas_utils import read_csv_safe


def _safe_token(text: str) -> str:
    token = re.sub(r"[^0-9A-Za-z_]+", "_", text).strip("_")
    return token or "pair"


def _normalize_columns(columns: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    normalized: list[str] = []
    for index, raw in enumerate(columns):
        base = str(raw).strip() or f"column_{index + 1}"
        count = seen.get(base, 0)
        seen[base] = count + 1
        if count == 0:
            normalized.append(base)
        else:
            normalized.append(f"{base}_{count + 1}")
    return normalized


def _write_csv_to_sqlite(*, csv_path: Path, db_path: Path, table_name: str) -> tuple[int, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    df = read_csv_safe(csv_path)
    df = df.copy()
    df.columns = _normalize_columns([str(col) for col in df.columns])

    conn = sqlite3.connect(str(db_path))
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    finally:
        conn.close()

    return int(len(df)), int(len(df.columns))


@dataclass(frozen=True)
class PairSQLiteContext:
    run_dir: Path
    source_db_path: Path
    target_db_path: Path
    source_domain_id: str
    target_domain_id: str
    source_rows: int
    source_columns: int
    target_rows: int
    target_columns: int


def build_pair_sqlite_context(
    *,
    pair: PairSample,
    temp_db_root: Path,
) -> PairSQLiteContext:
    dataset_token = _safe_token(pair.dataset_name)
    pair_token = _safe_token(pair.pair_id)
    run_dir = temp_db_root / dataset_token / pair_token
    run_dir.mkdir(parents=True, exist_ok=True)

    source_db_path = run_dir / "source.db"
    target_db_path = run_dir / "target.db"

    source_rows, source_columns = _write_csv_to_sqlite(
        csv_path=pair.source_path,
        db_path=source_db_path,
        table_name=pair.source_table_name,
    )
    target_rows, target_columns = _write_csv_to_sqlite(
        csv_path=pair.target_path,
        db_path=target_db_path,
        table_name=pair.target_table_name,
    )

    source_domain_id = f"{dataset_token}__{pair_token}__source"
    target_domain_id = f"{dataset_token}__{pair_token}__target"
    return PairSQLiteContext(
        run_dir=run_dir,
        source_db_path=source_db_path,
        target_db_path=target_db_path,
        source_domain_id=source_domain_id,
        target_domain_id=target_domain_id,
        source_rows=source_rows,
        source_columns=source_columns,
        target_rows=target_rows,
        target_columns=target_columns,
    )

