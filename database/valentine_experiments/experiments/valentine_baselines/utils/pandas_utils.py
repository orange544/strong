from __future__ import annotations

from pathlib import Path

import pandas as pd


def drop_duplicate_header_row(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    first_row = [str(v).strip() for v in df.iloc[0].tolist()]
    columns = [str(c).strip() for c in df.columns.tolist()]
    if first_row == columns:
        return df.iloc[1:].reset_index(drop=True)
    return df


def read_csv_safe(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="latin-1")
    return drop_duplicate_header_row(df)

