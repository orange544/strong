import json
import os
from typing import Any

from src.configs.config import OUTPUT_DIR


def save_json(data: Any, filename: str) -> str:
    """Save JSON to configured output directory and return full path."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"保存到文件: {path}")
    return path
