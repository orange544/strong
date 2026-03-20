# src/storage/registry.py
import json
import os
from typing import Any, cast

from src.configs.config import REGISTRY_PATH


def load_registry() -> dict[str, Any]:
    if os.path.exists(REGISTRY_PATH):
        with open(REGISTRY_PATH, encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return cast(dict[str, Any], payload)
    return {"runs": []}


def save_registry(data: dict[str, Any]) -> None:
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Registry 宸叉洿鏂?{REGISTRY_PATH}")


def append_run_record(record: dict[str, Any]) -> None:
    data = load_registry()
    runs = data.setdefault("runs", [])
    if not isinstance(runs, list):
        runs = []
        data["runs"] = runs
    runs.append(record)
    save_registry(data)


def get_latest_run() -> dict[str, Any] | None:
    data = load_registry()
    raw_runs = data.get("runs", [])
    runs: list[Any] = raw_runs if isinstance(raw_runs, list) else []
    if not runs:
        return None
    latest = runs[-1]
    if isinstance(latest, dict):
        return cast(dict[str, Any], latest)
    return None
