from __future__ import annotations

import json
import os
from pathlib import Path

from src.configs.dotenv_loader import load_dotenv_file
from src.db.plugin_registry import DatabaseSource, load_db_sources_from_env

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOTENV_PATH = PROJECT_ROOT / ".env"
load_dotenv_file(DOTENV_PATH)


def _as_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _as_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        return float(raw)
    except ValueError:
        return default


def _as_optional_float(name: str, default: float) -> float | None:
    value = _as_float(name, default)
    if value <= 0:
        return None
    return value


def _as_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default

    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _load_db_paths() -> dict[str, str]:
    raw_json = os.getenv("DB_PATHS_JSON", "").strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                normalized = {
                    str(k): str(v)
                    for k, v in parsed.items()
                    if str(k).strip() and str(v).strip()
                }
                if normalized:
                    return normalized
        except json.JSONDecodeError:
            pass

    # Prefix-based config: DB_PATH_<DOMAIN>=path/to/db
    prefixed = {
        key.removeprefix("DB_PATH_"): value
        for key, value in os.environ.items()
        if key.startswith("DB_PATH_") and key != "DB_PATHS_JSON" and value.strip()
    }
    if prefixed:
        return prefixed

    # Last fallback
    return {
        "IMDB": "data/dbs/DBDB.db",
        "TMDB": "data/dbs/TMDB.db",
    }


# ---------- Storage / IPFS ----------
IPFS_API_URL = os.getenv("IPFS_API_URL", "http://127.0.0.1:5001/api/v0")
IPFS_HTTP_TIMEOUT_SEC = _as_int("IPFS_HTTP_TIMEOUT_SEC", 30)
REGISTRY_PATH = os.getenv("REGISTRY_PATH", "ipfs_registry.json")


# ---------- Paths ----------
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "outputs")


# ---------- Database Domains ----------
DB_PATHS = _load_db_paths()
DB_SOURCES: dict[str, DatabaseSource] = load_db_sources_from_env(legacy_db_paths=DB_PATHS)


# ---------- Database Sampling ----------
DB_SAMPLE_RATIO = _as_float("DB_SAMPLE_RATIO", 0.02)
DB_SAMPLE_MIN = max(1, _as_int("DB_SAMPLE_MIN", 10))
DB_SAMPLE_MAX = max(DB_SAMPLE_MIN, _as_int("DB_SAMPLE_MAX", 20))


# ---------- LLM ----------
LLM_CONFIG = {
    "api_key": os.getenv("LLM_API_KEY", ""),
    "base_url": os.getenv("LLM_BASE_URL", "http://127.0.0.1:1234/v1"),
    "model_name": os.getenv("LLM_MODEL_NAME", "qwen3.5-4b"),
}
LLM_DESC_CONFIG = {
    "api_key": os.getenv("LLM_DESC_API_KEY", LLM_CONFIG["api_key"]),
    "base_url": os.getenv("LLM_DESC_BASE_URL", LLM_CONFIG["base_url"]),
    "model_name": os.getenv("LLM_DESC_MODEL_NAME", LLM_CONFIG["model_name"]),
}
LLM_UNIFY_CONFIG = {
    "api_key": os.getenv("LLM_UNIFY_API_KEY", LLM_DESC_CONFIG["api_key"]),
    "base_url": os.getenv("LLM_UNIFY_BASE_URL", LLM_DESC_CONFIG["base_url"]),
    "model_name": os.getenv("LLM_UNIFY_MODEL_NAME", LLM_DESC_CONFIG["model_name"]),
}
LLM_REQUEST_TIMEOUT_SEC = _as_optional_float("LLM_REQUEST_TIMEOUT_SEC", 45.0)
LLM_UNIFY_TIMEOUT_SEC = _as_optional_float("LLM_UNIFY_TIMEOUT_SEC", 120.0)
LLM_MAX_RETRIES = _as_int("LLM_MAX_RETRIES", 1)
LLM_DESCRIPTION_MAX_TOKENS = _as_int("LLM_DESCRIPTION_MAX_TOKENS", 96)
LLM_UNIFY_MAX_TOKENS = _as_int("LLM_UNIFY_MAX_TOKENS", 1024)


# ---------- Pipeline Runtime ----------
PIPELINE_CONFIG = {
    "llm_desc_max_workers": max(1, _as_int("LLM_DESC_MAX_WORKERS", 1)),
    "llm_desc_domain_timeout_sec": max(30, _as_int("LLM_DESC_DOMAIN_TIMEOUT", 900)),
    "run_max_fields_per_domain": max(0, _as_int("RUN_MAX_FIELDS_PER_DOMAIN", 0)),
    "run_preflight_enabled": _as_bool("RUN_PREFLIGHT_ENABLED", True),
    "run_preflight_check_sqlite_path": _as_bool("RUN_PREFLIGHT_CHECK_SQLITE_PATH", True),
    "run_preflight_check_tcp": _as_bool("RUN_PREFLIGHT_CHECK_TCP", False),
    "run_preflight_tcp_timeout_sec": max(0.2, _as_float("RUN_PREFLIGHT_TCP_TIMEOUT_SEC", 2.0)),
}


# ---------- Domain Share / Chain ----------
DOMAIN_SHARE_DEFAULTS = {
    "ipfs_chain_bin": os.getenv(
        "IPFS_CHAIN_BIN",
        r"D:\Program Files\BISHE\program\blockchain\go-norn-main\bin\ipfs-chain.exe",
    ),
    "go_norn_root": os.getenv(
        "GO_NORN_ROOT",
        r"D:\Program Files\BISHE\program\blockchain\go-norn-main",
    ),
    "receiver": os.getenv(
        "CHAIN_RECEIVER_ADDRESS",
        "f5c5822480a49523033fca24eb35bb5b8238b70d",
    ),
    "rpc_addr": os.getenv("CHAIN_RPC_ADDR", "127.0.0.1:45558"),
    "ipfs_api": os.getenv("CHAIN_IPFS_API", IPFS_API_URL.replace("/api/v0", "")),
    "timeout_sec": max(3, _as_int("CHAIN_TX_TIMEOUT_SEC", 12)),
}


# ---------- Auto Monitor ----------
AUTO_PIPELINE_DEFAULTS = {
    "db_folder": os.getenv("AUTO_DB_FOLDER", "data/dbs/"),
    "previous_unified_fields_cid": os.getenv("AUTO_PREVIOUS_UNIFIED_FIELDS_CID", ""),
    "poll_interval_sec": max(1, _as_int("AUTO_POLL_INTERVAL_SEC", 10)),
}
