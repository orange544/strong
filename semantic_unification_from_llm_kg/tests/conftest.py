from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

import pytest


@dataclass(frozen=True)
class RuntimePaths:
    output_dir: Path
    registry_path: Path


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        values[key] = value.strip().strip("'").strip('"')
    return values


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_ipfs_api(ipfs_api_url: str) -> str:
    api = ipfs_api_url.rstrip("/")
    if api.endswith("/api/v0"):
        return api
    return f"{api}/api/v0"


def ipfs_api_reachable(ipfs_api_url: str, timeout_sec: float = 1.5) -> bool:
    url = f"{_normalize_ipfs_api(ipfs_api_url)}/version"
    request = urllib.request.Request(url=url, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            status_code = int(getattr(response, "status", 0))
            return 200 <= status_code < 300
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def run_cli(
    *,
    command: list[str],
    cwd: Path,
    env: Mapping[str, str],
    timeout_sec: int = 180,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd),
        env=dict(env),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=timeout_sec,
        check=False,
    )


def latest_json_artifact(output_dir: Path, pattern: str) -> Path:
    matches = sorted(output_dir.glob(pattern), key=lambda item: item.stat().st_mtime)
    if not matches:
        raise AssertionError(f"no artifact matched pattern '{pattern}' in '{output_dir}'")
    return matches[-1]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def python_executable() -> str:
    return sys.executable


@pytest.fixture(scope="session")
def minimal_env_template(repo_root: Path) -> dict[str, str]:
    env_file = repo_root / "tests" / "fixtures" / "minimal_env.env"
    return _parse_env_file(env_file)


@pytest.fixture
def runtime_paths(tmp_path: Path) -> RuntimePaths:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    registry_path = tmp_path / "ipfs_registry.json"
    return RuntimePaths(output_dir=output_dir, registry_path=registry_path)


@pytest.fixture
def sqlite_fixture_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "contract_fixture.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE movies (
                movie_id INTEGER PRIMARY KEY,
                title TEXT,
                score REAL,
                notes TEXT
            )
            """
        )
        cursor.executemany(
            "INSERT INTO movies(movie_id, title, score, notes) VALUES (?, ?, ?, ?)",
            [
                (1, "Inception", 8.8, "sci-fi"),
                (2, "Interstellar", 8.6, ""),
                (3, "Memento", 8.4, None),
                (4, "Tenet", 7.3, "NULL"),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


@pytest.fixture
def make_contract_env(
    minimal_env_template: dict[str, str],
    runtime_paths: RuntimePaths,
    sqlite_fixture_db: Path,
) -> Callable[[], dict[str, str]]:
    def _make() -> dict[str, str]:
        env = os.environ.copy()
        env.update(minimal_env_template)
        env["PYTHONUTF8"] = "1"
        env["OUTPUT_DIR"] = str(runtime_paths.output_dir)
        env["REGISTRY_PATH"] = str(runtime_paths.registry_path)
        env["DB_PATHS_JSON"] = json.dumps({"TESTDB": str(sqlite_fixture_db)})
        return env

    return _make


@pytest.fixture(scope="session")
def run_live_contracts() -> bool:
    return _env_flag("RUN_LIVE_CONTRACTS")


@pytest.fixture
def cli_runner(repo_root: Path) -> Callable[[list[str], Mapping[str, str], int], subprocess.CompletedProcess[str]]:
    def _run(command: list[str], env: Mapping[str, str], timeout_sec: int = 180) -> subprocess.CompletedProcess[str]:
        return run_cli(command=command, cwd=repo_root, env=env, timeout_sec=timeout_sec)

    return _run


@pytest.fixture
def artifact_locator() -> Callable[[Path, str], Path]:
    return latest_json_artifact


@pytest.fixture
def ipfs_probe() -> Callable[[str, float], bool]:
    return ipfs_api_reachable
