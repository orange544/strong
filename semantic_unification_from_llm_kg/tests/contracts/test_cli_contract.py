from __future__ import annotations

import json
import subprocess
from collections.abc import Callable, Mapping
from pathlib import Path

import pytest

CliRunner = Callable[[list[str], Mapping[str, str], int], subprocess.CompletedProcess[str]]
EnvFactory = Callable[[], dict[str, str]]
ArtifactLocator = Callable[[Path, str], Path]
IpfsProbe = Callable[[str, float], bool]


def _assert_required_keys(payload: dict[str, object], required_keys: set[str]) -> None:
    missing = required_keys.difference(payload.keys())
    assert not missing, f"missing keys: {sorted(missing)}"


def test_sample_mode_contract_offline(
    python_executable: str,
    make_contract_env: EnvFactory,
    cli_runner: CliRunner,
    artifact_locator: ArtifactLocator,
) -> None:
    env = make_contract_env()
    result = cli_runner([python_executable, "main.py", "--mode", "sample"], env, 120)
    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    assert "Sampling completed:" in result.stdout

    output_dir = Path(env["OUTPUT_DIR"])
    sample_file = artifact_locator(output_dir, "samples_*.json")
    sample_payload = json.loads(sample_file.read_text(encoding="utf-8"))

    assert isinstance(sample_payload, list)
    assert len(sample_payload) >= 1
    first_item = sample_payload[0]
    assert isinstance(first_item, dict)
    _assert_required_keys(
        first_item,
        {"table", "field", "type", "samples", "db_name"},
    )
    assert first_item["db_name"] == "TESTDB"


def test_domain_share_invalid_domain_contract_offline(
    python_executable: str,
    make_contract_env: EnvFactory,
    cli_runner: CliRunner,
) -> None:
    env = make_contract_env()
    result = cli_runner(
        [
            python_executable,
            "run_domain_share.py",
            "--domain",
            "NOT_EXISTS",
            "--mock-llm",
            "--skip-chain",
            "--max-fields-per-domain",
            "5",
        ],
        env,
        120,
    )
    assert result.returncode != 0
    merged = f"{result.stdout}\n{result.stderr}"
    assert "No domains matched --domain values" in merged
    assert "Available:" in merged


def test_domain_share_contract_live(
    python_executable: str,
    make_contract_env: EnvFactory,
    run_live_contracts: bool,
    cli_runner: CliRunner,
    artifact_locator: ArtifactLocator,
    ipfs_probe: IpfsProbe,
) -> None:
    if not run_live_contracts:
        pytest.skip("set RUN_LIVE_CONTRACTS=1 to execute live contract tests")

    env = make_contract_env()
    ipfs_api = env.get("IPFS_API_URL", "http://127.0.0.1:5001/api/v0")
    if not ipfs_probe(ipfs_api, 1.5):
        pytest.skip(f"IPFS API is unreachable: {ipfs_api}")

    result = cli_runner(
        [
            python_executable,
            "run_domain_share.py",
            "--domain",
            "TESTDB",
            "--mock-llm",
            "--skip-chain",
            "--max-fields-per-domain",
            "3",
        ],
        env,
        180,
    )
    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    output_dir = Path(env["OUTPUT_DIR"])
    manifest_file = artifact_locator(output_dir, "domain_share_manifest_*.json")
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))

    assert isinstance(manifest, dict)
    _assert_required_keys(manifest, {"mode", "domains"})
    assert manifest["mode"] == "domain_sample_description_to_ipfs_and_chain"
    assert isinstance(manifest["domains"], list)
    assert len(manifest["domains"]) == 1
    domain_entry = manifest["domains"][0]
    assert isinstance(domain_entry, dict)
    _assert_required_keys(
        domain_entry,
        {
            "domain",
            "status",
            "samples_file",
            "field_descriptions_file",
            "samples_cid",
            "field_descriptions_cid",
        },
    )
    assert domain_entry["domain"] == "TESTDB"
    assert domain_entry["status"] == "completed"


def test_all_mode_contract_live(
    python_executable: str,
    make_contract_env: EnvFactory,
    run_live_contracts: bool,
    cli_runner: CliRunner,
    artifact_locator: ArtifactLocator,
    ipfs_probe: IpfsProbe,
) -> None:
    if not run_live_contracts:
        pytest.skip("set RUN_LIVE_CONTRACTS=1 to execute live contract tests")

    env = make_contract_env()
    ipfs_api = env.get("IPFS_API_URL", "http://127.0.0.1:5001/api/v0")
    if not ipfs_probe(ipfs_api, 1.5):
        pytest.skip(f"IPFS API is unreachable: {ipfs_api}")

    result = cli_runner([python_executable, "main.py", "--mode", "all"], env, 300)
    assert result.returncode == 0, result.stdout + "\n" + result.stderr

    output_dir = Path(env["OUTPUT_DIR"])
    unified_file = artifact_locator(output_dir, "unified_fields_*.json")
    cypher_file = artifact_locator(output_dir, "cypher_*.json")

    unified_payload = json.loads(unified_file.read_text(encoding="utf-8"))
    cypher_payload = json.loads(cypher_file.read_text(encoding="utf-8"))

    assert isinstance(unified_payload, list)
    assert isinstance(cypher_payload, list)
