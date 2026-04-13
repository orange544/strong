from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.db.plugin_registry import DatabaseSource
from src.db.unified.field_unit import FieldUnit
from src.distributed.contracts import (
    ArtifactType,
    JobStatus,
    LocalConceptQueryRequest,
    LocalJobRequest,
    LocalQueryRequest,
)
from src.distributed.local_agent_service import LocalDomainAgentService


class _FakeIPFSClient:
    def __init__(self) -> None:
        self._store: dict[str, object] = {}
        self._counter = 0

    def add_json(self, obj: object) -> str:
        self._counter += 1
        cid = f"cid_{self._counter}"
        self._store[cid] = obj
        return cid

    def cat_json(self, cid: str) -> object:
        if cid not in self._store:
            raise RuntimeError(f"cid not found: {cid}")
        return self._store[cid]


def _wait_for_terminal_status(service: LocalDomainAgentService, job_id: str) -> JobStatus:
    for _ in range(100):
        status_text = str(service.get_job_status(job_id)["status"])
        status = JobStatus(status_text)
        if status in {JobStatus.SUCCEEDED, JobStatus.FAILED}:
            return status
        time.sleep(0.02)
    raise AssertionError("job did not reach terminal status in time")


def test_local_agent_semantic_only_job_success(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_ipfs = _FakeIPFSClient()

    def _source_loader() -> dict[str, DatabaseSource]:
        return {
            "movie": DatabaseSource(name="movie", driver="sqlite", dsn="ignored.db", options={})
        }

    def _fake_extract_field_units_by_source(
        sources: dict[str, DatabaseSource],
        *,
        extractor: object | None = None,
        max_fields_per_domain: int = 0,
    ) -> dict[str, list[FieldUnit]]:
        del extractor
        del max_fields_per_domain
        assert "movie" in sources
        return {
            "movie": [
                FieldUnit(
                    source_name="movie",
                    database_type="sqlite",
                    container_name="films",
                    field_path="title",
                    original_field="title",
                    field_origin="column",
                    logical_type="text",
                    samples=("Inception", "Interstellar"),
                )
            ]
        }

    monkeypatch.setattr(
        "src.distributed.local_agent_service.extract_field_units_by_source",
        _fake_extract_field_units_by_source,
    )

    service = LocalDomainAgentService(
        node_id="node_a",
        source_loader=_source_loader,
        ipfs_client=fake_ipfs,
        executor_workers=1,
    )

    job_id = service.create_job(
        LocalJobRequest(
            run_id="run_001",
            source_names=("movie",),
            max_fields_per_source=0,
            mock_llm=True,
            share_mode="semantic_only",
        )
    )

    status = _wait_for_terminal_status(service, job_id)
    assert status == JobStatus.SUCCEEDED

    manifest = service.get_job_manifest(job_id)
    assert manifest.node_id == "node_a"
    assert len(manifest.domain_manifests) == 1

    domain_manifest = manifest.domain_manifests[0]
    artifact_types = {item.artifact_type for item in domain_manifest.artifacts}
    assert ArtifactType.FIELD_DESCRIPTIONS in artifact_types
    assert ArtifactType.DOMAIN_UNIFIED in artifact_types
    assert ArtifactType.DOMAIN_KG in artifact_types
    assert ArtifactType.SAMPLES not in artifact_types

    hits = service.execute_local_query(LocalQueryRequest(query_text="title inception", limit=5))
    assert len(hits) >= 1
    assert hits[0].field == "title"

    concept_hits = service.execute_local_concept_query(
        LocalConceptQueryRequest(canonical_names=("title",), limit=5)
    )
    assert len(concept_hits) >= 1
    assert concept_hits[0].canonical_name == "title"
    assert concept_hits[0].field == "title"
    assert len(concept_hits[0].resource_cids) >= 1


def test_local_agent_job_failure_when_source_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_ipfs = _FakeIPFSClient()

    def _source_loader() -> dict[str, DatabaseSource]:
        return {
            "movie": DatabaseSource(name="movie", driver="sqlite", dsn="ignored.db", options={})
        }

    monkeypatch.setattr(
        "src.distributed.local_agent_service.extract_field_units_by_source",
        lambda sources, extractor=None, max_fields_per_domain=0: {},
    )

    service = LocalDomainAgentService(
        node_id="node_b",
        source_loader=_source_loader,
        ipfs_client=fake_ipfs,
        executor_workers=1,
    )

    job_id = service.create_job(
        LocalJobRequest(
            run_id="run_002",
            source_names=("missing_domain",),
            mock_llm=True,
            share_mode="semantic_only",
        )
    )

    status = _wait_for_terminal_status(service, job_id)
    assert status == JobStatus.FAILED

    status_payload = service.get_job_status(job_id)
    assert "source not found" in str(status_payload["error_message"]).lower()


def test_local_agent_uses_llm_domain_unify_when_mock_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = _FakeIPFSClient()
    llm_call_count = {"count": 0}

    def _source_loader() -> dict[str, DatabaseSource]:
        return {
            "movie": DatabaseSource(name="movie", driver="sqlite", dsn="ignored.db", options={})
        }

    def _fake_extract_field_units_by_source(
        sources: dict[str, DatabaseSource],
        *,
        extractor: object | None = None,
        max_fields_per_domain: int = 0,
    ) -> dict[str, list[FieldUnit]]:
        del extractor
        del max_fields_per_domain
        assert "movie" in sources
        return {
            "movie": [
                FieldUnit(
                    source_name="movie",
                    database_type="sqlite",
                    container_name="films",
                    field_path="title",
                    original_field="title",
                    field_origin="column",
                    logical_type="text",
                    samples=("Inception", "Interstellar"),
                ),
                FieldUnit(
                    source_name="movie",
                    database_type="sqlite",
                    container_name="movie_name",
                    field_path="name",
                    original_field="name",
                    field_origin="column",
                    logical_type="text",
                    samples=("Inception", "Interstellar"),
                ),
            ]
        }

    class _FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str) -> None:
            del api_key
            del base_url
            del model_name

        def unify_within_domain(self, field_desc_list: list[dict[str, object]]) -> list[dict[str, object]]:
            llm_call_count["count"] += 1
            assert len(field_desc_list) == 2
            return [
                {
                    "canonical_name": "movie_title",
                    "fields": ["films.title", "movie_name.name", "ghost_table.ghost_field"],
                    "description": "movie title concept",
                }
            ]

    monkeypatch.setattr(
        "src.distributed.local_agent_service.extract_field_units_by_source",
        _fake_extract_field_units_by_source,
    )
    monkeypatch.setattr(
        "src.distributed.local_agent_service.FieldSemanticAgent",
        _FakeFieldSemanticAgent,
    )

    service = LocalDomainAgentService(
        node_id="node_c",
        source_loader=_source_loader,
        ipfs_client=fake_ipfs,
        executor_workers=1,
    )

    # Keep this test focused on within-domain unification; description generation is mocked.
    monkeypatch.setattr(
        service,
        "_generate_descriptions",
        lambda samples, mock_llm: [
            {
                "db_name": "movie",
                "table": "films",
                "field": "title",
                "description": "film title",
            },
            {
                "db_name": "movie",
                "table": "movie_name",
                "field": "name",
                "description": "movie display name",
            },
        ],
    )

    job_id = service.create_job(
        LocalJobRequest(
            run_id="run_003",
            source_names=("movie",),
            max_fields_per_source=0,
            mock_llm=False,
            share_mode="semantic_only",
        )
    )
    status = _wait_for_terminal_status(service, job_id)
    assert status == JobStatus.SUCCEEDED
    assert llm_call_count["count"] == 1

    manifest = service.get_job_manifest(job_id)
    assert len(manifest.domain_manifests) == 1
    domain_manifest = manifest.domain_manifests[0]

    domain_unified_artifact = next(
        artifact
        for artifact in domain_manifest.artifacts
        if artifact.artifact_type == ArtifactType.DOMAIN_UNIFIED
    )
    domain_unified_payload_obj = fake_ipfs.cat_json(domain_unified_artifact.cid)
    assert isinstance(domain_unified_payload_obj, list)
    assert len(domain_unified_payload_obj) == 1
    record = domain_unified_payload_obj[0]
    assert isinstance(record, dict)
    assert record.get("canonical_name") == "movie_title"
    assert record.get("fields") == ["movie.films.title", "movie.movie_name.name"]
