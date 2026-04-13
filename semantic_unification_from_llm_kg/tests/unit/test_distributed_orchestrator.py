from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.distributed.contracts import (
    ArtifactRef,
    ArtifactType,
    BatchRequest,
    DomainArtifactManifest,
    JobStatus,
    LocalConceptHit,
    LocalConceptQueryRequest,
    LocalJobRequest,
    LocalQueryHit,
    LocalQueryRequest,
    NodeJobManifest,
)
from src.distributed.orchestrator_service import GlobalOrchestratorService


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


class _StubLocalAgentClient:
    def __init__(
        self,
        manifest: NodeJobManifest,
        *,
        status: JobStatus = JobStatus.SUCCEEDED,
        query_hits: list[LocalQueryHit] | None = None,
        concept_hits: list[LocalConceptHit] | None = None,
    ) -> None:
        self._manifest = manifest
        self._status = status
        self._created_job_id = ""
        self._query_hits = query_hits or []
        self._concept_hits = concept_hits or []

    def create_job(self, request: LocalJobRequest) -> str:
        assert request.run_id
        self._created_job_id = f"job_{self._manifest.node_id}"
        return self._created_job_id

    def get_job_status(self, job_id: str) -> JobStatus:
        assert job_id == self._created_job_id
        return self._status

    def get_job_manifest(self, job_id: str) -> NodeJobManifest:
        assert job_id == self._created_job_id
        return self._manifest

    def execute_local_query(self, request: LocalQueryRequest) -> list[LocalQueryHit]:
        del request
        return list(self._query_hits)

    def execute_local_concept_query(self, request: LocalConceptQueryRequest) -> list[LocalConceptHit]:
        assert request.limit > 0
        return list(self._concept_hits)


def _wait_for_terminal_status(service: GlobalOrchestratorService, batch_id: str) -> JobStatus:
    for _ in range(120):
        status_text = str(service.get_batch_status(batch_id)["status"])
        status = JobStatus(status_text)
        if status in {JobStatus.SUCCEEDED, JobStatus.FAILED}:
            return status
        time.sleep(0.02)
    raise AssertionError("batch did not reach terminal status in time")


def test_orchestrator_batch_success_builds_alignment_index() -> None:
    fake_ipfs = _FakeIPFSClient()

    domain_a_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_a",
                "canonical_name": "title",
                "fields": ["domain_a.films.title"],
                "description": "movie title",
            }
        ]
    )
    domain_b_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_b",
                "canonical_name": "title",
                "fields": ["domain_b.movie_name.name"],
                "description": "film name",
            }
        ]
    )

    manifest_a = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_a",
        node_id="node_a",
        job_id="job_node_a",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_a",
                domain_id="domain_a",
                created_at="2026-03-23T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=domain_a_unified_cid,
                        sha256="x",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    manifest_b = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_a",
        node_id="node_b",
        job_id="job_node_b",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_a",
                domain_id="domain_b",
                created_at="2026-03-23T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=domain_b_unified_cid,
                        sha256="y",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    service = GlobalOrchestratorService(ipfs_client=fake_ipfs, executor_workers=1)
    service.register_domain_client("domain_a", _StubLocalAgentClient(manifest_a))
    service.register_domain_client("domain_b", _StubLocalAgentClient(manifest_b))

    batch_id = service.create_batch(
        BatchRequest(
            run_id="run_global_001",
            domain_ids=("domain_a", "domain_b"),
            poll_interval_sec=0.01,
            poll_timeout_sec=2.0,
        )
    )

    status = _wait_for_terminal_status(service, batch_id)
    assert status == JobStatus.SUCCEEDED

    manifest = service.get_batch_manifest(batch_id)
    manifest_dict = manifest.to_dict()
    assert manifest_dict["manifest_chain_cid"] == ""
    assert manifest_dict["manifest_chain_key"] == ""
    assert manifest_dict["manifest_tx_hash"] == ""
    assert len(manifest.node_manifests) == 2
    artifact_types = {item.artifact_type for item in manifest.artifacts}
    assert artifact_types == {
        ArtifactType.UNIFIED_FIELDS,
        ArtifactType.ALIGNMENT_INDEX,
        ArtifactType.ALIGNMENT_CYPHER,
    }

    alignment_artifact = next(
        item for item in manifest.artifacts if item.artifact_type == ArtifactType.ALIGNMENT_INDEX
    )
    alignment_payload_obj = fake_ipfs.cat_json(alignment_artifact.cid)
    assert isinstance(alignment_payload_obj, list)
    assert len(alignment_payload_obj) == 1

    alignment_cypher_artifact = next(
        item for item in manifest.artifacts if item.artifact_type == ArtifactType.ALIGNMENT_CYPHER
    )
    alignment_cypher_payload_obj = fake_ipfs.cat_json(alignment_cypher_artifact.cid)
    assert isinstance(alignment_cypher_payload_obj, list)
    assert len(alignment_cypher_payload_obj) >= 1


def test_orchestrator_rejects_unregistered_domain() -> None:
    service = GlobalOrchestratorService(ipfs_client=_FakeIPFSClient(), executor_workers=1)

    try:
        service.create_batch(
            BatchRequest(
                run_id="run_global_002",
                domain_ids=("missing_domain",),
                poll_interval_sec=0.01,
                poll_timeout_sec=1.0,
            )
        )
        raise AssertionError("expected RuntimeError for missing domain registration")
    except RuntimeError as exc:
        assert "unregistered domains" in str(exc)


def test_orchestrator_uses_llm_unify_when_mock_llm_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_ipfs = _FakeIPFSClient()

    domain_a_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_a",
                "canonical_name": "movie_name_local",
                "fields": ["domain_a.films.title"],
                "description": "movie title in domain a",
            }
        ]
    )
    domain_b_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_b",
                "canonical_name": "movie_name_alias",
                "fields": ["domain_b.movie_name.name"],
                "description": "movie title in domain b",
            }
        ]
    )

    manifest_a = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_llm",
        node_id="node_a",
        job_id="job_node_a",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_llm",
                domain_id="domain_a",
                created_at="2026-03-23T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=domain_a_unified_cid,
                        sha256="x",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    manifest_b = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_llm",
        node_id="node_b",
        job_id="job_node_b",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_llm",
                domain_id="domain_b",
                created_at="2026-03-23T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=domain_b_unified_cid,
                        sha256="y",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    class _FakeFieldSemanticAgent:
        def __init__(self, api_key: str, base_url: str, model_name: str) -> None:
            del api_key
            del base_url
            del model_name

        def unify_across_domains(self, domain_items: list[dict[str, object]]) -> list[dict[str, object]]:
            assert len(domain_items) == 2
            return [
                {
                    "canonical_name": "movie_title",
                    "fields": ["domain_a.films.title", "domain_b.movie_name.name"],
                    "description": "unified title concept",
                }
            ]

    monkeypatch.setattr(
        "src.distributed.orchestrator_service.FieldSemanticAgent",
        _FakeFieldSemanticAgent,
    )

    service = GlobalOrchestratorService(ipfs_client=fake_ipfs, executor_workers=1)
    service.register_domain_client("domain_a", _StubLocalAgentClient(manifest_a))
    service.register_domain_client("domain_b", _StubLocalAgentClient(manifest_b))

    batch_id = service.create_batch(
        BatchRequest(
            run_id="run_global_llm_001",
            domain_ids=("domain_a", "domain_b"),
            mock_llm=False,
            poll_interval_sec=0.01,
            poll_timeout_sec=2.0,
        )
    )

    status = _wait_for_terminal_status(service, batch_id)
    assert status == JobStatus.SUCCEEDED

    manifest = service.get_batch_manifest(batch_id)
    alignment_artifact = next(
        item for item in manifest.artifacts if item.artifact_type == ArtifactType.ALIGNMENT_INDEX
    )
    alignment_payload_obj = fake_ipfs.cat_json(alignment_artifact.cid)
    assert isinstance(alignment_payload_obj, list)
    assert len(alignment_payload_obj) == 1
    only_row = alignment_payload_obj[0]
    assert isinstance(only_row, dict)
    assert only_row.get("canonical_name") == "movie_title"


def test_orchestrator_query_prefers_routed_domains() -> None:
    fake_ipfs = _FakeIPFSClient()

    domain_a_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_a",
                "canonical_name": "movie_title",
                "fields": ["domain_a.films.title"],
                "description": "movie title",
            }
        ]
    )
    domain_b_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_b",
                "canonical_name": "movie_title",
                "fields": ["domain_b.movies.name"],
                "description": "movie title",
            }
        ]
    )

    manifest_a = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_q",
        node_id="node_a",
        job_id="job_node_a",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_q",
                domain_id="domain_a",
                created_at="2026-03-23T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=domain_a_unified_cid,
                        sha256="x",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    manifest_b = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_q",
        node_id="node_b",
        job_id="job_node_b",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_q",
                domain_id="domain_b",
                created_at="2026-03-23T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=domain_b_unified_cid,
                        sha256="y",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    domain_a_hits = [
        LocalQueryHit(
            domain_id="domain_a",
            table="films",
            field="title",
            description="title field",
            score=1,
        )
    ]
    domain_b_hits = [
        LocalQueryHit(
            domain_id="domain_b",
            table="movies",
            field="name",
            description="name field",
            score=1,
        )
    ]

    service = GlobalOrchestratorService(ipfs_client=fake_ipfs, executor_workers=1)
    service.register_domain_client(
        "domain_a",
        _StubLocalAgentClient(manifest_a, query_hits=domain_a_hits),
    )
    service.register_domain_client(
        "domain_b",
        _StubLocalAgentClient(manifest_b, query_hits=domain_b_hits),
    )

    batch_id = service.create_batch(
        BatchRequest(
            run_id="run_global_query_001",
            domain_ids=("domain_a", "domain_b"),
            poll_interval_sec=0.01,
            poll_timeout_sec=2.0,
        )
    )
    status = _wait_for_terminal_status(service, batch_id)
    assert status == JobStatus.SUCCEEDED

    query_hits = service.query(query_text="movie_title", limit=5, domain_ids=("domain_a", "domain_b"))
    assert len(query_hits) == 2
    assert query_hits[0].score >= query_hits[1].score


def test_orchestrator_federated_query_returns_anchor_alignment_and_hits() -> None:
    fake_ipfs = _FakeIPFSClient()

    domain_a_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_a",
                "canonical_name": "movie_title",
                "fields": ["domain_a.films.title"],
                "description": "title of the movie",
            }
        ]
    )
    domain_b_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_b",
                "canonical_name": "movie_title",
                "fields": ["domain_b.movies.name"],
                "description": "movie display name",
            }
        ]
    )

    manifest_a = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_fed",
        node_id="node_a",
        job_id="job_node_a",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_fed",
                domain_id="domain_a",
                created_at="2026-03-23T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=domain_a_unified_cid,
                        sha256="x",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    manifest_b = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_fed",
        node_id="node_b",
        job_id="job_node_b",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_fed",
                domain_id="domain_b",
                created_at="2026-03-23T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=domain_b_unified_cid,
                        sha256="y",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    concept_hits_b = [
        LocalConceptHit(
            domain_id="domain_b",
            canonical_name="movie_title",
            table="movies",
            field="name",
            description="movie display name",
            score=8,
            resource_cids=("cid_resource_1",),
        )
    ]

    service = GlobalOrchestratorService(ipfs_client=fake_ipfs, executor_workers=1)
    service.register_domain_client("domain_a", _StubLocalAgentClient(manifest_a))
    service.register_domain_client(
        "domain_b",
        _StubLocalAgentClient(manifest_b, concept_hits=concept_hits_b),
    )

    batch_id = service.create_batch(
        BatchRequest(
            run_id="run_global_fed_001",
            domain_ids=("domain_a", "domain_b"),
            poll_interval_sec=0.01,
            poll_timeout_sec=2.0,
        )
    )
    status = _wait_for_terminal_status(service, batch_id)
    assert status == JobStatus.SUCCEEDED

    result = service.query_federated(
        query_text="movie title",
        source_domain="domain_a",
        target_domain_ids=("domain_b",),
        limit=5,
    )
    anchors_obj = result.get("anchors")
    candidates_obj = result.get("alignment_candidates")
    hits_obj = result.get("hits")

    assert isinstance(anchors_obj, list)
    assert len(anchors_obj) >= 1
    assert isinstance(candidates_obj, list)
    assert len(candidates_obj) >= 1
    assert isinstance(hits_obj, list)
    assert len(hits_obj) == 1
    only_hit = hits_obj[0]
    assert isinstance(only_hit, dict)
    assert only_hit.get("domain_id") == "domain_b"
    assert only_hit.get("canonical_name") == "movie_title"
