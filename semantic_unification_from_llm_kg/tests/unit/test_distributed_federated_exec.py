from __future__ import annotations

import sys
import time
from pathlib import Path

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
    LocalSubQueryRequest,
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


class _ExecStubLocalAgentClient:
    def __init__(
        self,
        manifest: NodeJobManifest,
        *,
        status: JobStatus = JobStatus.SUCCEEDED,
        table_rows: dict[str, list[dict[str, object]]] | None = None,
    ) -> None:
        self._manifest = manifest
        self._status = status
        self._created_job_id = ""
        self._table_rows = table_rows or {}

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
        return []

    def execute_local_concept_query(self, request: LocalConceptQueryRequest) -> list[LocalConceptHit]:
        del request
        return []

    def execute_local_subquery(self, request: LocalSubQueryRequest):
        normalized = request.normalized()
        source_rows = self._table_rows.get(normalized.table, [])
        selected_rows: list[dict[str, object]] = []
        for row in source_rows:
            if not _matches_filters(row, normalized.filters):
                continue
            projected: dict[str, object] = {}
            for field in normalized.select_fields:
                if field in row:
                    projected[field] = row[field]
            selected_rows.append(
                {
                    "domain_id": normalized.source_name,
                    "table": normalized.table,
                    "data": projected,
                }
            )
            if len(selected_rows) >= normalized.limit:
                break
        from src.distributed.contracts import LocalSubQueryRow

        return [
            LocalSubQueryRow(
                domain_id=item["domain_id"],
                table=item["table"],
                data=item["data"],
            )
            for item in selected_rows
        ]


def _matches_filters(row: dict[str, object], filters) -> bool:
    for item in filters:
        value = row.get(item.field)
        if item.operator == "eq":
            if str(value) != str(item.value):
                return False
        elif item.operator == "gt":
            if not isinstance(value, int | float) or not isinstance(item.value, int | float):
                return False
            if value <= item.value:
                return False
        elif item.operator == "in":
            values = tuple(item.value) if isinstance(item.value, tuple) else ()
            if str(value) not in {str(v) for v in values}:
                return False
        else:
            return False
    return True


def _wait_for_terminal_status(service: GlobalOrchestratorService, batch_id: str) -> JobStatus:
    for _ in range(120):
        status_text = str(service.get_batch_status(batch_id)["status"])
        status = JobStatus(status_text)
        if status in {JobStatus.SUCCEEDED, JobStatus.FAILED}:
            return status
        time.sleep(0.02)
    raise AssertionError("batch did not reach terminal status in time")


def test_orchestrator_federated_exec_returns_merged_results() -> None:
    fake_ipfs = _FakeIPFSClient()

    domain_a_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_a",
                "canonical_name": "student_id",
                "fields": ["domain_a.students.student_id"],
                "description": "student identifier",
            }
        ]
    )
    domain_b_unified_cid = fake_ipfs.add_json(
        [
            {
                "db_name": "domain_b",
                "canonical_name": "student_id",
                "fields": ["domain_b.scores.student_code"],
                "description": "student identifier",
            }
        ]
    )
    domain_a_desc_cid = fake_ipfs.add_json(
        [{"db_name": "domain_a", "table": "students", "field": "student_id", "description": "id"}]
    )
    domain_b_desc_cid = fake_ipfs.add_json(
        [{"db_name": "domain_b", "table": "scores", "field": "student_code", "description": "id"}]
    )

    manifest_a = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_exec",
        node_id="node_a",
        job_id="job_node_a",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_exec",
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
                    ArtifactRef(
                        artifact_type=ArtifactType.FIELD_DESCRIPTIONS,
                        cid=domain_a_desc_cid,
                        sha256="y",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    manifest_b = NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id="run_exec",
        node_id="node_b",
        job_id="job_node_b",
        created_at="2026-03-23T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id="run_exec",
                domain_id="domain_b",
                created_at="2026-03-23T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=domain_b_unified_cid,
                        sha256="a",
                        record_count=1,
                    ),
                    ArtifactRef(
                        artifact_type=ArtifactType.FIELD_DESCRIPTIONS,
                        cid=domain_b_desc_cid,
                        sha256="b",
                        record_count=1,
                    ),
                ),
            ),
        ),
    )

    client_a = _ExecStubLocalAgentClient(
        manifest_a,
        table_rows={
            "students": [
                {"student_id": "20230001", "name": "Alice"},
                {"student_id": "20230002", "name": "Bob"},
            ]
        },
    )
    client_b = _ExecStubLocalAgentClient(
        manifest_b,
        table_rows={
            "scores": [
                {"student_code": "20230001", "course": "math", "grade": 95},
                {"student_code": "20230002", "course": "english", "grade": 86},
            ]
        },
    )

    service = GlobalOrchestratorService(ipfs_client=fake_ipfs, executor_workers=1)
    service.register_domain_client("domain_a", client_a)
    service.register_domain_client("domain_b", client_b)

    batch_id = service.create_batch(
        BatchRequest(
            run_id="run_exec_global",
            domain_ids=("domain_a", "domain_b"),
            poll_interval_sec=0.01,
            poll_timeout_sec=2.0,
        )
    )
    status = _wait_for_terminal_status(service, batch_id)
    assert status == JobStatus.SUCCEEDED

    payload = service.query_federated_execute(
        query_text="query student_id = 20230001",
        source_domain="domain_a",
        domain_ids=("domain_a", "domain_b"),
        limit=20,
    )

    assert payload["query"] == "query student_id = 20230001"
    domains_involved_obj = payload.get("domains_involved")
    assert isinstance(domains_involved_obj, list)
    assert "domain_a" in domains_involved_obj
    assert "domain_b" in domains_involved_obj

    results_obj = payload.get("results")
    assert isinstance(results_obj, list)
    assert len(results_obj) >= 1
    first_result = results_obj[0]
    assert isinstance(first_result, dict)
    related = first_result.get("related_records")
    assert isinstance(related, list)
    assert len(related) >= 2

    explanations_obj = payload.get("explanations")
    assert isinstance(explanations_obj, dict)
    plan_obj = explanations_obj.get("query_plan")
    assert isinstance(plan_obj, list)
    assert len(plan_obj) >= 2
    markdown_obj = explanations_obj.get("markdown")
    assert isinstance(markdown_obj, str)
    assert "Cross-Domain Query Explanation" in markdown_obj
