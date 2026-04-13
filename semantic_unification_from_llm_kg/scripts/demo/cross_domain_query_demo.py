from __future__ import annotations

import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.distributed.contracts import (  # noqa: E402
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
    LocalSubQueryRow,
    NodeJobManifest,
)
from src.distributed.orchestrator_service import GlobalOrchestratorService  # noqa: E402


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


class _DemoLocalAgentClient:
    def __init__(
        self,
        manifest: NodeJobManifest,
        *,
        table_rows: dict[str, list[dict[str, object]]],
    ) -> None:
        self._manifest = manifest
        self._created_job_id = ""
        self._table_rows = table_rows

    def create_job(self, request: LocalJobRequest) -> str:
        del request
        self._created_job_id = f"job_{self._manifest.node_id}"
        return self._created_job_id

    def get_job_status(self, job_id: str) -> JobStatus:
        if job_id != self._created_job_id:
            raise RuntimeError(f"unknown job id: {job_id}")
        return JobStatus.SUCCEEDED

    def get_job_manifest(self, job_id: str) -> NodeJobManifest:
        if job_id != self._created_job_id:
            raise RuntimeError(f"unknown job id: {job_id}")
        return self._manifest

    def execute_local_query(self, request: LocalQueryRequest) -> list[LocalQueryHit]:
        del request
        return []

    def execute_local_concept_query(self, request: LocalConceptQueryRequest) -> list[LocalConceptHit]:
        del request
        return []

    def execute_local_subquery(self, request: LocalSubQueryRequest) -> list[LocalSubQueryRow]:
        normalized = request.normalized()
        source_rows = self._table_rows.get(normalized.table, [])
        output: list[LocalSubQueryRow] = []
        for row in source_rows:
            if not _matches_filters(row=row, request=normalized):
                continue
            projected = {field: row.get(field) for field in normalized.select_fields}
            output.append(
                LocalSubQueryRow(
                    domain_id=normalized.source_name,
                    table=normalized.table,
                    data=projected,
                )
            )
            if len(output) >= normalized.limit:
                break
        return output


def _matches_filters(*, row: dict[str, object], request: LocalSubQueryRequest) -> bool:
    for item in request.filters:
        left = row.get(item.field)
        op = item.operator
        if op == "eq":
            if str(left) != str(item.value):
                return False
            continue
        if op == "neq":
            if str(left) == str(item.value):
                return False
            continue
        if op in {"gt", "gte", "lt", "lte"}:
            left_num = _to_float(left)
            right_num = _to_float(item.value)
            if left_num is None or right_num is None:
                return False
            if op == "gt" and not (left_num > right_num):
                return False
            if op == "gte" and not (left_num >= right_num):
                return False
            if op == "lt" and not (left_num < right_num):
                return False
            if op == "lte" and not (left_num <= right_num):
                return False
            continue
        if op == "like":
            if str(item.value).lower() not in str(left).lower():
                return False
            continue
        if op == "in":
            values = tuple(item.value) if isinstance(item.value, tuple) else ()
            if str(left) not in {str(value) for value in values}:
                return False
            continue
        return False
    return True


def _to_float(value: object) -> float | None:
    try:
        return float(str(value))
    except Exception:  # noqa: BLE001
        return None


def _wait_for_batch(service: GlobalOrchestratorService, batch_id: str) -> None:
    for _ in range(120):
        status_text = str(service.get_batch_status(batch_id)["status"])
        status = JobStatus(status_text)
        if status == JobStatus.SUCCEEDED:
            return
        if status == JobStatus.FAILED:
            raise RuntimeError(f"demo batch failed: {service.get_batch_status(batch_id)}")
        time.sleep(0.05)
    raise RuntimeError("demo batch did not finish in time")


def _domain_manifest(
    *,
    fake_ipfs: _FakeIPFSClient,
    run_id: str,
    node_id: str,
    domain_id: str,
    domain_unified: list[dict[str, object]],
    field_descriptions: list[dict[str, object]],
) -> NodeJobManifest:
    unified_cid = fake_ipfs.add_json(domain_unified)
    desc_cid = fake_ipfs.add_json(field_descriptions)
    return NodeJobManifest(
        schema_version="distributed-manifest/1.0",
        run_id=run_id,
        node_id=node_id,
        job_id=f"job_{node_id}",
        created_at="2026-04-02T00:00:00+00:00",
        status=JobStatus.SUCCEEDED,
        domain_manifests=(
            DomainArtifactManifest(
                schema_version="distributed-manifest/1.0",
                run_id=run_id,
                domain_id=domain_id,
                created_at="2026-04-02T00:00:00+00:00",
                status=JobStatus.SUCCEEDED,
                artifacts=(
                    ArtifactRef(
                        artifact_type=ArtifactType.DOMAIN_UNIFIED,
                        cid=unified_cid,
                        sha256=f"sha_{domain_id}_u",
                        record_count=len(domain_unified),
                    ),
                    ArtifactRef(
                        artifact_type=ArtifactType.FIELD_DESCRIPTIONS,
                        cid=desc_cid,
                        sha256=f"sha_{domain_id}_d",
                        record_count=len(field_descriptions),
                    ),
                ),
            ),
        ),
    )


def build_demo_service() -> GlobalOrchestratorService:
    fake_ipfs = _FakeIPFSClient()
    run_id = "run_demo"

    student_manifest = _domain_manifest(
        fake_ipfs=fake_ipfs,
        run_id=run_id,
        node_id="node_student",
        domain_id="student_registry",
        domain_unified=[
            {
                "db_name": "student_registry",
                "canonical_name": "student_id",
                "fields": ["student_registry.students.student_id"],
                "description": "student identifier",
            },
            {
                "db_name": "student_registry",
                "canonical_name": "name",
                "fields": ["student_registry.students.name"],
                "description": "student name",
            },
            {
                "db_name": "student_registry",
                "canonical_name": "major",
                "fields": ["student_registry.students.major"],
                "description": "major name",
            },
            {
                "db_name": "student_registry",
                "canonical_name": "status",
                "fields": ["student_registry.students.status"],
                "description": "enrollment status",
            },
            {
                "db_name": "student_registry",
                "canonical_name": "event_time",
                "fields": ["student_registry.students.updated_at"],
                "description": "record update time",
            },
        ],
        field_descriptions=[
            {"db_name": "student_registry", "table": "students", "field": "student_id", "description": "id"},
            {"db_name": "student_registry", "table": "students", "field": "name", "description": "name"},
            {"db_name": "student_registry", "table": "students", "field": "major", "description": "major"},
            {"db_name": "student_registry", "table": "students", "field": "status", "description": "status"},
            {
                "db_name": "student_registry",
                "table": "students",
                "field": "updated_at",
                "description": "update time",
            },
        ],
    )

    course_manifest = _domain_manifest(
        fake_ipfs=fake_ipfs,
        run_id=run_id,
        node_id="node_course",
        domain_id="course_selection",
        domain_unified=[
            {
                "db_name": "course_selection",
                "canonical_name": "student_id",
                "fields": ["course_selection.selections.student_code"],
                "description": "student identifier in course system",
            },
            {
                "db_name": "course_selection",
                "canonical_name": "course_name",
                "fields": ["course_selection.selections.course_name"],
                "description": "course name",
            },
            {
                "db_name": "course_selection",
                "canonical_name": "status",
                "fields": ["course_selection.selections.selection_status"],
                "description": "selection status",
            },
            {
                "db_name": "course_selection",
                "canonical_name": "event_time",
                "fields": ["course_selection.selections.event_time"],
                "description": "selection event time",
            },
        ],
        field_descriptions=[
            {"db_name": "course_selection", "table": "selections", "field": "student_code", "description": "id"},
            {
                "db_name": "course_selection",
                "table": "selections",
                "field": "course_name",
                "description": "course",
            },
            {
                "db_name": "course_selection",
                "table": "selections",
                "field": "selection_status",
                "description": "status",
            },
            {
                "db_name": "course_selection",
                "table": "selections",
                "field": "event_time",
                "description": "time",
            },
        ],
    )

    score_manifest = _domain_manifest(
        fake_ipfs=fake_ipfs,
        run_id=run_id,
        node_id="node_score",
        domain_id="score_center",
        domain_unified=[
            {
                "db_name": "score_center",
                "canonical_name": "student_id",
                "fields": ["score_center.scores.stu_no"],
                "description": "student identifier in score system",
            },
            {
                "db_name": "score_center",
                "canonical_name": "course_name",
                "fields": ["score_center.scores.course_name"],
                "description": "course name",
            },
            {
                "db_name": "score_center",
                "canonical_name": "score",
                "fields": ["score_center.scores.score"],
                "description": "numeric score",
            },
            {
                "db_name": "score_center",
                "canonical_name": "event_time",
                "fields": ["score_center.scores.exam_time"],
                "description": "exam time",
            },
        ],
        field_descriptions=[
            {"db_name": "score_center", "table": "scores", "field": "stu_no", "description": "id"},
            {"db_name": "score_center", "table": "scores", "field": "course_name", "description": "course"},
            {"db_name": "score_center", "table": "scores", "field": "score", "description": "score"},
            {"db_name": "score_center", "table": "scores", "field": "exam_time", "description": "time"},
        ],
    )

    student_client = _DemoLocalAgentClient(
        student_manifest,
        table_rows={
            "students": [
                {
                    "student_id": "20230001",
                    "name": "Alice",
                    "major": "ComputerScience",
                    "status": "registered",
                    "updated_at": "2026-03-01T08:00:00",
                },
                {
                    "student_id": "20230002",
                    "name": "Bob",
                    "major": "Mathematics",
                    "status": "registered",
                    "updated_at": "2026-03-01T09:00:00",
                },
            ]
        },
    )
    course_client = _DemoLocalAgentClient(
        course_manifest,
        table_rows={
            "selections": [
                {
                    "student_code": "20230001",
                    "course_name": "DistributedSystems",
                    "selection_status": "selected",
                    "event_time": "2026-03-02T10:00:00",
                },
                {
                    "student_code": "20230001",
                    "course_name": "DataMining",
                    "selection_status": "selected",
                    "event_time": "2026-03-03T10:00:00",
                },
                {
                    "student_code": "20230002",
                    "course_name": "LinearAlgebra",
                    "selection_status": "selected",
                    "event_time": "2026-03-04T10:00:00",
                },
            ]
        },
    )
    score_client = _DemoLocalAgentClient(
        score_manifest,
        table_rows={
            "scores": [
                {
                    "stu_no": "20230001",
                    "course_name": "DistributedSystems",
                    "score": 95,
                    "exam_time": "2026-03-20T10:00:00",
                },
                {
                    "stu_no": "20230001",
                    "course_name": "DataMining",
                    "score": 89,
                    "exam_time": "2026-03-20T15:00:00",
                },
                {
                    "stu_no": "20230002",
                    "course_name": "LinearAlgebra",
                    "score": 91,
                    "exam_time": "2026-03-21T10:00:00",
                },
            ]
        },
    )

    service = GlobalOrchestratorService(ipfs_client=fake_ipfs, executor_workers=1)
    service.register_domain_client("student_registry", student_client)
    service.register_domain_client("course_selection", course_client)
    service.register_domain_client("score_center", score_client)

    batch_id = service.create_batch(
        BatchRequest(
            run_id="run_demo_batch",
            domain_ids=("student_registry", "course_selection", "score_center"),
            poll_interval_sec=0.05,
            poll_timeout_sec=5.0,
        )
    )
    _wait_for_batch(service, batch_id)
    return service


def _print_case(
    *,
    service: GlobalOrchestratorService,
    title: str,
    query_text: str,
    source_domain: str,
    domain_ids: tuple[str, ...],
) -> None:
    payload = service.query_federated_execute(
        query_text=query_text,
        source_domain=source_domain,
        domain_ids=domain_ids,
        limit=20,
    )
    print(f"=== {title} ===")
    print(f"Query: {query_text}")
    print("Result JSON (trimmed):")
    compact_payload = {
        "query": payload.get("query"),
        "query_type": payload.get("intent", {}).get("query_type")
        if isinstance(payload.get("intent"), dict)
        else "",
        "domains_involved": payload.get("domains_involved"),
        "results_count": len(payload.get("results", []))
        if isinstance(payload.get("results"), list)
        else 0,
        "aggregation": payload.get("aggregation", {}),
    }
    print(json.dumps(compact_payload, ensure_ascii=False, indent=2))

    explanations = payload.get("explanations", {})
    markdown = explanations.get("markdown", "") if isinstance(explanations, dict) else ""
    if isinstance(markdown, str) and markdown.strip():
        print("Explanation Markdown:")
        print(markdown)
    print("")


def main() -> None:
    service = build_demo_service()
    cases = [
        (
            "Case 1: Entity Attribute Integration",
            "query name, major, course_name, score of student where student_id = 20230001",
            "student_registry",
            ("student_registry", "course_selection", "score_center"),
        ),
        (
            "Case 2: Path Tracking",
            "trace path of status, event_time of student where student_id = 20230001",
            "student_registry",
            ("student_registry", "course_selection", "score_center"),
        ),
        (
            "Case 3: Aggregation Across Domains",
            "count score and avg score of student where student_id = 20230001",
            "student_registry",
            ("student_registry", "course_selection", "score_center"),
        ),
    ]
    for title, query_text, source_domain, domain_ids in cases:
        _print_case(
            service=service,
            title=title,
            query_text=query_text,
            source_domain=source_domain,
            domain_ids=domain_ids,
        )


if __name__ == "__main__":
    main()
