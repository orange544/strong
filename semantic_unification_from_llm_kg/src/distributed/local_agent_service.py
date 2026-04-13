from __future__ import annotations

import hashlib
import json
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from src.configs.config import DB_PATHS, LLM_DESC_CONFIG, LLM_UNIFY_CONFIG, PIPELINE_CONFIG
from src.db.plugin_registry import DatabaseSource, load_db_sources_from_env
from src.distributed.contracts import (
    ArtifactRef,
    ArtifactType,
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
    ShareMode,
    utc_now_iso,
    validate_token,
)
from src.kg.kg_agent import KnowledgeGraphAgent
from src.llm.description_agent import FieldDescriptionAgent
from src.llm.semantic import FieldSemanticAgent
from src.pipeline.orchestration_common import (
    DESCRIPTION_FAILED,
    generate_descriptions_parallel,
)
from src.pipeline.unified_interface import (
    build_db_data_from_field_units,
    extract_field_units_by_source,
    field_units_to_sample_records,
)
from src.query.query_executor import create_query_executor
from src.storage.ipfs_client import IPFSClient

SampleRecord = dict[str, object]
DescriptionRecord = dict[str, object]
UnifiedRecord = dict[str, object]


@dataclass
class _LocalJobState:
    request: LocalJobRequest
    status: JobStatus
    created_at: str
    started_at: str = ""
    finished_at: str = ""
    error_message: str = ""
    manifest: NodeJobManifest | None = None


class LocalDomainAgentService:
    """
    Local execution node:
    - reads local DB sources
    - executes sampling and field description locally
    - publishes semantic artifacts to IPFS
    - returns node manifest to orchestrator
    """

    schema_version = "distributed-manifest/1.0"

    def __init__(
        self,
        *,
        node_id: str,
        source_loader: Callable[[], dict[str, DatabaseSource]] | None = None,
        ipfs_client: IPFSClient | None = None,
        executor_workers: int = 2,
    ) -> None:
        self.node_id = validate_token(node_id, context="node_id")
        self._source_loader = source_loader or self._default_source_loader
        self._ipfs = ipfs_client or IPFSClient()
        self._jobs: dict[str, _LocalJobState] = {}
        self._latest_successful_manifest: NodeJobManifest | None = None
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max(1, executor_workers))

    @staticmethod
    def _default_source_loader() -> dict[str, DatabaseSource]:
        loaded = load_db_sources_from_env(legacy_db_paths=DB_PATHS)
        normalized: dict[str, DatabaseSource] = {}
        for name, source in loaded.items():
            normalized_name = validate_token(name, context="source_name")
            normalized[normalized_name] = source
        return normalized

    def create_job(self, request: LocalJobRequest) -> str:
        normalized_request = request.normalized()
        job_id = str(validate_token(uuid.uuid4().hex[:24], context="job_id"))
        with self._lock:
            self._jobs[job_id] = _LocalJobState(
                request=normalized_request,
                status=JobStatus.PENDING,
                created_at=utc_now_iso(),
            )
        self._executor.submit(self._run_job, job_id)
        return job_id

    def get_job_status(self, job_id: str) -> dict[str, object]:
        normalized_job_id = validate_token(job_id, context="job_id")
        state = self._get_job_state(normalized_job_id)
        return {
            "job_id": normalized_job_id,
            "status": state.status.value,
            "created_at": state.created_at,
            "started_at": state.started_at,
            "finished_at": state.finished_at,
            "error_message": state.error_message,
        }

    def get_job_manifest(self, job_id: str) -> NodeJobManifest:
        normalized_job_id = validate_token(job_id, context="job_id")
        state = self._get_job_state(normalized_job_id)
        if state.manifest is None:
            raise RuntimeError(f"manifest not ready for job: {normalized_job_id}")
        return state.manifest

    def execute_local_query(self, request: LocalQueryRequest) -> list[LocalQueryHit]:
        normalized_request = request.normalized()
        with self._lock:
            manifest = self._latest_successful_manifest
        if manifest is None:
            raise RuntimeError("no successful local job manifest available")

        query_tokens = _query_tokens(normalized_request.query_text)
        hits: list[LocalQueryHit] = []

        for domain_manifest in manifest.domain_manifests:
            if (
                normalized_request.source_name is not None
                and domain_manifest.domain_id != normalized_request.source_name
            ):
                continue
            desc_cid = _artifact_cid(domain_manifest, ArtifactType.FIELD_DESCRIPTIONS)
            if not desc_cid:
                continue
            payload = self._ipfs.cat_json(desc_cid)
            descriptions = _coerce_description_records(payload)
            hits.extend(
                _search_descriptions(
                    domain_id=domain_manifest.domain_id,
                    query_tokens=query_tokens,
                    descriptions=descriptions,
                    limit=normalized_request.limit,
                )
            )

        hits.sort(key=lambda item: item.score, reverse=True)
        return hits[: normalized_request.limit]

    def execute_local_concept_query(
        self,
        request: LocalConceptQueryRequest,
    ) -> list[LocalConceptHit]:
        normalized_request = request.normalized()
        with self._lock:
            manifest = self._latest_successful_manifest
        if manifest is None:
            raise RuntimeError("no successful local job manifest available")

        requested = set(normalized_request.canonical_names)
        hits: list[LocalConceptHit] = []

        for domain_manifest in manifest.domain_manifests:
            if (
                normalized_request.source_name is not None
                and domain_manifest.domain_id != normalized_request.source_name
            ):
                continue

            unified_cid = _artifact_cid(domain_manifest, ArtifactType.DOMAIN_UNIFIED)
            if not unified_cid:
                continue
            desc_cid = _artifact_cid(domain_manifest, ArtifactType.FIELD_DESCRIPTIONS)
            descriptions = _coerce_description_records(self._ipfs.cat_json(desc_cid)) if desc_cid else []
            domain_unified = _coerce_stored_domain_unified_records(
                payload=self._ipfs.cat_json(unified_cid),
                domain_id=domain_manifest.domain_id,
            )
            resource_cids = _query_resource_cids(domain_manifest)
            hits.extend(
                _expand_domain_concepts(
                    domain_id=domain_manifest.domain_id,
                    requested_concepts=requested,
                    domain_unified=domain_unified,
                    descriptions=descriptions,
                    resource_cids=resource_cids,
                    limit=normalized_request.limit,
                )
            )

        hits.sort(key=lambda item: item.score, reverse=True)
        return hits[: normalized_request.limit]

    def execute_local_subquery(
        self,
        request: LocalSubQueryRequest,
    ) -> list[LocalSubQueryRow]:
        normalized_request = request.normalized()
        sources = self._source_loader()
        source = sources.get(normalized_request.source_name)
        if source is None:
            raise RuntimeError(f"source not found in local node: {normalized_request.source_name}")

        executor = create_query_executor(source)
        rows = executor.execute(
            table=normalized_request.table,
            select_fields=normalized_request.select_fields,
            filters=normalized_request.filters,
            limit=normalized_request.limit,
        )
        output: list[LocalSubQueryRow] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            output.append(
                LocalSubQueryRow(
                    domain_id=normalized_request.source_name,
                    table=normalized_request.table,
                    data=row,
                )
            )
        return output

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            state = self._jobs[job_id]
            state.status = JobStatus.RUNNING
            state.started_at = utc_now_iso()

        try:
            manifest = self._execute_pipeline(job_id, state.request)
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                state.status = JobStatus.FAILED
                state.finished_at = utc_now_iso()
                state.error_message = str(exc)
            return

        with self._lock:
            state.status = JobStatus.SUCCEEDED
            state.finished_at = utc_now_iso()
            state.manifest = manifest
            self._latest_successful_manifest = manifest

    def _execute_pipeline(
        self,
        job_id: str,
        request: LocalJobRequest,
    ) -> NodeJobManifest:
        all_sources = self._source_loader()
        selected_sources = _select_sources(
            all_sources=all_sources,
            source_names=request.source_names,
        )

        extracted = extract_field_units_by_source(
            selected_sources,
            max_fields_per_domain=request.max_fields_per_source,
        )

        domain_manifests: list[DomainArtifactManifest] = []
        for domain_id, source in selected_sources.items():
            units = extracted.get(domain_id, [])
            samples = field_units_to_sample_records(units)
            for sample in samples:
                sample["db_name"] = domain_id

            descriptions = self._generate_descriptions(samples, request.mock_llm)
            domain_unified = self._generate_domain_unified(
                domain_id=domain_id,
                descriptions=descriptions,
                mock_llm=request.mock_llm,
            )
            domain_kg = _build_domain_kg(
                domain_id=domain_id,
                units_by_domain={domain_id: units},
                descriptions=descriptions,
                domain_unified=domain_unified,
            )

            artifacts: list[ArtifactRef] = []
            if request.share_mode == "include_samples":
                artifacts.append(
                    self._publish_artifact(
                        payload=samples,
                        artifact_type=ArtifactType.SAMPLES,
                        share_mode="include_samples",
                    )
                )
            artifacts.append(
                self._publish_artifact(
                    payload=descriptions,
                    artifact_type=ArtifactType.FIELD_DESCRIPTIONS,
                    share_mode="semantic_only",
                )
            )
            artifacts.append(
                self._publish_artifact(
                    payload=domain_unified,
                    artifact_type=ArtifactType.DOMAIN_UNIFIED,
                    share_mode="semantic_only",
                )
            )
            artifacts.append(
                self._publish_artifact(
                    payload=domain_kg,
                    artifact_type=ArtifactType.DOMAIN_KG,
                    share_mode="semantic_only",
                )
            )

            domain_manifests.append(
                DomainArtifactManifest(
                    schema_version=self.schema_version,
                    run_id=request.run_id,
                    domain_id=domain_id,
                    created_at=utc_now_iso(),
                    status=JobStatus.SUCCEEDED,
                    artifacts=tuple(artifacts),
                )
            )

            # Safety check: local DSN is used internally only and must never be emitted.
            _ = source.dsn

        manifest = NodeJobManifest(
            schema_version=self.schema_version,
            run_id=request.run_id,
            node_id=self.node_id,
            job_id=job_id,
            created_at=utc_now_iso(),
            status=JobStatus.SUCCEEDED,
            domain_manifests=tuple(domain_manifests),
        )
        self._publish_artifact(
            payload=manifest.to_dict(),
            artifact_type=ArtifactType.NODE_MANIFEST,
            share_mode="semantic_only",
        )
        return manifest

    def _publish_artifact(
        self,
        *,
        payload: object,
        artifact_type: ArtifactType,
        share_mode: ShareMode,
    ) -> ArtifactRef:
        cid = self._ipfs.add_json(payload)
        sha256 = _sha256_payload(payload)
        record_count = _record_count(payload)
        return ArtifactRef(
            artifact_type=artifact_type,
            cid=cid,
            sha256=sha256,
            record_count=record_count,
            security_level=share_mode,
        )

    def _generate_descriptions(
        self,
        samples: list[SampleRecord],
        mock_llm: bool,
    ) -> list[DescriptionRecord]:
        if mock_llm:
            return [_mock_description(sample) for sample in samples]

        fd_agent = FieldDescriptionAgent(
            api_key=LLM_DESC_CONFIG["api_key"],
            base_url=LLM_DESC_CONFIG["base_url"],
            model_name=LLM_DESC_CONFIG["model_name"],
        )
        max_workers_obj = PIPELINE_CONFIG.get("llm_desc_max_workers", 1)
        timeout_obj = PIPELINE_CONFIG.get("llm_desc_domain_timeout_sec", 300)
        if not isinstance(max_workers_obj, int) or max_workers_obj <= 0:
            raise RuntimeError("PIPELINE_CONFIG['llm_desc_max_workers'] must be a positive integer")
        if not isinstance(timeout_obj, int) or timeout_obj <= 0:
            raise RuntimeError("PIPELINE_CONFIG['llm_desc_domain_timeout_sec'] must be a positive integer")

        raw_output = generate_descriptions_parallel(
            fd_agent=fd_agent,
            samples=samples,
            max_workers=max_workers_obj,
            domain_timeout_sec=timeout_obj,
        )
        return _coerce_description_records(raw_output)

    def _generate_domain_unified(
        self,
        *,
        domain_id: str,
        descriptions: list[DescriptionRecord],
        mock_llm: bool,
    ) -> list[UnifiedRecord]:
        if mock_llm:
            return _build_mock_domain_unified(domain_id=domain_id, descriptions=descriptions)

        fs_agent = FieldSemanticAgent(
            api_key=LLM_UNIFY_CONFIG["api_key"],
            base_url=LLM_UNIFY_CONFIG["base_url"],
            model_name=LLM_UNIFY_CONFIG["model_name"],
        )
        raw_output = fs_agent.unify_within_domain(descriptions)
        domain_unified = _coerce_domain_unified_records(
            payload=raw_output,
            domain_id=domain_id,
            descriptions=descriptions,
        )
        if not domain_unified:
            return _build_mock_domain_unified(domain_id=domain_id, descriptions=descriptions)
        return domain_unified

    def _get_job_state(self, job_id: str) -> _LocalJobState:
        with self._lock:
            state = self._jobs.get(job_id)
        if state is None:
            raise RuntimeError(f"job not found: {job_id}")
        return state


def _select_sources(
    *,
    all_sources: dict[str, DatabaseSource],
    source_names: tuple[str, ...],
) -> dict[str, DatabaseSource]:
    if not all_sources:
        raise RuntimeError("no database sources configured for local node")
    if not source_names:
        return all_sources

    selected: dict[str, DatabaseSource] = {}
    for name in source_names:
        source = all_sources.get(name)
        if source is None:
            raise RuntimeError(f"source not found in local node: {name}")
        selected[name] = source
    return selected


def _record_count(payload: object) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        return len(payload)
    return 1


def _sha256_payload(payload: object) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _mock_description(sample: SampleRecord) -> DescriptionRecord:
    table = str(sample.get("table", "")).strip()
    field = str(sample.get("field", "")).strip()
    db_name = str(sample.get("db_name", "")).strip()
    values_obj = sample.get("samples", [])
    sample_count = len(values_obj) if isinstance(values_obj, list) else 0
    if not table or not field or not db_name:
        raise RuntimeError("sample record must contain non-empty db_name/table/field")
    return {
        "db_name": db_name,
        "table": table,
        "field": field,
        "description": f"{field} in {table} of {db_name}, inferred from {sample_count} sample values.",
    }


def _build_mock_domain_unified(
    *,
    domain_id: str,
    descriptions: list[DescriptionRecord],
) -> list[UnifiedRecord]:
    grouped: dict[str, list[str]] = {}
    description_by_canonical: dict[str, str] = {}

    for item in descriptions:
        table = str(item.get("table", "")).strip()
        field = str(item.get("field", "")).strip()
        description = str(item.get("description", "")).strip()
        if not table or not field:
            continue
        canonical = field.lower()
        ref = f"{domain_id}.{table}.{field}"
        grouped.setdefault(canonical, []).append(ref)
        if canonical not in description_by_canonical:
            description_by_canonical[canonical] = description

    output: list[UnifiedRecord] = []
    for canonical_name, refs in grouped.items():
        output.append(
            {
                "db_name": domain_id,
                "canonical_name": canonical_name,
                "fields": refs,
                "description": description_by_canonical.get(canonical_name, ""),
            }
        )
    return output


def _build_domain_kg(
    *,
    domain_id: str,
    units_by_domain: dict[str, object],
    descriptions: list[DescriptionRecord],
    domain_unified: list[UnifiedRecord],
) -> list[str]:
    db_data = build_db_data_from_field_units(units_by_domain)
    kg_agent = KnowledgeGraphAgent()
    run_record = {
        "domains": [
            {
                "db_name": domain_id,
                "sample_file": "",
                "field_descriptions_file": "",
                "domain_unified_file": "",
                "samples_cid": "",
                "field_descriptions_cid": "",
                "domain_unified_cid": "",
            }
        ]
    }
    statements = kg_agent.generate_domain_kg_cypher(
        run_record=run_record,
        db_name=domain_id,
        tables_data=db_data.get(domain_id, {}),
        field_descs=descriptions,
        domain_unified=domain_unified,
    )
    return [str(item) for item in statements]


def _artifact_cid(manifest: DomainArtifactManifest, artifact_type: ArtifactType) -> str:
    for artifact in manifest.artifacts:
        if artifact.artifact_type == artifact_type:
            return str(artifact.cid)
    return ""


def _query_tokens(query_text: str) -> set[str]:
    return {token for token in query_text.lower().split() if token}


def _coerce_description_records(payload: object) -> list[DescriptionRecord]:
    if not isinstance(payload, list):
        raise RuntimeError("field descriptions payload must be a list")
    descriptions: list[DescriptionRecord] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        db_name = item.get("db_name")
        table = item.get("table")
        field = item.get("field")
        description = item.get("description")
        if not isinstance(db_name, str):
            continue
        if not isinstance(table, str):
            continue
        if not isinstance(field, str):
            continue
        if not isinstance(description, str):
            continue
        descriptions.append(
            {
                "db_name": db_name,
                "table": table,
                "field": field,
                "description": description,
            }
        )
    return descriptions


def _coerce_stored_domain_unified_records(
    *,
    payload: object,
    domain_id: str,
) -> list[UnifiedRecord]:
    if not isinstance(payload, list):
        return []

    output: list[UnifiedRecord] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        canonical_name_obj = item.get("canonical_name")
        fields_obj = item.get("fields")
        description_obj = item.get("description", "")
        db_name_obj = item.get("db_name", domain_id)

        if not isinstance(canonical_name_obj, str):
            continue
        canonical_name = canonical_name_obj.strip().lower()
        if not canonical_name:
            continue
        if not isinstance(db_name_obj, str):
            continue
        db_name = db_name_obj.strip() or domain_id
        if db_name != domain_id:
            continue
        if not isinstance(fields_obj, list):
            continue
        description = description_obj.strip() if isinstance(description_obj, str) else ""

        fields: list[str] = []
        seen: set[str] = set()
        for raw in fields_obj:
            normalized = _normalize_stored_field_ref(raw, domain_id=domain_id)
            if not normalized:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            fields.append(normalized)
        if not fields:
            continue

        output.append(
            {
                "db_name": domain_id,
                "canonical_name": canonical_name,
                "fields": fields,
                "description": description,
            }
        )
    return output


def _normalize_stored_field_ref(raw: object, *, domain_id: str) -> str:
    if not isinstance(raw, str):
        return ""
    field_ref = raw.strip()
    if not field_ref:
        return ""
    parts = field_ref.split(".")
    if len(parts) == 3:
        ref_db = parts[0].strip()
        table = parts[1].strip()
        field = parts[2].strip()
        if not ref_db or not table or not field:
            return ""
        if ref_db != domain_id:
            return ""
        return f"{ref_db}.{table}.{field}"
    if len(parts) == 2:
        table = parts[0].strip()
        field = parts[1].strip()
        if not table or not field:
            return ""
        return f"{domain_id}.{table}.{field}"
    return ""


def _query_resource_cids(manifest: DomainArtifactManifest) -> tuple[str, ...]:
    cids: list[str] = []
    for artifact in manifest.artifacts:
        if artifact.artifact_type == ArtifactType.SAMPLES:
            continue
        cid = str(artifact.cid).strip()
        if not cid:
            continue
        cids.append(cid)
    return tuple(cids)


def _expand_domain_concepts(
    *,
    domain_id: str,
    requested_concepts: set[str],
    domain_unified: list[UnifiedRecord],
    descriptions: list[DescriptionRecord],
    resource_cids: tuple[str, ...],
    limit: int,
) -> list[LocalConceptHit]:
    if limit <= 0:
        return []
    if not requested_concepts:
        return []

    desc_map: dict[tuple[str, str], str] = {}
    for item in descriptions:
        table_obj = item.get("table")
        field_obj = item.get("field")
        desc_obj = item.get("description")
        if not isinstance(table_obj, str):
            continue
        if not isinstance(field_obj, str):
            continue
        if not isinstance(desc_obj, str):
            continue
        table_name = table_obj.strip()
        field_name = field_obj.strip()
        if not table_name or not field_name:
            continue
        desc_map[(table_name, field_name)] = desc_obj.strip()

    merged: dict[tuple[str, str, str], LocalConceptHit] = {}
    for item in domain_unified:
        canonical_name_obj = item.get("canonical_name")
        fields_obj = item.get("fields")
        concept_desc_obj = item.get("description", "")
        if not isinstance(canonical_name_obj, str):
            continue
        if not isinstance(fields_obj, list):
            continue
        canonical_name = canonical_name_obj.strip().lower()
        if not canonical_name:
            continue
        concept_desc = concept_desc_obj.strip() if isinstance(concept_desc_obj, str) else ""
        concept_score = _best_concept_match_score(canonical_name, requested_concepts)
        if concept_score <= 0:
            continue

        for raw_field in fields_obj:
            if not isinstance(raw_field, str):
                continue
            parsed = _parse_field_ref(raw_field)
            if parsed is None:
                continue
            ref_db, table_name, field_name = parsed
            if ref_db != domain_id:
                continue
            description = desc_map.get((table_name, field_name), concept_desc)
            score = concept_score + _description_keyword_bonus(description, requested_concepts)
            key = (canonical_name, table_name, field_name)
            current = merged.get(key)
            next_hit = LocalConceptHit(
                domain_id=domain_id,
                canonical_name=canonical_name,
                table=table_name,
                field=field_name,
                description=description,
                score=score,
                resource_cids=resource_cids,
            )
            if current is None or next_hit.score > current.score:
                merged[key] = next_hit

    ranked = sorted(merged.values(), key=lambda item: item.score, reverse=True)
    return ranked[:limit]


def _best_concept_match_score(canonical_name: str, requested_concepts: set[str]) -> int:
    best = 0
    for target in requested_concepts:
        if canonical_name == target:
            if best < 20:
                best = 20
            continue
        overlap = _token_overlap_score(canonical_name, target)
        if overlap > best:
            best = overlap
    return best


def _description_keyword_bonus(description: str, requested_concepts: set[str]) -> int:
    desc_tokens = _concept_tokens(description)
    if not desc_tokens:
        return 0
    bonus = 0
    for target in requested_concepts:
        target_tokens = _concept_tokens(target)
        if not target_tokens:
            continue
        overlap = len(desc_tokens & target_tokens)
        if overlap > bonus:
            bonus = overlap
    return bonus


def _token_overlap_score(left: str, right: str) -> int:
    left_tokens = _concept_tokens(left)
    right_tokens = _concept_tokens(right)
    if not left_tokens or not right_tokens:
        return 0
    return len(left_tokens & right_tokens)


def _concept_tokens(text: str) -> set[str]:
    parts: set[str] = set()
    for token in text.lower().replace("-", "_").split("_"):
        token_value = token.strip()
        if not token_value:
            continue
        parts.add(token_value)
    return parts


def _parse_field_ref(field_ref: str) -> tuple[str, str, str] | None:
    parts = field_ref.split(".")
    if len(parts) != 3:
        return None
    db_name = parts[0].strip()
    table_name = parts[1].strip()
    field_name = parts[2].strip()
    if not db_name or not table_name or not field_name:
        return None
    return db_name, table_name, field_name


def _coerce_domain_unified_records(
    *,
    payload: object,
    domain_id: str,
    descriptions: list[DescriptionRecord],
) -> list[UnifiedRecord]:
    if not isinstance(payload, list):
        return []

    short_to_full: dict[str, str] = {}
    full_refs: set[str] = set()
    field_to_fulls: dict[str, set[str]] = {}
    for item in descriptions:
        table_obj = item.get("table")
        field_obj = item.get("field")
        if not isinstance(table_obj, str):
            continue
        if not isinstance(field_obj, str):
            continue
        table_name = table_obj.strip()
        field_name = field_obj.strip()
        if not table_name or not field_name:
            continue

        short_ref = f"{table_name}.{field_name}"
        full_ref = f"{domain_id}.{table_name}.{field_name}"
        short_to_full.setdefault(short_ref, full_ref)
        full_refs.add(full_ref)
        field_to_fulls.setdefault(field_name, set()).add(full_ref)

    merged: dict[str, UnifiedRecord] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue

        canonical_name_obj = item.get("canonical_name")
        if not isinstance(canonical_name_obj, str):
            continue
        canonical_name = canonical_name_obj.strip()
        if not canonical_name:
            continue

        description_obj = item.get("description", "")
        description = description_obj.strip() if isinstance(description_obj, str) else ""

        fields_obj = item.get("fields")
        if not isinstance(fields_obj, list):
            continue

        normalized_fields: list[str] = []
        seen_fields: set[str] = set()
        for raw_field in fields_obj:
            normalized = _normalize_domain_field_ref(
                raw_field,
                domain_id=domain_id,
                short_to_full=short_to_full,
                full_refs=full_refs,
                field_to_fulls=field_to_fulls,
            )
            if not normalized:
                continue
            if normalized in seen_fields:
                continue
            seen_fields.add(normalized)
            normalized_fields.append(normalized)

        if not normalized_fields:
            continue

        existing = merged.get(canonical_name)
        if existing is None:
            merged[canonical_name] = {
                "db_name": domain_id,
                "canonical_name": canonical_name,
                "fields": normalized_fields,
                "description": description,
            }
            continue

        existing_fields_obj = existing.get("fields")
        if not isinstance(existing_fields_obj, list):
            existing_fields_obj = []
            existing["fields"] = existing_fields_obj

        existing_seen = {
            field_ref
            for field_ref in existing_fields_obj
            if isinstance(field_ref, str) and field_ref.strip()
        }
        for field_ref in normalized_fields:
            if field_ref in existing_seen:
                continue
            existing_seen.add(field_ref)
            existing_fields_obj.append(field_ref)

        existing_description_obj = existing.get("description", "")
        existing_description = (
            existing_description_obj.strip()
            if isinstance(existing_description_obj, str)
            else ""
        )
        if not existing_description and description:
            existing["description"] = description

    return list(merged.values())


def _normalize_domain_field_ref(
    raw_field: object,
    *,
    domain_id: str,
    short_to_full: dict[str, str],
    full_refs: set[str],
    field_to_fulls: dict[str, set[str]],
) -> str:
    if not isinstance(raw_field, str):
        return ""

    field_ref = raw_field.strip()
    if not field_ref:
        return ""

    if field_ref in full_refs:
        return field_ref
    if field_ref in short_to_full:
        return short_to_full[field_ref]

    parts = field_ref.split(".")
    if len(parts) == 3:
        db_name = parts[0].strip()
        table_name = parts[1].strip()
        field_name = parts[2].strip()
        if not db_name or not table_name or not field_name:
            return ""
        if db_name != domain_id:
            return ""
        candidate = f"{db_name}.{table_name}.{field_name}"
        return candidate if candidate in full_refs else ""

    if len(parts) == 2:
        table_name = parts[0].strip()
        field_name = parts[1].strip()
        if not table_name or not field_name:
            return ""
        return short_to_full.get(f"{table_name}.{field_name}", "")

    if len(parts) == 1:
        field_name = parts[0].strip()
        if not field_name:
            return ""
        candidates = sorted(field_to_fulls.get(field_name, set()))
        if len(candidates) == 1:
            return candidates[0]

    return ""


def _score_description(query_tokens: set[str], record: DescriptionRecord) -> int:
    text = (
        f"{record.get('table', '')} {record.get('field', '')} {record.get('description', '')}".lower()
    )
    if DESCRIPTION_FAILED in text:
        return 0
    return sum(1 for token in query_tokens if token in text)


def _search_descriptions(
    *,
    domain_id: str,
    query_tokens: set[str],
    descriptions: list[DescriptionRecord],
    limit: int,
) -> list[LocalQueryHit]:
    scored: list[LocalQueryHit] = []
    for item in descriptions:
        score = _score_description(query_tokens, item)
        if score <= 0:
            continue
        table = str(item["table"])
        field = str(item["field"])
        description = str(item["description"])
        scored.append(
            LocalQueryHit(
                domain_id=domain_id,
                table=table,
                field=field,
                description=description,
                score=score,
            )
        )
    scored.sort(key=lambda entry: entry.score, reverse=True)
    return scored[:limit]
