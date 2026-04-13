from __future__ import annotations

import hashlib
import json
import re
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Protocol

import requests

from src.configs.config import DOMAIN_SHARE_DEFAULTS, LLM_UNIFY_CONFIG
from src.distributed.contracts.models import (
    ArtifactRef,
    ArtifactType,
    BatchRequest,
    DomainArtifactManifest,
    DomainRegistration,
    GlobalBatchManifest,
    JobStatus,
    LocalConceptHit,
    LocalConceptQueryRequest,
    LocalJobRequest,
    LocalQueryHit,
    LocalQueryRequest,
    LocalSubQueryFilter,
    LocalSubQueryRequest,
    LocalSubQueryRow,
    NodeJobManifest,
    normalize_canonical_name,
    utc_now_iso,
    validate_token,
)
from src.kg.kg_agent import KnowledgeGraphAgent
from src.llm.semantic import FieldSemanticAgent
from src.query.cross_domain_models import (
    CrossDomainQueryPlan,
    ExecutedSubQuery,
    QueryConstraint,
)
from src.query.cross_domain_planner import (
    build_cross_domain_query_plan,
    load_domain_semantic_contexts,
    resolve_semantic_candidates,
)
from src.query.intent_parser import parse_query_intent
from src.query.join_path_resolver import propagated_constraints_for_plan
from src.query.result_assembler import assemble_cross_domain_response
from src.storage.ipfs_client import IPFSClient

_AlignmentRow = dict[str, object]
_UnifiedField = dict[str, object]
_DomainUnifiedItem = dict[str, object]
_DomainQueryBoostMap = dict[str, int]
_AnchorConcept = dict[str, object]
_AlignmentCandidate = dict[str, object]
_FederatedQueryResult = dict[str, object]

_CHAIN_CID_PATTERN = re.compile(r"(?im)^\s*cid\s*:\s*(\S+)\s*$")
_CHAIN_TXHASH_PATTERN = re.compile(r"(?im)^\s*txhash\s*:\s*(\S+)\s*$")
_CHAIN_RECEIVER_PATTERN = re.compile(r"^(?:0x)?[0-9a-fA-F]{40}$")


@dataclass(frozen=True)
class ChainWriteConfig:
    ipfs_chain_bin: Path
    receiver: str
    rpc_addr: str
    ipfs_api: str
    timeout_sec: int


@dataclass(frozen=True)
class _ChainWriteResult:
    cid: str
    tx_hash: str
    chain_key: str


class LocalAgentClient(Protocol):
    def create_job(self, request: LocalJobRequest) -> str:
        ...

    def get_job_status(self, job_id: str) -> JobStatus:
        ...

    def get_job_manifest(self, job_id: str) -> NodeJobManifest:
        ...

    def execute_local_query(self, request: LocalQueryRequest) -> list[LocalQueryHit]:
        ...

    def execute_local_concept_query(
        self,
        request: LocalConceptQueryRequest,
    ) -> list[LocalConceptHit]:
        ...

    def execute_local_subquery(
        self,
        request: LocalSubQueryRequest,
    ) -> list[LocalSubQueryRow]:
        ...


class LocalAgentHttpClient(LocalAgentClient):
    def __init__(self, *, endpoint: str, access_token: str = "", timeout_sec: float = 10.0) -> None:
        normalized_endpoint = endpoint.strip().rstrip("/")
        if not normalized_endpoint:
            raise RuntimeError("endpoint must be non-empty")
        self._endpoint = normalized_endpoint
        self._access_token = access_token.strip()
        self._timeout_sec = timeout_sec

    def create_job(self, request: LocalJobRequest) -> str:
        payload: dict[str, object] = {
            "run_id": request.run_id,
            "source_names": list(request.source_names),
            "max_fields_per_source": request.max_fields_per_source,
            "mock_llm": request.mock_llm,
            "share_mode": request.share_mode,
        }
        response = self._post_json("/v1/jobs", payload)
        job_id_obj = response.get("job_id")
        if not isinstance(job_id_obj, str):
            raise RuntimeError("local agent response missing job_id")
        return str(validate_token(job_id_obj, context="job_id"))

    def get_job_status(self, job_id: str) -> JobStatus:
        normalized_job_id = validate_token(job_id, context="job_id")
        response = self._get_json(f"/v1/jobs/{normalized_job_id}")
        status_text_obj = response.get("status")
        if not isinstance(status_text_obj, str):
            raise RuntimeError("local agent status response missing status")
        try:
            return JobStatus(status_text_obj)
        except ValueError as exc:
            raise RuntimeError(f"local agent returned unknown status: {status_text_obj}") from exc

    def get_job_manifest(self, job_id: str) -> NodeJobManifest:
        normalized_job_id = validate_token(job_id, context="job_id")
        response = self._get_json(f"/v1/jobs/{normalized_job_id}/manifest")
        return NodeJobManifest.from_dict(response)

    def execute_local_query(self, request: LocalQueryRequest) -> list[LocalQueryHit]:
        payload: dict[str, object] = {
            "query_text": request.query_text,
            "limit": request.limit,
        }
        if request.source_name is not None:
            payload["source_name"] = request.source_name
        response = self._post_json("/v1/query/local-exec", payload)

        items_obj = response.get("hits")
        if not isinstance(items_obj, list):
            raise RuntimeError("local query response must contain a list field: hits")

        hits: list[LocalQueryHit] = []
        for item in items_obj:
            if not isinstance(item, dict):
                continue
            domain_id = item.get("domain_id")
            table = item.get("table")
            field = item.get("field")
            description = item.get("description")
            score = item.get("score")
            if not isinstance(domain_id, str):
                continue
            if not isinstance(table, str):
                continue
            if not isinstance(field, str):
                continue
            if not isinstance(description, str):
                continue
            if not isinstance(score, int):
                continue
            hits.append(
                LocalQueryHit(
                    domain_id=domain_id,
                    table=table,
                    field=field,
                    description=description,
                    score=score,
                )
            )
        return hits

    def execute_local_concept_query(
        self,
        request: LocalConceptQueryRequest,
    ) -> list[LocalConceptHit]:
        payload: dict[str, object] = {
            "canonical_names": list(request.canonical_names),
            "limit": request.limit,
        }
        if request.source_name is not None:
            payload["source_name"] = request.source_name
        response = self._post_json("/v1/query/local-concept", payload)

        items_obj = response.get("hits")
        if not isinstance(items_obj, list):
            raise RuntimeError("local concept query response must contain a list field: hits")

        hits: list[LocalConceptHit] = []
        for item in items_obj:
            if not isinstance(item, dict):
                continue
            domain_id = item.get("domain_id")
            canonical_name = item.get("canonical_name")
            table = item.get("table")
            field = item.get("field")
            description = item.get("description")
            score = item.get("score")
            relation_type_obj = item.get("relation_type", "")
            alignment_score_obj = item.get("alignment_score", 0.0)
            resource_cids_obj = item.get("resource_cids", [])

            if not isinstance(domain_id, str):
                continue
            if not isinstance(canonical_name, str):
                continue
            if not isinstance(table, str):
                continue
            if not isinstance(field, str):
                continue
            if not isinstance(description, str):
                continue
            if not isinstance(score, int):
                continue
            if not isinstance(relation_type_obj, str):
                continue
            if not isinstance(alignment_score_obj, int | float):
                continue
            if not isinstance(resource_cids_obj, list):
                continue

            resource_cids: list[str] = []
            valid_resource_cids = True
            for raw_cid in resource_cids_obj:
                if not isinstance(raw_cid, str):
                    valid_resource_cids = False
                    break
                cid = raw_cid.strip()
                if cid:
                    resource_cids.append(cid)
            if not valid_resource_cids:
                continue

            hits.append(
                LocalConceptHit(
                    domain_id=domain_id,
                    canonical_name=canonical_name,
                    table=table,
                    field=field,
                    description=description,
                    score=score,
                    relation_type=relation_type_obj,
                    alignment_score=float(alignment_score_obj),
                    resource_cids=tuple(resource_cids),
                )
            )
        return hits

    def execute_local_subquery(
        self,
        request: LocalSubQueryRequest,
    ) -> list[LocalSubQueryRow]:
        payload: dict[str, object] = {
            "source_name": request.source_name,
            "table": request.table,
            "select_fields": list(request.select_fields),
            "filters": [item.to_dict() for item in request.filters],
            "limit": request.limit,
        }
        response = self._post_json("/v1/query/subquery-exec", payload)
        rows_obj = response.get("rows")
        if not isinstance(rows_obj, list):
            raise RuntimeError("local subquery response must contain a list field: rows")

        rows: list[LocalSubQueryRow] = []
        for item in rows_obj:
            if not isinstance(item, dict):
                continue
            domain_id = item.get("domain_id")
            table = item.get("table")
            data = item.get("data")
            if not isinstance(domain_id, str):
                continue
            if not isinstance(table, str):
                continue
            if not isinstance(data, dict):
                continue
            normalized_data: dict[str, object] = {}
            valid_data = True
            for key, value in data.items():
                if not isinstance(key, str):
                    valid_data = False
                    break
                normalized_data[key] = value
            if not valid_data:
                continue
            rows.append(
                LocalSubQueryRow(
                    domain_id=domain_id,
                    table=table,
                    data=normalized_data,
                )
            )
        return rows

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._access_token:
            headers["X-Agent-Token"] = self._access_token
        return headers

    def _post_json(self, path: str, payload: dict[str, object]) -> dict[str, object]:
        response = requests.post(
            f"{self._endpoint}{path}",
            headers=self._headers(),
            data=json.dumps(payload, ensure_ascii=False),
            timeout=self._timeout_sec,
        )
        response.raise_for_status()
        payload_obj = response.json()
        if not isinstance(payload_obj, dict):
            raise RuntimeError("HTTP response must be a JSON object")
        return payload_obj

    def _get_json(self, path: str) -> dict[str, object]:
        response = requests.get(
            f"{self._endpoint}{path}",
            headers=self._headers(),
            timeout=self._timeout_sec,
        )
        response.raise_for_status()
        payload_obj = response.json()
        if not isinstance(payload_obj, dict):
            raise RuntimeError("HTTP response must be a JSON object")
        return payload_obj


@dataclass
class _BatchState:
    request: BatchRequest
    status: JobStatus
    created_at: str
    started_at: str = ""
    finished_at: str = ""
    error_message: str = ""
    manifest: GlobalBatchManifest | None = None
    manifest_cid: str = ""
    manifest_chain_cid: str = ""
    manifest_chain_key: str = ""
    manifest_tx_hash: str = ""


class GlobalOrchestratorService:
    schema_version = "distributed-manifest/1.0"

    def __init__(
        self,
        *,
        ipfs_client: IPFSClient | None = None,
        executor_workers: int = 4,
        enable_chain_registration: bool = False,
        chain_write_config: ChainWriteConfig | None = None,
    ) -> None:
        self._ipfs = ipfs_client or IPFSClient()
        self._registry: dict[str, LocalAgentClient] = {}
        self._batches: dict[str, _BatchState] = {}
        self._latest_successful_batch_id: str = ""
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max(1, executor_workers))
        self._enable_chain_registration = enable_chain_registration
        self._chain_write_config = chain_write_config or _load_chain_write_config_from_defaults()
        if self._enable_chain_registration:
            _validate_chain_write_config(self._chain_write_config)

    def register_domain(self, registration: DomainRegistration) -> None:
        normalized = registration.normalized()
        client = LocalAgentHttpClient(
            endpoint=normalized.endpoint,
            access_token=normalized.access_token,
        )
        with self._lock:
            self._registry[normalized.domain_id] = client

    def register_domain_client(self, domain_id: str, client: LocalAgentClient) -> None:
        normalized_domain_id = validate_token(domain_id, context="domain_id")
        with self._lock:
            self._registry[normalized_domain_id] = client

    def create_batch(self, request: BatchRequest) -> str:
        normalized_request = request.normalized()
        with self._lock:
            missing = [domain_id for domain_id in normalized_request.domain_ids if domain_id not in self._registry]
        if missing:
            raise RuntimeError(f"unregistered domains: {', '.join(missing)}")

        batch_id = str(validate_token(uuid.uuid4().hex[:24], context="batch_id"))
        with self._lock:
            self._batches[batch_id] = _BatchState(
                request=normalized_request,
                status=JobStatus.PENDING,
                created_at=utc_now_iso(),
            )
        self._executor.submit(self._run_batch, batch_id)
        return batch_id

    def get_batch_status(self, batch_id: str) -> dict[str, object]:
        normalized_batch_id = validate_token(batch_id, context="batch_id")
        state = self._get_batch_state(normalized_batch_id)
        return {
            "batch_id": normalized_batch_id,
            "status": state.status.value,
            "created_at": state.created_at,
            "started_at": state.started_at,
            "finished_at": state.finished_at,
            "error_message": state.error_message,
            "manifest_cid": state.manifest_cid,
            "manifest_chain_cid": state.manifest_chain_cid,
            "manifest_chain_key": state.manifest_chain_key,
            "manifest_tx_hash": state.manifest_tx_hash,
        }

    def get_batch_manifest(self, batch_id: str) -> GlobalBatchManifest:
        normalized_batch_id = validate_token(batch_id, context="batch_id")
        state = self._get_batch_state(normalized_batch_id)
        if state.manifest is None:
            raise RuntimeError(f"batch manifest not ready: {normalized_batch_id}")
        return state.manifest

    def query(
        self,
        *,
        query_text: str,
        limit: int = 20,
        domain_ids: tuple[str, ...] = (),
    ) -> list[LocalQueryHit]:
        request = LocalQueryRequest(query_text=query_text, limit=limit).normalized()
        target_domains = domain_ids or tuple(self._registry.keys())

        normalized_domains: list[str] = []
        for domain_id in target_domains:
            normalized_domains.append(validate_token(domain_id, context="domain_id"))

        routing_boosts = self._build_query_routing_boosts(
            query_text=request.query_text,
            domain_ids=tuple(normalized_domains),
        )
        sorted_domains = sorted(
            normalized_domains,
            key=lambda domain_id: routing_boosts.get(domain_id, 0),
            reverse=True,
        )

        merged: dict[tuple[str, str, str], LocalQueryHit] = {}
        for domain_id in sorted_domains:
            client = self._registry.get(domain_id)
            if client is None:
                continue

            local_request = LocalQueryRequest(
                query_text=request.query_text,
                limit=request.limit,
                source_name=domain_id,
            )
            try:
                local_hits = client.execute_local_query(local_request)
            except Exception:  # noqa: BLE001
                continue

            boost = routing_boosts.get(domain_id, 0)
            for hit in local_hits:
                boosted_hit = LocalQueryHit(
                    domain_id=hit.domain_id,
                    table=hit.table,
                    field=hit.field,
                    description=hit.description,
                    score=max(0, hit.score + boost),
                )
                dedup_key = (boosted_hit.domain_id, boosted_hit.table, boosted_hit.field)
                previous = merged.get(dedup_key)
                if previous is None or boosted_hit.score > previous.score:
                    merged[dedup_key] = boosted_hit

        ranked_hits = sorted(merged.values(), key=lambda item: item.score, reverse=True)
        return ranked_hits[:limit]

    def query_federated(
        self,
        *,
        query_text: str,
        source_domain: str,
        target_domain_ids: tuple[str, ...] = (),
        limit: int = 20,
    ) -> _FederatedQueryResult:
        request = LocalQueryRequest(query_text=query_text, limit=limit).normalized()
        normalized_source_domain = validate_token(source_domain, context="source_domain")

        with self._lock:
            if normalized_source_domain not in self._registry:
                raise RuntimeError(f"source domain is not registered: {normalized_source_domain}")
            if target_domain_ids:
                raw_targets = list(target_domain_ids)
            else:
                raw_targets = [
                    domain_id
                    for domain_id in self._registry
                    if domain_id != normalized_source_domain
                ]

        normalized_targets: list[str] = []
        seen_targets: set[str] = set()
        for raw_target in raw_targets:
            normalized_target = validate_token(raw_target, context="target_domain_ids[]")
            if normalized_target == normalized_source_domain:
                continue
            if normalized_target in seen_targets:
                continue
            with self._lock:
                if normalized_target not in self._registry:
                    raise RuntimeError(f"target domain is not registered: {normalized_target}")
            seen_targets.add(normalized_target)
            normalized_targets.append(normalized_target)

        if not normalized_targets:
            raise RuntimeError("target domains must not be empty")

        anchors = self._build_source_anchor_concepts(
            query_text=request.query_text,
            source_domain=normalized_source_domain,
        )
        if not anchors:
            fallback_anchor = _fallback_anchor_concept(request.query_text, normalized_source_domain)
            if fallback_anchor is not None:
                anchors = [fallback_anchor]

        candidates = self._build_alignment_candidates(
            source_domain=normalized_source_domain,
            target_domains=tuple(normalized_targets),
            anchors=anchors,
        )
        if not candidates:
            return self._build_federated_query_response(
                query_text=request.query_text,
                source_domain=normalized_source_domain,
                target_domains=tuple(normalized_targets),
                anchors=anchors,
                candidates=[],
                hits=[],
            )

        domain_to_concepts: dict[str, set[str]] = {}
        candidate_index: dict[tuple[str, str], _AlignmentCandidate] = {}
        for candidate in candidates:
            target_domain_obj = candidate.get("target_domain")
            canonical_name_obj = candidate.get("canonical_name")
            if not isinstance(target_domain_obj, str):
                continue
            if not isinstance(canonical_name_obj, str):
                continue
            target_domain = target_domain_obj.strip()
            canonical_name = canonical_name_obj.strip()
            if not target_domain or not canonical_name:
                continue
            domain_to_concepts.setdefault(target_domain, set()).add(canonical_name)
            key = (target_domain, canonical_name)
            previous = candidate_index.get(key)
            if previous is None:
                candidate_index[key] = candidate
                continue
            previous_score_obj = previous.get("composite_score", 0.0)
            current_score_obj = candidate.get("composite_score", 0.0)
            previous_score = float(previous_score_obj) if isinstance(previous_score_obj, int | float) else 0.0
            current_score = float(current_score_obj) if isinstance(current_score_obj, int | float) else 0.0
            if current_score > previous_score:
                candidate_index[key] = candidate

        merged_hits: dict[tuple[str, str, str, str], LocalConceptHit] = {}
        for target_domain in normalized_targets:
            canonical_set = domain_to_concepts.get(target_domain, set())
            if not canonical_set:
                continue
            client = self._registry.get(target_domain)
            if client is None:
                continue
            local_request = LocalConceptQueryRequest(
                canonical_names=tuple(sorted(canonical_set)),
                limit=request.limit,
                source_name=target_domain,
            )
            try:
                local_hits = client.execute_local_concept_query(local_request)
            except Exception:  # noqa: BLE001
                continue

            for hit in local_hits:
                canonical_name = normalize_canonical_name(
                    hit.canonical_name,
                    context="local_concept_hit.canonical_name",
                )
                matched_candidate_obj = candidate_index.get((target_domain, canonical_name))
                if matched_candidate_obj is None:
                    continue
                matched_candidate = matched_candidate_obj

                relation_type_obj = matched_candidate.get("relation_type", "")
                relation_type = relation_type_obj if isinstance(relation_type_obj, str) else ""
                alignment_score_obj = matched_candidate.get("alignment_score", 0.0)
                alignment_score = (
                    float(alignment_score_obj) if isinstance(alignment_score_obj, int | float) else 0.0
                )
                boost = max(0, int(round(alignment_score * 10)))
                composite_score_obj = matched_candidate.get("composite_score", 0.0)
                composite_score = (
                    float(composite_score_obj) if isinstance(composite_score_obj, int | float) else 0.0
                )
                score = max(hit.score + boost, int(round(composite_score)))

                enriched_hit = LocalConceptHit(
                    domain_id=hit.domain_id,
                    canonical_name=canonical_name,
                    table=hit.table,
                    field=hit.field,
                    description=hit.description,
                    score=score,
                    relation_type=relation_type,
                    alignment_score=alignment_score,
                    resource_cids=hit.resource_cids,
                )
                dedup_key = (
                    enriched_hit.domain_id,
                    enriched_hit.canonical_name,
                    enriched_hit.table,
                    enriched_hit.field,
                )
                previous_hit = merged_hits.get(dedup_key)
                if previous_hit is None or enriched_hit.score > previous_hit.score:
                    merged_hits[dedup_key] = enriched_hit

        ranked_hits = sorted(merged_hits.values(), key=lambda item: item.score, reverse=True)
        return self._build_federated_query_response(
            query_text=request.query_text,
            source_domain=normalized_source_domain,
            target_domains=tuple(normalized_targets),
            anchors=anchors,
            candidates=candidates,
            hits=ranked_hits[: request.limit],
        )

    def query_federated_execute(
        self,
        *,
        query_text: str,
        limit: int = 20,
        source_domain: str = "",
        domain_ids: tuple[str, ...] = (),
    ) -> dict[str, object]:
        request = LocalQueryRequest(query_text=query_text, limit=limit).normalized()
        with self._lock:
            available_domains = tuple(self._registry.keys())
        if not available_domains:
            raise RuntimeError("no registered domains available")

        if domain_ids:
            normalized_domains = tuple(
                validate_token(item, context="domain_ids[]")
                for item in domain_ids
            )
        else:
            normalized_domains = available_domains
        if not normalized_domains:
            raise RuntimeError("domain_ids must not be empty")

        normalized_source = ""
        if source_domain.strip():
            normalized_source = validate_token(source_domain, context="source_domain")
            if normalized_source not in normalized_domains:
                normalized_domains = (normalized_source, *normalized_domains)
            if normalized_source not in available_domains:
                raise RuntimeError(f"source domain is not registered: {normalized_source}")

        for domain in normalized_domains:
            if domain not in available_domains:
                raise RuntimeError(f"domain is not registered: {domain}")

        latest_manifest = self._latest_successful_manifest()
        if latest_manifest is None:
            raise RuntimeError("no successful global batch manifest available for federated execution")

        intent = parse_query_intent(
            request.query_text,
            available_domains=normalized_domains,
        )
        contexts = load_domain_semantic_contexts(
            manifest=latest_manifest,
            ipfs_client=self._ipfs,
            domain_ids=normalized_domains,
        )
        candidate_map = resolve_semantic_candidates(intent=intent, contexts=contexts)
        plan = build_cross_domain_query_plan(
            query_text=request.query_text,
            intent=intent,
            candidate_map=candidate_map,
            target_domains=normalized_domains,
            source_domain=normalized_source,
            limit=request.limit,
        )
        executed_subqueries = self._execute_cross_domain_plan(plan)
        response = assemble_cross_domain_response(
            query_text=request.query_text,
            intent=intent,
            plan=plan,
            executed_subqueries=executed_subqueries,
            limit=request.limit,
        )
        response["domains_requested"] = list(normalized_domains)
        response["source_domain"] = normalized_source
        response["chain_context"] = {
            "manifest_chain_cid": latest_manifest.manifest_chain_cid,
            "manifest_chain_key": latest_manifest.manifest_chain_key,
            "manifest_tx_hash": latest_manifest.manifest_tx_hash,
        }
        return response

    def _execute_cross_domain_plan(
        self,
        plan: CrossDomainQueryPlan,
    ) -> list[ExecutedSubQuery]:
        propagated_values: dict[str, set[str]] = {}
        plan_lookup = {item.plan_id: item for item in plan.subqueries}
        executed: list[ExecutedSubQuery] = []

        for plan_id in plan.execution_order:
            subquery = plan_lookup.get(plan_id)
            if subquery is None:
                continue
            client = self._registry.get(subquery.domain)
            if client is None:
                executed.append(
                    ExecutedSubQuery(
                        plan=subquery,
                        rows=[],
                        error=f"domain client is not available: {subquery.domain}",
                    )
                )
                continue

            runtime_constraints = self._build_runtime_constraints(
                base_constraints=subquery.constraints,
                dependency_concepts=subquery.dependency_concepts,
                propagated_values=propagated_values,
            )
            concept_field_map = dict(subquery.concept_field_map)
            filters: list[LocalSubQueryFilter] = []
            for constraint in runtime_constraints:
                mapped_field = concept_field_map.get(constraint.concept, "")
                if not mapped_field:
                    continue
                filters.append(
                    LocalSubQueryFilter(
                        field=mapped_field,
                        operator=constraint.operator,
                        value=constraint.value,
                    )
                )
            if not filters and subquery.constraints:
                executed.append(
                    ExecutedSubQuery(
                        plan=subquery,
                        rows=[],
                        error="no mapped fields available for query constraints",
                    )
                )
                continue

            local_request = LocalSubQueryRequest(
                source_name=subquery.domain,
                table=subquery.table,
                select_fields=subquery.select_fields,
                filters=tuple(filters),
                limit=subquery.limit,
            )
            try:
                rows = client.execute_local_subquery(local_request)
            except Exception as exc:  # noqa: BLE001
                executed.append(
                    ExecutedSubQuery(
                        plan=subquery,
                        rows=[],
                        error=str(exc),
                    )
                )
                continue

            normalized_rows: list[dict[str, object]] = []
            for item in rows:
                row = item.data
                if not isinstance(row, dict):
                    continue
                normalized_rows.append(row)
                for concept, field in subquery.concept_field_map:
                    value = row.get(field)
                    if value is None:
                        continue
                    text = str(value).strip()
                    if not text:
                        continue
                    values = propagated_values.setdefault(concept, set())
                    if len(values) < 200:
                        values.add(text)

            executed.append(
                ExecutedSubQuery(
                    plan=subquery,
                    rows=normalized_rows,
                    error="",
                )
            )

        return executed

    def _build_runtime_constraints(
        self,
        *,
        base_constraints: tuple[QueryConstraint, ...],
        dependency_concepts: tuple[str, ...],
        propagated_values: dict[str, set[str]],
    ) -> tuple[QueryConstraint, ...]:
        return propagated_constraints_for_plan(
            base_constraints=base_constraints,
            dependency_concepts=dependency_concepts,
            propagated_values=propagated_values,
        )

    def _build_source_anchor_concepts(
        self,
        *,
        query_text: str,
        source_domain: str,
    ) -> list[_AnchorConcept]:
        latest_manifest = self._latest_successful_manifest()
        if latest_manifest is None:
            return []
        source_manifest = _find_domain_manifest(latest_manifest, source_domain)
        if source_manifest is None:
            return []

        unified_cid = _artifact_cid(source_manifest, ArtifactType.DOMAIN_UNIFIED)
        if not unified_cid:
            return []
        payload = self._ipfs.cat_json(unified_cid)
        if not isinstance(payload, list):
            return []

        query_tokens = _query_tokens(query_text)
        if not query_tokens:
            return []

        merged: dict[str, _AnchorConcept] = {}
        for raw_item in payload:
            if not isinstance(raw_item, dict):
                continue
            item = _coerce_domain_unified_item(raw_item, default_domain_id=source_domain)
            if item is None:
                continue

            canonical_name_obj = item.get("canonical_name")
            fields_obj = item.get("fields")
            description_obj = item.get("description", "")
            if not isinstance(canonical_name_obj, str):
                continue
            if not isinstance(fields_obj, list):
                continue
            canonical_name = normalize_canonical_name(
                canonical_name_obj,
                context="domain_unified.canonical_name",
            )
            description = description_obj.strip() if isinstance(description_obj, str) else ""
            score = _score_anchor_concept(
                query_tokens=query_tokens,
                canonical_name=canonical_name,
                description=description,
                fields=fields_obj,
            )
            if score <= 0:
                continue

            top_fields = _top_field_evidence(fields_obj, limit=3)
            anchor: _AnchorConcept = {
                "source_domain": source_domain,
                "canonical_name": canonical_name,
                "anchor_score": score,
                "evidence_fields": top_fields,
            }
            previous = merged.get(canonical_name)
            if previous is None:
                merged[canonical_name] = anchor
                continue
            previous_score_obj = previous.get("anchor_score", 0)
            previous_score = int(previous_score_obj) if isinstance(previous_score_obj, int) else 0
            if score > previous_score:
                merged[canonical_name] = anchor

        ranked = sorted(
            merged.values(),
            key=_anchor_score_value,
            reverse=True,
        )
        return ranked[:12]

    def _build_alignment_candidates(
        self,
        *,
        source_domain: str,
        target_domains: tuple[str, ...],
        anchors: list[_AnchorConcept],
    ) -> list[_AlignmentCandidate]:
        latest_manifest = self._latest_successful_manifest()
        if latest_manifest is None:
            return []
        if not anchors:
            return []

        alignment_cid = _artifact_cid_from_batch_manifest(latest_manifest, ArtifactType.ALIGNMENT_INDEX)
        if not alignment_cid:
            return []

        payload = self._ipfs.cat_json(alignment_cid)
        alignment_rows = _coerce_alignment_index(payload)
        if not alignment_rows:
            return []

        target_set = set(target_domains)
        anchor_score_map: dict[str, int] = {}
        for anchor in anchors:
            canonical_obj = anchor.get("canonical_name")
            score_obj = anchor.get("anchor_score", 0)
            if not isinstance(canonical_obj, str):
                continue
            if not isinstance(score_obj, int):
                continue
            canonical_name = normalize_canonical_name(canonical_obj, context="anchors[].canonical_name")
            current = anchor_score_map.get(canonical_name, 0)
            if score_obj > current:
                anchor_score_map[canonical_name] = score_obj
        if not anchor_score_map:
            return []

        merged: dict[tuple[str, str, str], _AlignmentCandidate] = {}
        for row in alignment_rows:
            source_domain_obj = row.get("source_domain")
            target_domain_obj = row.get("target_domain")
            canonical_name_obj = row.get("canonical_name")
            relation_type_obj = row.get("relation_type", "SAME_AS")
            alignment_score_obj = row.get("score", 1.0)
            if not isinstance(source_domain_obj, str):
                continue
            if not isinstance(target_domain_obj, str):
                continue
            if not isinstance(canonical_name_obj, str):
                continue
            if not isinstance(relation_type_obj, str):
                continue
            if not isinstance(alignment_score_obj, int | float):
                continue

            canonical_name = normalize_canonical_name(
                canonical_name_obj,
                context="alignment_index[].canonical_name",
            )
            anchor_score = anchor_score_map.get(canonical_name, 0)
            if anchor_score <= 0:
                continue

            selected_target = ""
            if source_domain_obj == source_domain and target_domain_obj in target_set:
                selected_target = target_domain_obj
            elif target_domain_obj == source_domain and source_domain_obj in target_set:
                selected_target = source_domain_obj
            if not selected_target:
                continue

            relation_type = relation_type_obj.strip() or "SAME_AS"
            alignment_score = float(alignment_score_obj)
            composite_score = alignment_score * 10.0 + float(anchor_score)

            key = (selected_target, canonical_name, relation_type)
            candidate: _AlignmentCandidate = {
                "source_domain": source_domain,
                "target_domain": selected_target,
                "canonical_name": canonical_name,
                "relation_type": relation_type,
                "alignment_score": alignment_score,
                "anchor_score": anchor_score,
                "composite_score": composite_score,
                "evidence": _alignment_row_evidence(row),
            }
            previous = merged.get(key)
            if previous is None:
                merged[key] = candidate
                continue
            previous_score_obj = previous.get("composite_score", 0.0)
            previous_score = float(previous_score_obj) if isinstance(previous_score_obj, int | float) else 0.0
            if composite_score > previous_score:
                merged[key] = candidate

        ranked = sorted(
            merged.values(),
            key=_composite_score_value,
            reverse=True,
        )
        return ranked

    def _build_federated_query_response(
        self,
        *,
        query_text: str,
        source_domain: str,
        target_domains: tuple[str, ...],
        anchors: list[_AnchorConcept],
        candidates: list[_AlignmentCandidate],
        hits: list[LocalConceptHit],
    ) -> _FederatedQueryResult:
        latest_manifest = self._latest_successful_manifest()
        chain_context: dict[str, str] = {
            "manifest_chain_cid": "",
            "manifest_chain_key": "",
            "manifest_tx_hash": "",
        }
        if latest_manifest is not None:
            chain_context = {
                "manifest_chain_cid": str(latest_manifest.manifest_chain_cid),
                "manifest_chain_key": str(latest_manifest.manifest_chain_key),
                "manifest_tx_hash": str(latest_manifest.manifest_tx_hash),
            }

        return {
            "query_text": query_text,
            "source_domain": source_domain,
            "target_domains": list(target_domains),
            "anchors": anchors,
            "alignment_candidates": candidates,
            "hits": [item.to_dict() for item in hits],
            "chain_context": chain_context,
        }

    def _run_batch(self, batch_id: str) -> None:
        with self._lock:
            state = self._batches[batch_id]
            state.status = JobStatus.RUNNING
            state.started_at = utc_now_iso()
            request = state.request
            clients = {domain_id: self._registry[domain_id] for domain_id in request.domain_ids}

        try:
            manifest, manifest_cid, manifest_chain_key, manifest_tx_hash = self._execute_batch(
                clients=clients,
                batch_id=batch_id,
                request=request,
            )
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
            state.manifest_cid = manifest_cid
            state.manifest_chain_cid = manifest.manifest_chain_cid
            state.manifest_chain_key = manifest_chain_key
            state.manifest_tx_hash = manifest_tx_hash
            self._latest_successful_batch_id = batch_id

    def _execute_batch(
        self,
        *,
        clients: dict[str, LocalAgentClient],
        batch_id: str,
        request: BatchRequest,
    ) -> tuple[GlobalBatchManifest, str, str, str]:
        job_ids: dict[str, str] = {}
        for domain_id, client in clients.items():
            job_ids[domain_id] = client.create_job(
                LocalJobRequest(
                    run_id=request.run_id,
                    max_fields_per_source=request.max_fields_per_source,
                    mock_llm=request.mock_llm,
                    share_mode=request.share_mode,
                )
            )

        start_time = time.monotonic()
        done_domains: set[str] = set()
        failed_domains: dict[str, str] = {}

        while len(done_domains) < len(clients):
            elapsed = time.monotonic() - start_time
            if elapsed >= request.poll_timeout_sec:
                timeout_domains = sorted(set(clients) - done_domains)
                raise RuntimeError(f"batch poll timeout; unfinished domains: {timeout_domains}")

            for domain_id, client in clients.items():
                if domain_id in done_domains:
                    continue
                status = client.get_job_status(job_ids[domain_id])
                if status == JobStatus.SUCCEEDED:
                    done_domains.add(domain_id)
                elif status == JobStatus.FAILED:
                    done_domains.add(domain_id)
                    failed_domains[domain_id] = "local job failed"
            if len(done_domains) < len(clients):
                time.sleep(request.poll_interval_sec)

        if failed_domains:
            failed_text = ", ".join(f"{domain}:{reason}" for domain, reason in sorted(failed_domains.items()))
            raise RuntimeError(f"local job failures detected: {failed_text}")

        node_manifests = tuple(
            clients[domain_id].get_job_manifest(job_ids[domain_id])
            for domain_id in request.domain_ids
        )

        unified_fields, alignment_index = self._build_alignment_index(
            node_manifests=node_manifests,
            mock_llm=request.mock_llm,
        )
        unified_fields_artifact = self._publish_artifact(
            payload=unified_fields,
            artifact_type=ArtifactType.UNIFIED_FIELDS,
        )
        alignment_artifact = self._publish_artifact(
            payload=alignment_index,
            artifact_type=ArtifactType.ALIGNMENT_INDEX,
        )
        alignment_cypher = self._build_alignment_cypher(
            node_manifests=node_manifests,
            unified_fields=unified_fields,
            alignment_index=alignment_index,
        )
        alignment_cypher_artifact = self._publish_artifact(
            payload=alignment_cypher,
            artifact_type=ArtifactType.ALIGNMENT_CYPHER,
        )

        draft_manifest = GlobalBatchManifest(
            schema_version=self.schema_version,
            run_id=request.run_id,
            batch_id=batch_id,
            created_at=utc_now_iso(),
            status=JobStatus.SUCCEEDED,
            node_manifests=node_manifests,
            artifacts=(unified_fields_artifact, alignment_artifact, alignment_cypher_artifact),
        )

        draft_manifest_payload = draft_manifest.to_dict()
        if self._enable_chain_registration:
            chain_result = self._register_manifest_on_chain(
                batch_id=batch_id,
                run_id=request.run_id,
                manifest_payload=draft_manifest_payload,
            )
            manifest = GlobalBatchManifest(
                schema_version=draft_manifest.schema_version,
                run_id=draft_manifest.run_id,
                batch_id=draft_manifest.batch_id,
                created_at=draft_manifest.created_at,
                status=draft_manifest.status,
                node_manifests=draft_manifest.node_manifests,
                artifacts=draft_manifest.artifacts,
                manifest_chain_cid=chain_result.cid,
                manifest_chain_key=chain_result.chain_key,
                manifest_tx_hash=chain_result.tx_hash,
            )
            return manifest, chain_result.cid, chain_result.chain_key, chain_result.tx_hash

        manifest = draft_manifest
        manifest_cid = self._ipfs.add_json(manifest.to_dict())
        return manifest, manifest_cid, "", ""

    def _build_alignment_index(
        self,
        *,
        node_manifests: tuple[NodeJobManifest, ...],
        mock_llm: bool,
    ) -> tuple[list[_UnifiedField], list[_AlignmentRow]]:
        domain_items = self._collect_domain_unified_items(node_manifests)
        unified_fields = self._unify_across_domains(domain_items=domain_items, mock_llm=mock_llm)
        kg_agent = KnowledgeGraphAgent()
        raw_index = kg_agent.generate_alignment_index(unified_fields)
        return unified_fields, _coerce_alignment_index(raw_index)

    def _build_alignment_cypher(
        self,
        *,
        node_manifests: tuple[NodeJobManifest, ...],
        unified_fields: list[_UnifiedField],
        alignment_index: list[_AlignmentRow],
    ) -> list[str]:
        db_data = _build_db_data_from_unified_fields(unified_fields)
        run_record: dict[str, object] = {
            "timestamp": utc_now_iso(),
            "domains": [domain_manifest.to_dict() for domain_manifest in _flatten_domain_manifests(node_manifests)],
        }
        kg_agent = KnowledgeGraphAgent()
        raw_cypher = kg_agent.generate_alignment_cypher(
            run_record=run_record,
            db_data=db_data,
            unified_fields=unified_fields,
            alignment_index=alignment_index,
        )
        return _coerce_alignment_cypher(raw_cypher)

    def _collect_domain_unified_items(
        self,
        node_manifests: tuple[NodeJobManifest, ...],
    ) -> list[_DomainUnifiedItem]:
        items: list[_DomainUnifiedItem] = []
        for node_manifest in node_manifests:
            for domain_manifest in node_manifest.domain_manifests:
                cid = _artifact_cid(domain_manifest, ArtifactType.DOMAIN_UNIFIED)
                if not cid:
                    continue
                payload = self._ipfs.cat_json(cid)
                if not isinstance(payload, list):
                    raise RuntimeError(
                        f"domain_unified payload must be a list for domain '{domain_manifest.domain_id}'"
                    )

                for raw_item in payload:
                    if not isinstance(raw_item, dict):
                        continue
                    normalized = _coerce_domain_unified_item(
                        raw_item,
                        default_domain_id=domain_manifest.domain_id,
                    )
                    if normalized is not None:
                        items.append(normalized)
        return items

    def _unify_across_domains(
        self,
        *,
        domain_items: list[_DomainUnifiedItem],
        mock_llm: bool,
    ) -> list[_UnifiedField]:
        if not domain_items:
            return []

        if mock_llm:
            return _mock_unify_across_domains(domain_items)

        fs_agent = FieldSemanticAgent(
            api_key=LLM_UNIFY_CONFIG["api_key"],
            base_url=LLM_UNIFY_CONFIG["base_url"],
            model_name=LLM_UNIFY_CONFIG["model_name"],
        )
        raw_output = fs_agent.unify_across_domains(domain_items)
        return _coerce_unified_fields(raw_output)

    def _build_query_routing_boosts(
        self,
        *,
        query_text: str,
        domain_ids: tuple[str, ...],
    ) -> _DomainQueryBoostMap:
        boosts: _DomainQueryBoostMap = dict.fromkeys(domain_ids, 0)
        latest_manifest = self._latest_successful_manifest()
        if latest_manifest is None:
            return boosts

        alignment_cid = _artifact_cid_from_batch_manifest(latest_manifest, ArtifactType.ALIGNMENT_INDEX)
        if not alignment_cid:
            return boosts

        try:
            payload = self._ipfs.cat_json(alignment_cid)
            alignment_rows = _coerce_alignment_index(payload)
        except Exception:  # noqa: BLE001
            return boosts

        query_tokens = _query_tokens(query_text)
        if not query_tokens:
            return boosts

        for row in alignment_rows:
            source_domain_obj = row.get("source_domain")
            target_domain_obj = row.get("target_domain")
            if not isinstance(source_domain_obj, str):
                continue
            if not isinstance(target_domain_obj, str):
                continue
            score = _score_alignment_row(row=row, query_tokens=query_tokens)
            if score <= 0:
                continue
            if source_domain_obj in boosts:
                boosts[source_domain_obj] += score
            if target_domain_obj in boosts:
                boosts[target_domain_obj] += score

        return boosts

    def _latest_successful_manifest(self) -> GlobalBatchManifest | None:
        with self._lock:
            latest_id = self._latest_successful_batch_id
            if not latest_id:
                return None
            state = self._batches.get(latest_id)
        if state is None:
            return None
        return state.manifest

    def _register_manifest_on_chain(
        self,
        *,
        batch_id: str,
        run_id: str,
        manifest_payload: dict[str, object],
    ) -> _ChainWriteResult:
        chain_key = _build_manifest_chain_key(run_id=run_id, batch_id=batch_id)
        with NamedTemporaryFile(
            mode="w",
            suffix="_global_manifest.json",
            encoding="utf-8",
            delete=False,
        ) as tmp_file:
            json.dump(manifest_payload, tmp_file, ensure_ascii=False, indent=2)
            tmp_path = Path(tmp_file.name)

        try:
            cid, tx_hash = _put_file_on_chain(
                config=self._chain_write_config,
                chain_key=chain_key,
                file_path=tmp_path,
            )
        finally:
            with suppress(OSError):
                tmp_path.unlink(missing_ok=True)

        return _ChainWriteResult(cid=cid, tx_hash=tx_hash, chain_key=chain_key)

    def _publish_artifact(
        self,
        *,
        payload: object,
        artifact_type: ArtifactType,
    ) -> ArtifactRef:
        cid = self._ipfs.add_json(payload)
        sha = _sha256_payload(payload)
        count = _record_count(payload)
        return ArtifactRef(
            artifact_type=artifact_type,
            cid=cid,
            sha256=sha,
            record_count=count,
            security_level="semantic_only",
        )

    def _get_batch_state(self, batch_id: str) -> _BatchState:
        with self._lock:
            state = self._batches.get(batch_id)
        if state is None:
            raise RuntimeError(f"batch not found: {batch_id}")
        return state


def _record_count(payload: object) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        return len(payload)
    return 1


def _sha256_payload(payload: object) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _artifact_cid(manifest: DomainArtifactManifest, artifact_type: ArtifactType) -> str:
    for artifact in manifest.artifacts:
        if artifact.artifact_type == artifact_type:
            return str(artifact.cid)
    return ""


def _coerce_domain_unified_item(
    payload: dict[str, object],
    *,
    default_domain_id: str,
) -> _DomainUnifiedItem | None:
    canonical_name_obj = payload.get("canonical_name")
    fields_obj = payload.get("fields")
    description_obj = payload.get("description", "")
    db_name_obj = payload.get("db_name", default_domain_id)

    if not isinstance(canonical_name_obj, str):
        return None
    canonical_name = canonical_name_obj.strip()
    if not canonical_name:
        return None

    if not isinstance(db_name_obj, str):
        return None
    db_name = db_name_obj.strip() or default_domain_id

    if not isinstance(description_obj, str):
        return None
    description = description_obj.strip()

    if not isinstance(fields_obj, list):
        return None
    fields: list[str] = []
    for raw_field in fields_obj:
        if not isinstance(raw_field, str):
            continue
        field_ref = raw_field.strip()
        if field_ref:
            fields.append(field_ref)
    if not fields:
        return None

    return {
        "db_name": db_name,
        "canonical_name": canonical_name,
        "fields": fields,
        "description": description,
    }


def _mock_unify_across_domains(domain_items: list[_DomainUnifiedItem]) -> list[_UnifiedField]:
    grouped_fields: dict[str, list[str]] = {}
    grouped_descriptions: dict[str, str] = {}

    for item in domain_items:
        canonical_name_obj = item.get("canonical_name")
        fields_obj = item.get("fields")
        description_obj = item.get("description", "")
        if not isinstance(canonical_name_obj, str):
            continue
        if not isinstance(fields_obj, list):
            continue
        canonical_name = canonical_name_obj.strip()
        if not canonical_name:
            continue

        members = grouped_fields.setdefault(canonical_name, [])
        seen = set(members)
        for raw_field in fields_obj:
            if not isinstance(raw_field, str):
                continue
            field_ref = raw_field.strip()
            if not field_ref:
                continue
            if field_ref in seen:
                continue
            seen.add(field_ref)
            members.append(field_ref)

        if canonical_name not in grouped_descriptions and isinstance(description_obj, str):
            grouped_descriptions[canonical_name] = description_obj.strip()

    output: list[_UnifiedField] = []
    for canonical_name, fields in grouped_fields.items():
        output.append(
            {
                "canonical_name": canonical_name,
                "fields": fields,
                "description": grouped_descriptions.get(canonical_name, ""),
            }
        )
    return output


def _coerce_unified_fields(payload: object) -> list[_UnifiedField]:
    if not isinstance(payload, list):
        raise RuntimeError("unified_fields payload from semantic agent must be a list")

    output: list[_UnifiedField] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise RuntimeError(f"unified_fields item at index {index} must be an object")

        canonical_name_obj = item.get("canonical_name")
        fields_obj = item.get("fields")
        description_obj = item.get("description", "")

        if not isinstance(canonical_name_obj, str) or not canonical_name_obj.strip():
            raise RuntimeError(f"unified_fields item at index {index} has invalid canonical_name")
        if not isinstance(fields_obj, list):
            raise RuntimeError(f"unified_fields item at index {index} has invalid fields")
        if not isinstance(description_obj, str):
            raise RuntimeError(f"unified_fields item at index {index} has invalid description")

        fields: list[str] = []
        seen: set[str] = set()
        for raw_field in fields_obj:
            if not isinstance(raw_field, str):
                raise RuntimeError(f"unified_fields item at index {index} contains non-string field")
            field_ref = raw_field.strip()
            if not field_ref:
                continue
            if field_ref in seen:
                continue
            seen.add(field_ref)
            fields.append(field_ref)

        output.append(
            {
                "canonical_name": canonical_name_obj.strip(),
                "fields": fields,
                "description": description_obj.strip(),
            }
        )
    return output


def _coerce_alignment_index(payload: object) -> list[_AlignmentRow]:
    if not isinstance(payload, list):
        raise RuntimeError("alignment index must be a list")

    output: list[_AlignmentRow] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise RuntimeError(f"alignment item at index {index} must be an object")

        required = [
            "source_domain",
            "source_table",
            "source_field",
            "target_domain",
            "target_table",
            "target_field",
            "canonical_name",
        ]
        normalized: dict[str, object] = {}
        for key in required:
            value = item.get(key)
            if not isinstance(value, str) or not value.strip():
                raise RuntimeError(f"alignment item at index {index} has invalid {key}")
            normalized[key] = value.strip()

        relation_type_obj = item.get("relation_type", "SAME_AS")
        if isinstance(relation_type_obj, str) and relation_type_obj.strip():
            normalized["relation_type"] = relation_type_obj.strip()
        else:
            normalized["relation_type"] = "SAME_AS"

        score_obj = item.get("score", 1.0)
        if isinstance(score_obj, int | float):
            normalized["score"] = float(score_obj)
        else:
            normalized["score"] = 1.0

        output.append(normalized)

    return output


def _coerce_alignment_cypher(payload: object) -> list[str]:
    if not isinstance(payload, list):
        raise RuntimeError("alignment cypher must be a list")
    output: list[str] = []
    for index, item in enumerate(payload):
        if not isinstance(item, str):
            raise RuntimeError(f"alignment cypher item at index {index} must be a string")
        statement = item.strip()
        if statement:
            output.append(statement)
    return output


def _flatten_domain_manifests(node_manifests: tuple[NodeJobManifest, ...]) -> list[DomainArtifactManifest]:
    flattened: list[DomainArtifactManifest] = []
    for node_manifest in node_manifests:
        for domain_manifest in node_manifest.domain_manifests:
            flattened.append(domain_manifest)
    return flattened


def _find_domain_manifest(
    manifest: GlobalBatchManifest,
    domain_id: str,
) -> DomainArtifactManifest | None:
    for node_manifest in manifest.node_manifests:
        for domain_manifest in node_manifest.domain_manifests:
            if domain_manifest.domain_id == domain_id:
                return domain_manifest
    return None


def _score_anchor_concept(
    *,
    query_tokens: set[str],
    canonical_name: str,
    description: str,
    fields: list[str],
) -> int:
    if not query_tokens:
        return 0
    canonical_tokens = _query_tokens(canonical_name)
    description_tokens = _query_tokens(description)
    field_tokens: set[str] = set()
    for raw_field in fields:
        field_tokens |= _query_tokens(raw_field)

    score = 0
    score += len(query_tokens & canonical_tokens) * 5
    score += len(query_tokens & description_tokens) * 2
    score += len(query_tokens & field_tokens)
    return score


def _top_field_evidence(fields: list[str], *, limit: int) -> list[str]:
    output: list[str] = []
    for raw_field in fields:
        field_ref = raw_field.strip()
        if not field_ref:
            continue
        output.append(field_ref)
        if len(output) >= limit:
            break
    return output


def _anchor_score_value(item: _AnchorConcept) -> int:
    score_obj = item.get("anchor_score", 0)
    return score_obj if isinstance(score_obj, int) else 0


def _composite_score_value(item: _AlignmentCandidate) -> float:
    score_obj = item.get("composite_score", 0.0)
    if isinstance(score_obj, int | float):
        return float(score_obj)
    return 0.0


def _alignment_row_evidence(row: _AlignmentRow) -> str:
    source_domain_obj = row.get("source_domain")
    source_table_obj = row.get("source_table")
    source_field_obj = row.get("source_field")
    target_domain_obj = row.get("target_domain")
    target_table_obj = row.get("target_table")
    target_field_obj = row.get("target_field")
    if not isinstance(source_domain_obj, str):
        return ""
    if not isinstance(source_table_obj, str):
        return ""
    if not isinstance(source_field_obj, str):
        return ""
    if not isinstance(target_domain_obj, str):
        return ""
    if not isinstance(target_table_obj, str):
        return ""
    if not isinstance(target_field_obj, str):
        return ""
    return (
        f"{source_domain_obj}.{source_table_obj}.{source_field_obj} -> "
        f"{target_domain_obj}.{target_table_obj}.{target_field_obj}"
    )


def _build_db_data_from_unified_fields(
    unified_fields: list[_UnifiedField],
) -> dict[str, dict[str, list[str]]]:
    db_data: dict[str, dict[str, list[str]]] = {}
    seen: dict[str, dict[str, set[str]]] = {}

    for item in unified_fields:
        fields_obj = item.get("fields")
        if not isinstance(fields_obj, list):
            continue
        for raw_field in fields_obj:
            if not isinstance(raw_field, str):
                continue
            parsed = _parse_field_ref(raw_field)
            if parsed is None:
                continue
            domain_id, table_name, field_name = parsed
            domain_map = db_data.setdefault(domain_id, {})
            domain_seen = seen.setdefault(domain_id, {})
            fields = domain_map.setdefault(table_name, [])
            table_seen = domain_seen.setdefault(table_name, set())
            if field_name in table_seen:
                continue
            table_seen.add(field_name)
            fields.append(field_name)

    return db_data


def _artifact_cid_from_batch_manifest(manifest: GlobalBatchManifest, artifact_type: ArtifactType) -> str:
    for artifact in manifest.artifacts:
        if artifact.artifact_type == artifact_type:
            return artifact.cid
    return ""


def _parse_field_ref(field_ref: str) -> tuple[str, str, str] | None:
    parts = field_ref.split(".")
    if len(parts) != 3:
        return None
    domain_id = parts[0].strip()
    table_name = parts[1].strip()
    field_name = parts[2].strip()
    if not domain_id or not table_name or not field_name:
        return None
    return domain_id, table_name, field_name


def _fallback_anchor_concept(query_text: str, source_domain: str) -> _AnchorConcept | None:
    tokens = [token for token in _query_tokens(query_text) if token]
    if not tokens:
        return None
    canonical_name = normalize_canonical_name("_".join(tokens[:6]), context="query_text")
    return {
        "source_domain": source_domain,
        "canonical_name": canonical_name,
        "anchor_score": 1,
        "evidence_fields": [],
    }


def _query_tokens(query_text: str) -> set[str]:
    return {token for token in re.split(r"[^0-9A-Za-z_]+", query_text.lower()) if token}


def _score_alignment_row(*, row: _AlignmentRow, query_tokens: set[str]) -> int:
    parts: list[str] = []
    for key in (
        "canonical_name",
        "source_table",
        "source_field",
        "target_table",
        "target_field",
    ):
        value = row.get(key)
        if isinstance(value, str):
            parts.append(value.lower())
    text = " ".join(parts)
    return sum(1 for token in query_tokens if token in text)


def _load_chain_write_config_from_defaults() -> ChainWriteConfig:
    binary_obj = DOMAIN_SHARE_DEFAULTS.get("ipfs_chain_bin")
    receiver_obj = DOMAIN_SHARE_DEFAULTS.get("receiver")
    rpc_obj = DOMAIN_SHARE_DEFAULTS.get("rpc_addr")
    ipfs_obj = DOMAIN_SHARE_DEFAULTS.get("ipfs_api")
    timeout_obj = DOMAIN_SHARE_DEFAULTS.get("timeout_sec")

    if not isinstance(binary_obj, str) or not binary_obj.strip():
        raise RuntimeError("DOMAIN_SHARE_DEFAULTS['ipfs_chain_bin'] must be a non-empty string")
    if not isinstance(receiver_obj, str) or not receiver_obj.strip():
        raise RuntimeError("DOMAIN_SHARE_DEFAULTS['receiver'] must be a non-empty string")
    if not isinstance(rpc_obj, str) or not rpc_obj.strip():
        raise RuntimeError("DOMAIN_SHARE_DEFAULTS['rpc_addr'] must be a non-empty string")
    if not isinstance(ipfs_obj, str) or not ipfs_obj.strip():
        raise RuntimeError("DOMAIN_SHARE_DEFAULTS['ipfs_api'] must be a non-empty string")
    if not isinstance(timeout_obj, int):
        raise RuntimeError("DOMAIN_SHARE_DEFAULTS['timeout_sec'] must be an integer")

    return ChainWriteConfig(
        ipfs_chain_bin=Path(binary_obj.strip()),
        receiver=receiver_obj.strip(),
        rpc_addr=rpc_obj.strip(),
        ipfs_api=ipfs_obj.strip(),
        timeout_sec=max(3, timeout_obj),
    )


def _validate_chain_write_config(config: ChainWriteConfig) -> None:
    if not config.ipfs_chain_bin.is_file():
        raise RuntimeError(f"ipfs-chain binary not found: {config.ipfs_chain_bin}")
    if _CHAIN_RECEIVER_PATTERN.fullmatch(config.receiver) is None:
        raise RuntimeError("chain receiver must be a 20-byte hex address")
    if not config.rpc_addr.strip():
        raise RuntimeError("chain rpc address must be non-empty")
    if not config.ipfs_api.strip():
        raise RuntimeError("chain ipfs api address must be non-empty")
    if config.timeout_sec < 3:
        raise RuntimeError("chain timeout_sec must be >= 3")


def _build_manifest_chain_key(*, run_id: str, batch_id: str) -> str:
    normalized_run_id = validate_token(run_id, context="run_id")
    normalized_batch_id = validate_token(batch_id, context="batch_id")
    return f"REGISTER_RUN_MANIFEST:{normalized_run_id}:{normalized_batch_id}"


def _put_file_on_chain(
    *,
    config: ChainWriteConfig,
    chain_key: str,
    file_path: Path,
) -> tuple[str, str]:
    cmd = [
        str(config.ipfs_chain_bin),
        "put",
        "-receiver",
        config.receiver,
        "-key",
        chain_key,
        "-file",
        str(file_path),
        "-rpc",
        config.rpc_addr,
        "-ipfs",
        config.ipfs_api,
        "-timeout",
        str(config.timeout_sec),
    ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(3, config.timeout_sec + 2),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"ipfs-chain put timed out for key={chain_key}") from exc

    if proc.returncode != 0:
        raise RuntimeError(
            "ipfs-chain put failed for global manifest\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )

    cid_match = _CHAIN_CID_PATTERN.search(proc.stdout)
    tx_hash_match = _CHAIN_TXHASH_PATTERN.search(proc.stdout)
    if cid_match is None or tx_hash_match is None:
        raise RuntimeError("failed to parse CID/TxHash from ipfs-chain output")

    return cid_match.group(1), tx_hash_match.group(1)
