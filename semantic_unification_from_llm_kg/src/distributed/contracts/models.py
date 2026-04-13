from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_MAX_QUERY_LENGTH = 512
_MAX_DOMAIN_COUNT = 128
_MAX_CANONICAL_NAME_LENGTH = 128
_MAX_SUBQUERY_LIMIT = 1000
_SUPPORTED_FILTER_OPERATORS = {"eq", "neq", "gt", "gte", "lt", "lte", "like", "in"}

ShareMode = Literal["semantic_only", "include_samples"]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def validate_token(value: str, *, context: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise RuntimeError(f"{context} must be a non-empty string")
    if _TOKEN_PATTERN.fullmatch(normalized) is None:
        raise RuntimeError(
            f"{context} contains invalid characters. Allowed: letters, digits, underscore, hyphen."
        )
    return normalized


def normalize_canonical_name(value: str, *, context: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        raise RuntimeError(f"{context} must be non-empty")
    if len(normalized) > _MAX_CANONICAL_NAME_LENGTH:
        raise RuntimeError(f"{context} exceeds max length")
    return normalized


def _required_str(
    payload: dict[str, object],
    key: str,
    *,
    context: str,
) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise RuntimeError(f"{context}.{key} must be a string")
    normalized = value.strip()
    if not normalized:
        raise RuntimeError(f"{context}.{key} must be non-empty")
    return normalized


def _required_int(
    payload: dict[str, object],
    key: str,
    *,
    context: str,
) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise RuntimeError(f"{context}.{key} must be an integer")
    return value


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ArtifactType(StrEnum):
    FIELD_DESCRIPTIONS = "field_descriptions"
    DOMAIN_UNIFIED = "domain_unified"
    DOMAIN_KG = "domain_kg"
    SAMPLES = "samples"
    UNIFIED_FIELDS = "unified_fields"
    ALIGNMENT_INDEX = "alignment_index"
    ALIGNMENT_CYPHER = "alignment_cypher"
    GLOBAL_MANIFEST = "global_manifest"
    NODE_MANIFEST = "node_manifest"


@dataclass(frozen=True)
class ArtifactRef:
    artifact_type: ArtifactType
    cid: str
    sha256: str
    record_count: int
    security_level: ShareMode = "semantic_only"

    def to_dict(self) -> dict[str, object]:
        return {
            "type": self.artifact_type.value,
            "cid": self.cid,
            "sha256": self.sha256,
            "record_count": self.record_count,
            "security_level": self.security_level,
        }

    @staticmethod
    def from_dict(payload: dict[str, object]) -> ArtifactRef:
        type_text = _required_str(payload, "type", context="artifact")
        cid = _required_str(payload, "cid", context="artifact")
        sha256 = _required_str(payload, "sha256", context="artifact")
        record_count = _required_int(payload, "record_count", context="artifact")
        security_level_obj = payload.get("security_level", "semantic_only")
        if security_level_obj not in {"semantic_only", "include_samples"}:
            raise RuntimeError("artifact.security_level must be semantic_only or include_samples")
        security_level: ShareMode
        if security_level_obj == "include_samples":
            security_level = "include_samples"
        else:
            security_level = "semantic_only"
        try:
            artifact_type = ArtifactType(type_text)
        except ValueError as exc:
            raise RuntimeError(f"unsupported artifact type: {type_text}") from exc
        return ArtifactRef(
            artifact_type=artifact_type,
            cid=cid,
            sha256=sha256,
            record_count=record_count,
            security_level=security_level,
        )


@dataclass(frozen=True)
class DomainArtifactManifest:
    schema_version: str
    run_id: str
    domain_id: str
    created_at: str
    status: JobStatus
    artifacts: tuple[ArtifactRef, ...]
    failed_stage: str = ""
    error_message: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "domain_id": self.domain_id,
            "created_at": self.created_at,
            "status": self.status.value,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "failed_stage": self.failed_stage,
            "error_message": self.error_message,
        }

    @staticmethod
    def from_dict(payload: dict[str, object]) -> DomainArtifactManifest:
        schema_version = _required_str(payload, "schema_version", context="domain_manifest")
        run_id = _required_str(payload, "run_id", context="domain_manifest")
        domain_id = _required_str(payload, "domain_id", context="domain_manifest")
        created_at = _required_str(payload, "created_at", context="domain_manifest")
        status_text = _required_str(payload, "status", context="domain_manifest")
        try:
            status = JobStatus(status_text)
        except ValueError as exc:
            raise RuntimeError(f"domain_manifest.status is invalid: {status_text}") from exc

        artifacts_obj = payload.get("artifacts")
        if not isinstance(artifacts_obj, list):
            raise RuntimeError("domain_manifest.artifacts must be a list")
        artifacts = tuple(
            ArtifactRef.from_dict(item)
            for item in artifacts_obj
            if isinstance(item, dict)
        )
        if len(artifacts) != len(artifacts_obj):
            raise RuntimeError("domain_manifest.artifacts items must be objects")

        failed_stage_obj = payload.get("failed_stage", "")
        error_message_obj = payload.get("error_message", "")
        if not isinstance(failed_stage_obj, str):
            raise RuntimeError("domain_manifest.failed_stage must be a string")
        if not isinstance(error_message_obj, str):
            raise RuntimeError("domain_manifest.error_message must be a string")
        return DomainArtifactManifest(
            schema_version=schema_version,
            run_id=run_id,
            domain_id=domain_id,
            created_at=created_at,
            status=status,
            artifacts=artifacts,
            failed_stage=failed_stage_obj,
            error_message=error_message_obj,
        )


@dataclass(frozen=True)
class NodeJobManifest:
    schema_version: str
    run_id: str
    node_id: str
    job_id: str
    created_at: str
    status: JobStatus
    domain_manifests: tuple[DomainArtifactManifest, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "node_id": self.node_id,
            "job_id": self.job_id,
            "created_at": self.created_at,
            "status": self.status.value,
            "domain_manifests": [manifest.to_dict() for manifest in self.domain_manifests],
        }

    @staticmethod
    def from_dict(payload: dict[str, object]) -> NodeJobManifest:
        schema_version = _required_str(payload, "schema_version", context="node_manifest")
        run_id = _required_str(payload, "run_id", context="node_manifest")
        node_id = _required_str(payload, "node_id", context="node_manifest")
        job_id = _required_str(payload, "job_id", context="node_manifest")
        created_at = _required_str(payload, "created_at", context="node_manifest")
        status_text = _required_str(payload, "status", context="node_manifest")
        try:
            status = JobStatus(status_text)
        except ValueError as exc:
            raise RuntimeError(f"node_manifest.status is invalid: {status_text}") from exc

        domain_items_obj = payload.get("domain_manifests")
        if not isinstance(domain_items_obj, list):
            raise RuntimeError("node_manifest.domain_manifests must be a list")
        domain_manifests = tuple(
            DomainArtifactManifest.from_dict(item)
            for item in domain_items_obj
            if isinstance(item, dict)
        )
        if len(domain_manifests) != len(domain_items_obj):
            raise RuntimeError("node_manifest.domain_manifests items must be objects")

        return NodeJobManifest(
            schema_version=schema_version,
            run_id=run_id,
            node_id=node_id,
            job_id=job_id,
            created_at=created_at,
            status=status,
            domain_manifests=domain_manifests,
        )


@dataclass(frozen=True)
class LocalJobRequest:
    run_id: str
    source_names: tuple[str, ...] = ()
    max_fields_per_source: int = 0
    mock_llm: bool = True
    share_mode: ShareMode = "semantic_only"

    def normalized(self) -> LocalJobRequest:
        run_id = validate_token(self.run_id, context="run_id")
        normalized_sources = tuple(
            validate_token(name, context="source_names[]") for name in self.source_names
        )
        if self.max_fields_per_source < 0:
            raise RuntimeError("max_fields_per_source must be >= 0")
        if self.share_mode not in {"semantic_only", "include_samples"}:
            raise RuntimeError("share_mode must be semantic_only or include_samples")
        return LocalJobRequest(
            run_id=run_id,
            source_names=normalized_sources,
            max_fields_per_source=self.max_fields_per_source,
            mock_llm=self.mock_llm,
            share_mode=self.share_mode,
        )


@dataclass(frozen=True)
class LocalQueryRequest:
    query_text: str
    limit: int = 20
    source_name: str | None = None

    def normalized(self) -> LocalQueryRequest:
        query_text = self.query_text.strip()
        if not query_text:
            raise RuntimeError("query_text must be non-empty")
        if len(query_text) > _MAX_QUERY_LENGTH:
            raise RuntimeError("query_text exceeds max length")
        if self.limit <= 0 or self.limit > 200:
            raise RuntimeError("limit must be in range [1, 200]")
        source_name = None
        if self.source_name is not None:
            source_name = validate_token(self.source_name, context="source_name")
        return LocalQueryRequest(
            query_text=query_text,
            limit=self.limit,
            source_name=source_name,
        )


@dataclass(frozen=True)
class LocalQueryHit:
    domain_id: str
    table: str
    field: str
    description: str
    score: int

    def to_dict(self) -> dict[str, object]:
        return {
            "domain_id": self.domain_id,
            "table": self.table,
            "field": self.field,
            "description": self.description,
            "score": self.score,
        }


@dataclass(frozen=True)
class LocalConceptQueryRequest:
    canonical_names: tuple[str, ...]
    limit: int = 50
    source_name: str | None = None

    def normalized(self) -> LocalConceptQueryRequest:
        if not self.canonical_names:
            raise RuntimeError("canonical_names must not be empty")
        normalized_names = tuple(
            normalize_canonical_name(item, context="canonical_names[]")
            for item in self.canonical_names
        )
        if self.limit <= 0 or self.limit > 500:
            raise RuntimeError("limit must be in range [1, 500]")
        source_name = None
        if self.source_name is not None:
            source_name = validate_token(self.source_name, context="source_name")
        return LocalConceptQueryRequest(
            canonical_names=normalized_names,
            limit=self.limit,
            source_name=source_name,
        )


@dataclass(frozen=True)
class LocalConceptHit:
    domain_id: str
    canonical_name: str
    table: str
    field: str
    description: str
    score: int
    relation_type: str = ""
    alignment_score: float = 0.0
    resource_cids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "domain_id": self.domain_id,
            "canonical_name": self.canonical_name,
            "table": self.table,
            "field": self.field,
            "description": self.description,
            "score": self.score,
            "relation_type": self.relation_type,
            "alignment_score": self.alignment_score,
            "resource_cids": list(self.resource_cids),
        }


def _normalize_filter_scalar(value: object, *, context: str) -> str | int | float | bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise RuntimeError(f"{context} must be non-empty")
        if len(normalized) > _MAX_QUERY_LENGTH:
            raise RuntimeError(f"{context} exceeds max length")
        return normalized
    raise RuntimeError(f"{context} must be bool/int/float/string")


@dataclass(frozen=True)
class LocalSubQueryFilter:
    field: str
    operator: str = "eq"
    value: object = ""

    def normalized(self) -> LocalSubQueryFilter:
        field = validate_token(self.field, context="local_subquery_filter.field")
        operator = self.operator.strip().lower()
        if operator not in _SUPPORTED_FILTER_OPERATORS:
            raise RuntimeError(
                "local_subquery_filter.operator must be one of: "
                + ", ".join(sorted(_SUPPORTED_FILTER_OPERATORS))
            )

        if operator == "in":
            if not isinstance(self.value, list | tuple):
                raise RuntimeError("local_subquery_filter.value must be a list for operator='in'")
            normalized_values = tuple(
                _normalize_filter_scalar(item, context="local_subquery_filter.value[]")
                for item in self.value
            )
            if not normalized_values:
                raise RuntimeError("local_subquery_filter.value must not be empty for operator='in'")
            return LocalSubQueryFilter(
                field=field,
                operator=operator,
                value=normalized_values,
            )

        normalized_value = _normalize_filter_scalar(
            self.value,
            context="local_subquery_filter.value",
        )
        return LocalSubQueryFilter(
            field=field,
            operator=operator,
            value=normalized_value,
        )

    def to_dict(self) -> dict[str, object]:
        value: object = self.value
        if isinstance(value, tuple):
            value = list(value)
        return {
            "field": self.field,
            "operator": self.operator,
            "value": value,
        }


@dataclass(frozen=True)
class LocalSubQueryRequest:
    source_name: str
    table: str
    select_fields: tuple[str, ...]
    filters: tuple[LocalSubQueryFilter, ...] = ()
    limit: int = 100

    def normalized(self) -> LocalSubQueryRequest:
        source_name = validate_token(self.source_name, context="local_subquery.source_name")
        table = validate_token(self.table, context="local_subquery.table")
        if not self.select_fields:
            raise RuntimeError("local_subquery.select_fields must not be empty")
        select_fields = tuple(
            validate_token(field, context="local_subquery.select_fields[]")
            for field in self.select_fields
        )
        if self.limit <= 0 or self.limit > _MAX_SUBQUERY_LIMIT:
            raise RuntimeError(f"local_subquery.limit must be in range [1, {_MAX_SUBQUERY_LIMIT}]")
        normalized_filters = tuple(item.normalized() for item in self.filters)
        return LocalSubQueryRequest(
            source_name=source_name,
            table=table,
            select_fields=select_fields,
            filters=normalized_filters,
            limit=self.limit,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "source_name": self.source_name,
            "table": self.table,
            "select_fields": list(self.select_fields),
            "filters": [item.to_dict() for item in self.filters],
            "limit": self.limit,
        }


@dataclass(frozen=True)
class LocalSubQueryRow:
    domain_id: str
    table: str
    data: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "domain_id": self.domain_id,
            "table": self.table,
            "data": self.data,
        }


@dataclass(frozen=True)
class DomainRegistration:
    domain_id: str
    endpoint: str
    access_token: str = ""

    def normalized(self) -> DomainRegistration:
        domain_id = validate_token(self.domain_id, context="domain_id")
        endpoint = self.endpoint.strip().rstrip("/")
        if not endpoint:
            raise RuntimeError("endpoint must be non-empty")
        if not (endpoint.startswith("http://") or endpoint.startswith("https://")):
            raise RuntimeError("endpoint must start with http:// or https://")
        token = self.access_token.strip()
        return DomainRegistration(
            domain_id=domain_id,
            endpoint=endpoint,
            access_token=token,
        )


@dataclass(frozen=True)
class BatchRequest:
    run_id: str
    domain_ids: tuple[str, ...]
    max_fields_per_source: int = 0
    mock_llm: bool = True
    share_mode: ShareMode = "semantic_only"
    poll_interval_sec: float = 0.5
    poll_timeout_sec: float = 300.0

    def normalized(self) -> BatchRequest:
        run_id = validate_token(self.run_id, context="run_id")
        if not self.domain_ids:
            raise RuntimeError("domain_ids must not be empty")
        if len(self.domain_ids) > _MAX_DOMAIN_COUNT:
            raise RuntimeError("domain_ids exceeds maximum allowed count")
        domain_ids = tuple(validate_token(item, context="domain_ids[]") for item in self.domain_ids)
        if self.max_fields_per_source < 0:
            raise RuntimeError("max_fields_per_source must be >= 0")
        if self.share_mode not in {"semantic_only", "include_samples"}:
            raise RuntimeError("share_mode must be semantic_only or include_samples")
        if self.poll_interval_sec <= 0:
            raise RuntimeError("poll_interval_sec must be > 0")
        if self.poll_timeout_sec <= 0:
            raise RuntimeError("poll_timeout_sec must be > 0")
        return BatchRequest(
            run_id=run_id,
            domain_ids=domain_ids,
            max_fields_per_source=self.max_fields_per_source,
            mock_llm=self.mock_llm,
            share_mode=self.share_mode,
            poll_interval_sec=self.poll_interval_sec,
            poll_timeout_sec=self.poll_timeout_sec,
        )


@dataclass(frozen=True)
class GlobalBatchManifest:
    schema_version: str
    run_id: str
    batch_id: str
    created_at: str
    status: JobStatus
    node_manifests: tuple[NodeJobManifest, ...]
    artifacts: tuple[ArtifactRef, ...] = ()
    manifest_chain_cid: str = ""
    manifest_chain_key: str = ""
    manifest_tx_hash: str = ""
    error_message: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "created_at": self.created_at,
            "status": self.status.value,
            "node_manifests": [item.to_dict() for item in self.node_manifests],
            "artifacts": [item.to_dict() for item in self.artifacts],
            "manifest_chain_cid": self.manifest_chain_cid,
            "manifest_chain_key": self.manifest_chain_key,
            "manifest_tx_hash": self.manifest_tx_hash,
            "error_message": self.error_message,
        }
