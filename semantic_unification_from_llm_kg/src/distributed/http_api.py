from __future__ import annotations

import json
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.distributed.contracts import (
    BatchRequest,
    DomainRegistration,
    LocalConceptQueryRequest,
    LocalJobRequest,
    LocalQueryRequest,
    LocalSubQueryFilter,
    LocalSubQueryRequest,
    ShareMode,
)
from src.distributed.local_agent_service import LocalDomainAgentService
from src.distributed.orchestrator_service import GlobalOrchestratorService

type _JsonObject = dict[str, object]

_LOCAL_JOB_STATUS_PATTERN = re.compile(r"^/v1/jobs/([A-Za-z0-9_-]{1,64})$")
_LOCAL_JOB_MANIFEST_PATTERN = re.compile(r"^/v1/jobs/([A-Za-z0-9_-]{1,64})/manifest$")
_GLOBAL_BATCH_STATUS_PATTERN = re.compile(r"^/v1/batches/([A-Za-z0-9_-]{1,64})$")
_GLOBAL_BATCH_MANIFEST_PATTERN = re.compile(r"^/v1/batches/([A-Za-z0-9_-]{1,64})/manifest$")


def serve_local_agent(
    *,
    service: LocalDomainAgentService,
    host: str,
    port: int,
    access_token: str,
) -> None:
    handler_cls = _build_local_handler(service=service, access_token=access_token)
    server = ThreadingHTTPServer((host, port), handler_cls)
    server.serve_forever()


def serve_orchestrator(
    *,
    service: GlobalOrchestratorService,
    host: str,
    port: int,
) -> None:
    handler_cls = _build_orchestrator_handler(service)
    server = ThreadingHTTPServer((host, port), handler_cls)
    server.serve_forever()


def _build_local_handler(
    *,
    service: LocalDomainAgentService,
    access_token: str,
) -> type[BaseHTTPRequestHandler]:
    class _LocalHandler(BaseHTTPRequestHandler):
        _service = service
        _access_token = access_token

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

        def do_POST(self) -> None:  # noqa: N802
            if self.path == "/v1/jobs":
                if not _check_agent_auth(self, self._access_token):
                    return
                body = _read_json_object(self)
                if body is None:
                    return
                try:
                    source_names = _coerce_string_tuple(body.get("source_names", []), context="source_names")
                    max_fields = _coerce_non_negative_int(
                        body.get("max_fields_per_source", 0),
                        context="max_fields_per_source",
                    )
                    mock_llm = _coerce_bool(body.get("mock_llm", True), context="mock_llm")
                    share_mode = _coerce_share_mode(body.get("share_mode", "semantic_only"))
                    run_id = _coerce_non_empty_string(body.get("run_id"), context="run_id")
                    job_id = self._service.create_job(
                        LocalJobRequest(
                            run_id=run_id,
                            source_names=source_names,
                            max_fields_per_source=max_fields,
                            mock_llm=mock_llm,
                            share_mode=share_mode,
                        )
                    )
                    _write_json(self, HTTPStatus.OK, {"job_id": job_id})
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            if self.path == "/v1/query/local-exec":
                if not _check_agent_auth(self, self._access_token):
                    return
                body = _read_json_object(self)
                if body is None:
                    return
                try:
                    query_text = _coerce_non_empty_string(body.get("query_text"), context="query_text")
                    limit = _coerce_positive_int(body.get("limit", 20), context="limit")
                    source_name_obj = body.get("source_name")
                    source_name: str | None
                    if source_name_obj is None:
                        source_name = None
                    else:
                        source_name = _coerce_non_empty_string(source_name_obj, context="source_name")
                    hits = self._service.execute_local_query(
                        LocalQueryRequest(
                            query_text=query_text,
                            limit=limit,
                            source_name=source_name,
                        )
                    )
                    _write_json(self, HTTPStatus.OK, {"hits": [item.to_dict() for item in hits]})
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            if self.path == "/v1/query/local-concept":
                if not _check_agent_auth(self, self._access_token):
                    return
                body = _read_json_object(self)
                if body is None:
                    return
                try:
                    canonical_names = _coerce_string_tuple(
                        body.get("canonical_names", []),
                        context="canonical_names",
                    )
                    limit = _coerce_positive_int(body.get("limit", 50), context="limit")
                    source_name_obj = body.get("source_name")
                    concept_source_name: str | None
                    if source_name_obj is None:
                        concept_source_name = None
                    else:
                        concept_source_name = _coerce_non_empty_string(
                            source_name_obj,
                            context="source_name",
                        )
                    concept_hits = self._service.execute_local_concept_query(
                        LocalConceptQueryRequest(
                            canonical_names=canonical_names,
                            limit=limit,
                            source_name=concept_source_name,
                        )
                    )
                    _write_json(self, HTTPStatus.OK, {"hits": [item.to_dict() for item in concept_hits]})
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            if self.path == "/v1/query/subquery-exec":
                if not _check_agent_auth(self, self._access_token):
                    return
                body = _read_json_object(self)
                if body is None:
                    return
                try:
                    source_name = _coerce_non_empty_string(body.get("source_name"), context="source_name")
                    table = _coerce_non_empty_string(body.get("table"), context="table")
                    select_fields = _coerce_string_tuple(
                        body.get("select_fields", []),
                        context="select_fields",
                    )
                    filters = _coerce_subquery_filters(
                        body.get("filters", []),
                        context="filters",
                    )
                    limit = _coerce_positive_int(body.get("limit", 100), context="limit")
                    rows = self._service.execute_local_subquery(
                        LocalSubQueryRequest(
                            source_name=source_name,
                            table=table,
                            select_fields=select_fields,
                            filters=filters,
                            limit=limit,
                        )
                    )
                    _write_json(self, HTTPStatus.OK, {"rows": [item.to_dict() for item in rows]})
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            _write_json(self, HTTPStatus.NOT_FOUND, {"error": "path not found"})

        def do_GET(self) -> None:  # noqa: N802
            if not _check_agent_auth(self, self._access_token):
                return

            status_match = _LOCAL_JOB_STATUS_PATTERN.fullmatch(self.path)
            if status_match is not None:
                job_id = status_match.group(1)
                try:
                    payload = self._service.get_job_status(job_id)
                    _write_json(self, HTTPStatus.OK, payload)
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            manifest_match = _LOCAL_JOB_MANIFEST_PATTERN.fullmatch(self.path)
            if manifest_match is not None:
                job_id = manifest_match.group(1)
                try:
                    manifest = self._service.get_job_manifest(job_id)
                    _write_json(self, HTTPStatus.OK, manifest.to_dict())
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            _write_json(self, HTTPStatus.NOT_FOUND, {"error": "path not found"})

    return _LocalHandler


def _build_orchestrator_handler(
    service: GlobalOrchestratorService,
) -> type[BaseHTTPRequestHandler]:
    class _OrchestratorHandler(BaseHTTPRequestHandler):
        _service = service

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

        def do_POST(self) -> None:  # noqa: N802
            if self.path == "/v1/domains/register":
                body = _read_json_object(self)
                if body is None:
                    return
                try:
                    domain_id = _coerce_non_empty_string(body.get("domain_id"), context="domain_id")
                    endpoint = _coerce_non_empty_string(body.get("endpoint"), context="endpoint")
                    token_obj = body.get("access_token", "")
                    token = _coerce_string(token_obj, context="access_token")
                    self._service.register_domain(
                        DomainRegistration(domain_id=domain_id, endpoint=endpoint, access_token=token)
                    )
                    _write_json(self, HTTPStatus.OK, {"registered": domain_id})
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            if self.path == "/v1/batches":
                body = _read_json_object(self)
                if body is None:
                    return
                try:
                    run_id = _coerce_non_empty_string(body.get("run_id"), context="run_id")
                    domain_ids = _coerce_string_tuple(body.get("domain_ids", []), context="domain_ids")
                    max_fields = _coerce_non_negative_int(
                        body.get("max_fields_per_source", 0),
                        context="max_fields_per_source",
                    )
                    mock_llm = _coerce_bool(body.get("mock_llm", True), context="mock_llm")
                    share_mode = _coerce_share_mode(body.get("share_mode", "semantic_only"))
                    poll_interval = _coerce_positive_float(
                        body.get("poll_interval_sec", 0.5),
                        context="poll_interval_sec",
                    )
                    poll_timeout = _coerce_positive_float(
                        body.get("poll_timeout_sec", 300.0),
                        context="poll_timeout_sec",
                    )
                    batch_id = self._service.create_batch(
                        BatchRequest(
                            run_id=run_id,
                            domain_ids=domain_ids,
                            max_fields_per_source=max_fields,
                            mock_llm=mock_llm,
                            share_mode=share_mode,
                            poll_interval_sec=poll_interval,
                            poll_timeout_sec=poll_timeout,
                        )
                    )
                    _write_json(self, HTTPStatus.OK, {"batch_id": batch_id})
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            if self.path == "/v1/query":
                body = _read_json_object(self)
                if body is None:
                    return
                try:
                    query_text = _coerce_non_empty_string(body.get("query_text"), context="query_text")
                    limit = _coerce_positive_int(body.get("limit", 20), context="limit")
                    domain_ids = _coerce_string_tuple(body.get("domain_ids", []), context="domain_ids")
                    hits = self._service.query(query_text=query_text, limit=limit, domain_ids=domain_ids)
                    _write_json(self, HTTPStatus.OK, {"hits": [item.to_dict() for item in hits]})
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            if self.path == "/v1/query/federated":
                body = _read_json_object(self)
                if body is None:
                    return
                try:
                    query_text = _coerce_non_empty_string(body.get("query_text"), context="query_text")
                    source_domain = _coerce_non_empty_string(
                        body.get("source_domain"),
                        context="source_domain",
                    )
                    limit = _coerce_positive_int(body.get("limit", 20), context="limit")
                    target_domain_ids = _coerce_string_tuple(
                        body.get("target_domain_ids", []),
                        context="target_domain_ids",
                    )
                    payload = self._service.query_federated(
                        query_text=query_text,
                        source_domain=source_domain,
                        target_domain_ids=target_domain_ids,
                        limit=limit,
                    )
                    _write_json(self, HTTPStatus.OK, payload)
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            if self.path == "/v1/query/federated-exec":
                body = _read_json_object(self)
                if body is None:
                    return
                try:
                    query_text = _coerce_non_empty_string(body.get("query_text"), context="query_text")
                    limit = _coerce_positive_int(body.get("limit", 20), context="limit")
                    source_domain_obj = body.get("source_domain", "")
                    source_domain = _coerce_string(source_domain_obj, context="source_domain")
                    domain_ids = _coerce_string_tuple(body.get("domain_ids", []), context="domain_ids")
                    payload = self._service.query_federated_execute(
                        query_text=query_text,
                        limit=limit,
                        source_domain=source_domain,
                        domain_ids=domain_ids,
                    )
                    _write_json(self, HTTPStatus.OK, payload)
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            _write_json(self, HTTPStatus.NOT_FOUND, {"error": "path not found"})

        def do_GET(self) -> None:  # noqa: N802
            status_match = _GLOBAL_BATCH_STATUS_PATTERN.fullmatch(self.path)
            if status_match is not None:
                batch_id = status_match.group(1)
                try:
                    payload = self._service.get_batch_status(batch_id)
                    _write_json(self, HTTPStatus.OK, payload)
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            manifest_match = _GLOBAL_BATCH_MANIFEST_PATTERN.fullmatch(self.path)
            if manifest_match is not None:
                batch_id = manifest_match.group(1)
                try:
                    manifest = self._service.get_batch_manifest(batch_id)
                    _write_json(self, HTTPStatus.OK, manifest.to_dict())
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return

            _write_json(self, HTTPStatus.NOT_FOUND, {"error": "path not found"})

    return _OrchestratorHandler


def _check_agent_auth(handler: BaseHTTPRequestHandler, access_token: str) -> bool:
    expected = access_token.strip()
    if not expected:
        return True
    provided = handler.headers.get("X-Agent-Token", "").strip()
    if provided == expected:
        return True
    _write_json(handler, HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
    return False


def _read_json_object(handler: BaseHTTPRequestHandler) -> _JsonObject | None:
    content_length_text = handler.headers.get("Content-Length", "")
    try:
        content_length = int(content_length_text)
    except ValueError:
        _write_json(handler, HTTPStatus.BAD_REQUEST, {"error": "invalid content length"})
        return None

    if content_length <= 0 or content_length > 1024 * 1024:
        _write_json(handler, HTTPStatus.BAD_REQUEST, {"error": "invalid payload size"})
        return None

    body = handler.rfile.read(content_length)
    try:
        parsed = json.loads(body.decode("utf-8"))
    except Exception:  # noqa: BLE001
        _write_json(handler, HTTPStatus.BAD_REQUEST, {"error": "invalid json body"})
        return None

    if not isinstance(parsed, dict):
        _write_json(handler, HTTPStatus.BAD_REQUEST, {"error": "json body must be an object"})
        return None
    return parsed


def _write_json(handler: BaseHTTPRequestHandler, status: HTTPStatus, payload: _JsonObject) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status.value)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _coerce_non_empty_string(value: object, *, context: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"{context} must be a string")
    normalized = value.strip()
    if not normalized:
        raise RuntimeError(f"{context} must be non-empty")
    return normalized


def _coerce_string(value: object, *, context: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"{context} must be a string")
    return value.strip()


def _coerce_string_tuple(value: object, *, context: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise RuntimeError(f"{context} must be a list")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise RuntimeError(f"{context} items must be strings")
        normalized = item.strip()
        if not normalized:
            raise RuntimeError(f"{context} items must be non-empty")
        result.append(normalized)
    return tuple(result)


def _coerce_subquery_filters(value: object, *, context: str) -> tuple[LocalSubQueryFilter, ...]:
    if not isinstance(value, list):
        raise RuntimeError(f"{context} must be a list")
    output: list[LocalSubQueryFilter] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise RuntimeError(f"{context}[{index}] must be an object")
        field = _coerce_non_empty_string(item.get("field"), context=f"{context}[{index}].field")
        operator = _coerce_non_empty_string(
            item.get("operator", "eq"),
            context=f"{context}[{index}].operator",
        )
        if "value" not in item:
            raise RuntimeError(f"{context}[{index}].value is required")
        output.append(
            LocalSubQueryFilter(
                field=field,
                operator=operator,
                value=item.get("value"),
            )
        )
    return tuple(output)


def _coerce_non_negative_int(value: object, *, context: str) -> int:
    if not isinstance(value, int):
        raise RuntimeError(f"{context} must be an integer")
    if value < 0:
        raise RuntimeError(f"{context} must be >= 0")
    return value


def _coerce_positive_int(value: object, *, context: str) -> int:
    if not isinstance(value, int):
        raise RuntimeError(f"{context} must be an integer")
    if value <= 0:
        raise RuntimeError(f"{context} must be > 0")
    return value


def _coerce_positive_float(value: object, *, context: str) -> float:
    if isinstance(value, int | float):
        result = float(value)
    else:
        raise RuntimeError(f"{context} must be numeric")
    if result <= 0:
        raise RuntimeError(f"{context} must be > 0")
    return result


def _coerce_bool(value: object, *, context: str) -> bool:
    if not isinstance(value, bool):
        raise RuntimeError(f"{context} must be a boolean")
    return value


def _coerce_share_mode(value: object) -> ShareMode:
    if not isinstance(value, str):
        raise RuntimeError("share_mode must be a string")
    normalized = value.strip()
    if normalized not in {"semantic_only", "include_samples"}:
        raise RuntimeError("share_mode must be semantic_only or include_samples")
    if normalized == "include_samples":
        return "include_samples"
    return "semantic_only"
