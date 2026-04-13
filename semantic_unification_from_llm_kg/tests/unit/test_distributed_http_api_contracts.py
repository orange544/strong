from __future__ import annotations

import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.distributed.contracts import LocalSubQueryRequest, LocalSubQueryRow  # noqa: E402
from src.distributed.http_api import _build_local_handler, _build_orchestrator_handler  # noqa: E402


def _serve(handler_cls: type) -> tuple[ThreadingHTTPServer, str, threading.Thread]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, base_url, thread


def test_local_http_contract_subquery_exec_endpoint() -> None:
    class _FakeLocalService:
        def execute_local_subquery(self, request: LocalSubQueryRequest) -> list[LocalSubQueryRow]:
            normalized = request.normalized()
            assert normalized.source_name == "domain_a"
            assert normalized.table == "students"
            assert normalized.select_fields == ("student_id", "name")
            return [
                LocalSubQueryRow(
                    domain_id="domain_a",
                    table="students",
                    data={"student_id": "20230001", "name": "Alice"},
                )
            ]

    handler_cls = _build_local_handler(service=_FakeLocalService(), access_token="token_local")
    server, base_url, thread = _serve(handler_cls)
    try:
        response = requests.post(
            f"{base_url}/v1/query/subquery-exec",
            headers={"Content-Type": "application/json", "X-Agent-Token": "token_local"},
            json={
                "source_name": "domain_a",
                "table": "students",
                "select_fields": ["student_id", "name"],
                "filters": [{"field": "student_id", "operator": "eq", "value": "20230001"}],
                "limit": 20,
            },
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        assert isinstance(payload, dict)
        rows = payload.get("rows")
        assert isinstance(rows, list)
        assert rows == [
            {
                "domain_id": "domain_a",
                "table": "students",
                "data": {"student_id": "20230001", "name": "Alice"},
            }
        ]
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_orchestrator_http_contract_federated_exec_endpoint() -> None:
    class _FakeOrchestratorService:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int, str, tuple[str, ...]]] = []

        def query_federated_execute(
            self,
            *,
            query_text: str,
            limit: int,
            source_domain: str,
            domain_ids: tuple[str, ...],
        ) -> dict[str, object]:
            self.calls.append((query_text, limit, source_domain, domain_ids))
            return {
                "query": query_text,
                "source_domain": source_domain,
                "domains_requested": list(domain_ids),
                "results": [{"entity_id": "20230001"}],
            }

    fake_service = _FakeOrchestratorService()
    handler_cls = _build_orchestrator_handler(fake_service)
    server, base_url, thread = _serve(handler_cls)
    try:
        response = requests.post(
            f"{base_url}/v1/query/federated-exec",
            headers={"Content-Type": "application/json"},
            json={
                "query_text": "query student_id = 20230001",
                "source_domain": "domain_a",
                "domain_ids": ["domain_a", "domain_b"],
                "limit": 10,
            },
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()
        assert isinstance(payload, dict)
        assert payload.get("query") == "query student_id = 20230001"
        assert payload.get("source_domain") == "domain_a"
        assert payload.get("domains_requested") == ["domain_a", "domain_b"]
        assert fake_service.calls == [
            ("query student_id = 20230001", 10, "domain_a", ("domain_a", "domain_b"))
        ]
    finally:
        server.shutdown()
        thread.join(timeout=2)
