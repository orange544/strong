from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.distributed.local_agent_service import LocalDomainAgentService
    from src.distributed.orchestrator_service import GlobalOrchestratorService, LocalAgentHttpClient

__all__ = ["LocalDomainAgentService", "GlobalOrchestratorService", "LocalAgentHttpClient"]


def __getattr__(name: str):
    if name == "LocalDomainAgentService":
        from src.distributed.local_agent_service import LocalDomainAgentService

        return LocalDomainAgentService
    if name == "GlobalOrchestratorService":
        from src.distributed.orchestrator_service import GlobalOrchestratorService

        return GlobalOrchestratorService
    if name == "LocalAgentHttpClient":
        from src.distributed.orchestrator_service import LocalAgentHttpClient

        return LocalAgentHttpClient
    raise AttributeError(name)
