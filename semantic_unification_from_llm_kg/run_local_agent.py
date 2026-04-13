from __future__ import annotations

import argparse

from src.distributed.http_api import serve_local_agent
from src.distributed.local_agent_service import LocalDomainAgentService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Local Domain Agent HTTP service")
    parser.add_argument("--node-id", required=True, help="Unique local node identifier")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=18081, help="Bind port")
    parser.add_argument(
        "--access-token",
        default="",
        help="Optional token required in X-Agent-Token header",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    service = LocalDomainAgentService(node_id=args.node_id)
    serve_local_agent(
        service=service,
        host=args.host,
        port=args.port,
        access_token=args.access_token,
    )


if __name__ == "__main__":
    main()
