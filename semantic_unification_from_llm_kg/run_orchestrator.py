from __future__ import annotations

import argparse
from pathlib import Path

from src.configs.config import DOMAIN_SHARE_DEFAULTS
from src.distributed.http_api import serve_orchestrator
from src.distributed.orchestrator_service import ChainWriteConfig, GlobalOrchestratorService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Global Orchestrator HTTP service")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=19081, help="Bind port")
    parser.add_argument(
        "--enable-chain-registration",
        action="store_true",
        help="Write global manifest CID to chain via ipfs-chain.",
    )
    parser.add_argument(
        "--chain-bin",
        default=str(DOMAIN_SHARE_DEFAULTS["ipfs_chain_bin"]),
        help="Path to ipfs-chain binary.",
    )
    parser.add_argument(
        "--chain-receiver",
        default=str(DOMAIN_SHARE_DEFAULTS["receiver"]),
        help="Receiver address used in chain writes.",
    )
    parser.add_argument(
        "--chain-rpc",
        default=str(DOMAIN_SHARE_DEFAULTS["rpc_addr"]),
        help="Blockchain RPC endpoint.",
    )
    parser.add_argument(
        "--chain-ipfs",
        default=str(DOMAIN_SHARE_DEFAULTS["ipfs_api"]),
        help="IPFS API endpoint used by ipfs-chain.",
    )
    parser.add_argument(
        "--chain-timeout",
        type=int,
        default=int(DOMAIN_SHARE_DEFAULTS["timeout_sec"]),
        help="Timeout seconds for ipfs-chain put.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    chain_config = ChainWriteConfig(
        ipfs_chain_bin=Path(args.chain_bin),
        receiver=args.chain_receiver,
        rpc_addr=args.chain_rpc,
        ipfs_api=args.chain_ipfs,
        timeout_sec=max(3, args.chain_timeout),
    )
    service = GlobalOrchestratorService(
        enable_chain_registration=args.enable_chain_registration,
        chain_write_config=chain_config,
    )
    serve_orchestrator(service=service, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
