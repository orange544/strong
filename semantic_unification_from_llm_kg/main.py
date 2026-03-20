from __future__ import annotations

import argparse
from pprint import pprint

from src.pipeline.run import run_pipeline
from src.pipeline.run_sampling import run_sampling_only


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Semantic unification pipeline entrypoint."
    )
    parser.add_argument(
        "--mode",
        choices=["sample", "all"],
        default="sample",
        help="sample: only database sampling; all: full pipeline",
    )
    parser.add_argument(
        "--upload-ipfs",
        action="store_true",
        help="Upload sampling output to IPFS in sample mode.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.mode == "sample":
        result = run_sampling_only(upload_to_ipfs=args.upload_ipfs)
        print("\nSampling completed:")
        pprint(result)
        return

    run_pipeline()


if __name__ == "__main__":
    main()
