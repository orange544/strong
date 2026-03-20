from __future__ import annotations

import re
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from concurrent.futures import (
    TimeoutError as FuturesTimeoutError,
)
from typing import Any, Protocol

DESCRIPTION_FAILED = "generation_failed"


class DescriptionGenerator(Protocol):
    def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
        ...


def safe_db_tag(db_name: str) -> str:
    tag = re.sub(r"[^0-9A-Za-z_]+", "_", db_name).strip("_")
    return tag or "domain"


def build_sample_artifact(
    db_name: str,
    timestamp: str,
    samples: list[dict[str, Any]],
) -> dict[str, Any]:
    sampled_field_count = len(samples)
    total_sample_value_count = sum(len(item.get("samples", [])) for item in samples)
    return {
        "summary": {
            "db_name": db_name,
            "timestamp": timestamp,
            "sampled_field_count": sampled_field_count,
            "total_sample_value_count": total_sample_value_count,
        },
        "samples": samples,
    }


def generate_descriptions_parallel(
    fd_agent: DescriptionGenerator,
    samples: list[dict[str, Any]],
    max_workers: int,
    domain_timeout_sec: int,
) -> list[dict[str, Any]]:
    if not samples:
        return []

    results: list[dict[str, Any]] = []
    executor = ThreadPoolExecutor(max_workers=max_workers)
    future_to_sample = {
        executor.submit(fd_agent.generate_description, sample): sample
        for sample in samples
    }

    done_count = 0
    try:
        try:
            for future in as_completed(future_to_sample, timeout=domain_timeout_sec):
                done_count += 1
                sample = future_to_sample[future]
                try:
                    result = future.result()
                    results.append(result)
                    print(f"[{done_count}/{len(samples)}] done: {sample['table']}.{sample['field']}")
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"[{done_count}/{len(samples)}] failed: "
                        f"{sample['table']}.{sample['field']} -> {exc}"
                    )
                    results.append(
                        {
                            "table": sample["table"],
                            "field": sample["field"],
                            "description": DESCRIPTION_FAILED,
                        }
                    )
        except FuturesTimeoutError:
            print(f"[timeout] description stage exceeded {domain_timeout_sec}s")
            for future, sample in future_to_sample.items():
                if future.done():
                    continue
                future.cancel()
                results.append(
                    {
                        "table": sample["table"],
                        "field": sample["field"],
                        "description": DESCRIPTION_FAILED,
                    }
                )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return results


def wrap_single_table_fields_for_cross_domain(
    field_desc_list: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    wrapped: list[dict[str, Any]] = []
    for item in field_desc_list:
        wrapped.append(
            {
                "db_name": item["db_name"],
                "canonical_name": item["field"].lower(),
                "fields": [f"{item['db_name']}.{item['table']}.{item['field']}"],
                "description": item["description"],
            }
        )
    return wrapped


def attach_db_name_to_domain_unified(
    domain_unified: list[dict[str, Any]],
    db_name: str,
) -> list[dict[str, Any]]:
    for item in domain_unified:
        item["db_name"] = db_name
    return domain_unified
