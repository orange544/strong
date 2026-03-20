from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import src.pipeline.orchestration_common as common


def test_generate_descriptions_parallel_returns_empty_for_no_samples() -> None:
    class _Agent:
        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            return sample

    assert common.generate_descriptions_parallel(_Agent(), [], max_workers=1, domain_timeout_sec=1) == []


def test_generate_descriptions_parallel_timeout_marks_pending_and_keeps_done() -> None:
    class _Agent:
        def generate_description(self, sample: dict[str, Any]) -> dict[str, Any]:
            if sample["field"] == "slow":
                time.sleep(2)
            return {
                "table": sample["table"],
                "field": sample["field"],
                "description": "ok",
            }

    samples = [
        {"table": "movie", "field": "fast"},
        {"table": "movie", "field": "slow"},
    ]
    results = common.generate_descriptions_parallel(
        _Agent(),
        samples,
        max_workers=1,
        domain_timeout_sec=1,
    )

    assert any(item["field"] == "fast" and item["description"] == "ok" for item in results)
    assert any(
        item["field"] == "slow" and item["description"] == common.DESCRIPTION_FAILED
        for item in results
    )


def test_common_wrappers_safe_tag_and_attach_db_name() -> None:
    assert common.safe_db_tag("IMDB-2026!") == "IMDB_2026"
    assert common.safe_db_tag("!!!") == "domain"

    wrapped = common.wrap_single_table_fields_for_cross_domain(
        [
            {
                "db_name": "DB",
                "table": "movie",
                "field": "Title",
                "description": "title text",
            }
        ]
    )
    assert wrapped[0]["canonical_name"] == "title"
    assert wrapped[0]["fields"] == ["DB.movie.Title"]

    unified = [{"canonical_name": "movie_title"}]
    updated = common.attach_db_name_to_domain_unified(unified, "DB")
    assert updated[0]["db_name"] == "DB"
