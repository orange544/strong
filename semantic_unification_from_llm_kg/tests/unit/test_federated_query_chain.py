from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.query.federated_query import federated_query
from src.query.query_parser import parse_query
from src.query.target_domain_resolver import resolve_matches_in_target_domains


class _FakeIPFS:
    def __init__(self, storage: dict[str, Any]) -> None:
        self._storage = storage

    def cat_json(self, cid: str) -> Any:
        return self._storage[cid]


def _fake_run_record() -> dict[str, Any]:
    return {
        "domains": [
            {
                "db_name": "IMDB",
                "field_descriptions_cid": "desc-imdb",
                "domain_unified_cid": "du-imdb",
                "sample_chain_key": "REGISTER_SAMPLE:IMDB",
                "samples_cid": "sample-imdb",
                "description_chain_key": "REGISTER_DESCRIPTION:IMDB",
                "domain_kg_chain_key": "REGISTER_DOMAIN_KG:IMDB",
            },
            {
                "db_name": "TMDB",
                "field_descriptions_cid": "desc-tmdb",
                "domain_unified_cid": "du-tmdb",
                "sample_chain_key": "REGISTER_SAMPLE:TMDB",
                "samples_cid": "sample-tmdb",
                "description_chain_key": "REGISTER_DESCRIPTION:TMDB",
                "domain_kg_chain_key": "REGISTER_DOMAIN_KG:TMDB",
            },
        ],
        "alignment_index_cid": "align-cid",
    }


def _fake_ipfs() -> _FakeIPFS:
    storage = {
        "desc-imdb": {
            "field_descriptions": [
                {"table": "movie", "field": "score", "description": "movie score"},
            ]
        },
        "desc-tmdb": {
            "field_descriptions": [
                {"table": "movie", "field": "vote_average", "description": "tmdb rating"},
            ]
        },
        "du-imdb": [
            {
                "canonical_name": "rating",
                "fields": ["IMDB.movie.score"],
                "description": "rating concept",
            }
        ],
        "du-tmdb": [
            {
                "canonical_name": "rating",
                "fields": ["TMDB.movie.vote_average"],
                "description": "rating concept",
            }
        ],
        "align-cid": [
            {
                "relation_type": "SAME_AS",
                "canonical_name": "rating",
                "score": 0.93,
                "source_domain": "IMDB",
                "source_table": "movie",
                "source_field": "score",
                "target_domain": "TMDB",
                "target_table": "movie",
                "target_field": "vote_average",
            }
        ],
    }
    return _FakeIPFS(storage)


def test_federated_query_canonical_concept_happy_path() -> None:
    result = federated_query(
        source_domain="IMDB",
        query_text="canonical:rating",
        run_record=_fake_run_record(),
        ipfs_client=_fake_ipfs(),
    )

    assert result["anchor"]["anchor_type"] == "canonical"
    assert result["anchor"]["canonical_name"] == "rating"
    assert result["matches"][0]["target_domain"] == "TMDB"
    assert result["resolved_results"][0]["target_field"] == "vote_average"
    assert result["resolved_results"][0]["field_description"]["field"] == "vote_average"


def test_federated_query_table_field_happy_path() -> None:
    result = federated_query(
        source_domain="IMDB",
        query_text="movie.score",
        run_record=_fake_run_record(),
        ipfs_client=_fake_ipfs(),
    )

    assert result["anchor"]["anchor_type"] == "field"
    assert result["anchor"]["matched_fields"] == ["IMDB.movie.score"]
    assert result["matches"][0]["canonical_name"] == "rating"


def test_federated_query_returns_not_found_when_anchor_misses() -> None:
    result = federated_query(
        source_domain="IMDB",
        query_text="movie.unknown_field",
        run_record=_fake_run_record(),
        ipfs_client=_fake_ipfs(),
    )

    assert result["anchor"]["anchor_type"] == "not_found"
    assert result["matches"] == []
    assert result["resolved_results"] == []


def test_compat_alias_modules_work() -> None:
    parsed = parse_query("TMDB.movie.vote_average")
    assert parsed["query_type"] == "qualified_field"

    resolved = resolve_matches_in_target_domains(
        matches=[
            {
                "target_domain": "TMDB",
                "target_table": "movie",
                "target_field": "vote_average",
                "canonical_name": "rating",
                "relation_type": "SAME_AS",
                "score": 0.91,
            }
        ],
        run_record=_fake_run_record(),
        ipfs_client=_fake_ipfs(),
    )
    assert len(resolved) == 1
    assert resolved[0]["target_domain"] == "TMDB"


def test_federated_query_rejects_empty_query() -> None:
    with pytest.raises(ValueError, match="query_text is empty"):
        federated_query(
            source_domain="IMDB",
            query_text="  ",
            run_record=_fake_run_record(),
            ipfs_client=_fake_ipfs(),
        )
