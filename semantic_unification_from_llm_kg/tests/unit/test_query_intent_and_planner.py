from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.query.cross_domain_models import (
    QueryConstraint,
    QueryEntity,
    QueryIntent,
    SemanticFieldCandidate,
)
from src.query.cross_domain_planner import build_cross_domain_query_plan
from src.query.intent_parser import parse_query_intent


def test_intent_parser_extracts_constraints_and_type() -> None:
    intent = parse_query_intent("query student_id = 20230001 and score > 90")
    assert intent.query_type == "entity_lookup"
    assert len(intent.constraints) >= 2
    concepts = {item.concept for item in intent.constraints}
    assert "student_id" in concepts
    assert "score" in concepts


def test_cross_domain_planner_builds_executable_subqueries() -> None:
    intent = QueryIntent(
        raw_query="query student_id = 20230001",
        query_type="entity_lookup",
        entities=(QueryEntity(text="student"),),
        target_attributes=("name", "grade"),
        constraints=(
            QueryConstraint(concept="student_id", operator="eq", value="20230001"),
        ),
    )
    candidate_map = {
        "student_id": [
            SemanticFieldCandidate(
                concept="student_id",
                domain="domain_a",
                table="students",
                field="student_id",
                canonical_name="student_id",
                score=1.2,
                reason="exact",
            ),
            SemanticFieldCandidate(
                concept="student_id",
                domain="domain_b",
                table="scores",
                field="student_code",
                canonical_name="student_id",
                score=1.0,
                reason="canonical",
            ),
        ],
        "name": [
            SemanticFieldCandidate(
                concept="name",
                domain="domain_a",
                table="students",
                field="name",
                canonical_name="name",
                score=0.9,
                reason="field overlap",
            )
        ],
        "grade": [
            SemanticFieldCandidate(
                concept="grade",
                domain="domain_b",
                table="scores",
                field="grade",
                canonical_name="grade",
                score=0.95,
                reason="field overlap",
            )
        ],
    }

    plan = build_cross_domain_query_plan(
        query_text=intent.raw_query,
        intent=intent,
        candidate_map=candidate_map,
        target_domains=("domain_a", "domain_b"),
        source_domain="domain_a",
        limit=20,
    )
    assert len(plan.subqueries) == 2
    assert plan.execution_order[0].endswith("domain_a")
    all_domains = {item.domain for item in plan.subqueries}
    assert all_domains == {"domain_a", "domain_b"}
