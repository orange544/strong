from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

QueryType = Literal[
    "entity_lookup",
    "path_tracking",
    "aggregation",
    "association_lookup",
    "state_tracking",
]
ConstraintOperator = Literal["eq", "neq", "gt", "gte", "lt", "lte", "like", "in"]
AggregationFunction = Literal["count", "sum", "avg", "min", "max"]


@dataclass(frozen=True)
class QueryEntity:
    text: str
    canonical_name: str = ""
    role: str = "primary"

    def to_dict(self) -> dict[str, object]:
        return {
            "text": self.text,
            "canonical_name": self.canonical_name,
            "role": self.role,
        }


@dataclass(frozen=True)
class QueryConstraint:
    concept: str
    operator: ConstraintOperator
    value: str | int | float | bool | tuple[str | int | float | bool, ...]
    raw_text: str = ""

    def to_dict(self) -> dict[str, object]:
        value: object = self.value
        if isinstance(value, tuple):
            value = list(value)
        return {
            "concept": self.concept,
            "operator": self.operator,
            "value": value,
            "raw_text": self.raw_text,
        }


@dataclass(frozen=True)
class QueryAggregation:
    function: AggregationFunction
    concept: str
    alias: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "function": self.function,
            "concept": self.concept,
            "alias": self.alias,
        }


@dataclass(frozen=True)
class QueryIntent:
    raw_query: str
    query_type: QueryType
    entities: tuple[QueryEntity, ...] = ()
    target_attributes: tuple[str, ...] = ()
    constraints: tuple[QueryConstraint, ...] = ()
    aggregations: tuple[QueryAggregation, ...] = ()
    domains_hint: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "raw_query": self.raw_query,
            "query_type": self.query_type,
            "entities": [item.to_dict() for item in self.entities],
            "target_attributes": list(self.target_attributes),
            "constraints": [item.to_dict() for item in self.constraints],
            "aggregations": [item.to_dict() for item in self.aggregations],
            "domains_hint": list(self.domains_hint),
        }


@dataclass(frozen=True)
class SemanticFieldCandidate:
    concept: str
    domain: str
    table: str
    field: str
    canonical_name: str
    score: float
    reason: str

    def full_field_ref(self) -> str:
        return f"{self.domain}.{self.table}.{self.field}"

    def to_dict(self) -> dict[str, object]:
        return {
            "concept": self.concept,
            "domain": self.domain,
            "table": self.table,
            "field": self.field,
            "canonical_name": self.canonical_name,
            "score": self.score,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class SubQueryPlan:
    plan_id: str
    domain: str
    table: str
    select_fields: tuple[str, ...]
    constraints: tuple[QueryConstraint, ...]
    concept_field_map: tuple[tuple[str, str], ...]
    dependency_concepts: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    limit: int = 100

    def to_dict(self) -> dict[str, object]:
        return {
            "plan_id": self.plan_id,
            "domain": self.domain,
            "table": self.table,
            "select_fields": list(self.select_fields),
            "constraints": [item.to_dict() for item in self.constraints],
            "concept_field_map": [
                {"concept": concept, "field": field}
                for concept, field in self.concept_field_map
            ],
            "dependency_concepts": list(self.dependency_concepts),
            "depends_on": list(self.depends_on),
            "limit": self.limit,
        }


@dataclass(frozen=True)
class CrossDomainQueryPlan:
    query_text: str
    intent: QueryIntent
    subqueries: tuple[SubQueryPlan, ...]
    execution_order: tuple[str, ...]
    semantic_candidates: tuple[SemanticFieldCandidate, ...] = ()
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "query_text": self.query_text,
            "intent": self.intent.to_dict(),
            "subqueries": [item.to_dict() for item in self.subqueries],
            "execution_order": list(self.execution_order),
            "semantic_candidates": [item.to_dict() for item in self.semantic_candidates],
            "notes": list(self.notes),
        }


@dataclass
class ExecutedSubQuery:
    plan: SubQueryPlan
    rows: list[dict[str, object]] = field(default_factory=list)
    error: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "plan": self.plan.to_dict(),
            "rows": self.rows,
            "error": self.error,
        }
