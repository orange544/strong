from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha1

from src.query.cross_domain_models import (
    CrossDomainQueryPlan,
    ExecutedSubQuery,
    QueryAggregation,
    QueryIntent,
)


def assemble_cross_domain_response(
    *,
    query_text: str,
    intent: QueryIntent,
    plan: CrossDomainQueryPlan,
    executed_subqueries: Iterable[ExecutedSubQuery],
    limit: int,
) -> dict[str, object]:
    executed = list(executed_subqueries)
    entity_groups: dict[str, dict[str, object]] = {}
    domains_involved: set[str] = set()
    partial_errors: list[dict[str, object]] = []
    concept_values: dict[str, list[object]] = {}

    for executed_item in executed:
        subquery = executed_item.plan
        domains_involved.add(subquery.domain)
        if executed_item.error:
            partial_errors.append(
                {
                    "plan_id": subquery.plan_id,
                    "domain": subquery.domain,
                    "table": subquery.table,
                    "error": executed_item.error,
                }
            )
            continue

        concept_field_map = dict(subquery.concept_field_map)
        for row in executed_item.rows:
            entity_id = _resolve_entity_id(
                row=row,
                concept_field_map=concept_field_map,
                fallback_scope=f"{subquery.domain}.{subquery.table}",
            )
            group = entity_groups.get(entity_id)
            if group is None:
                group = {
                    "entity_id": entity_id,
                    "entity_type": _entity_type(intent),
                    "attributes": {},
                    "related_records": [],
                    "provenance": [],
                }
                entity_groups[entity_id] = group

            attributes_obj = group["attributes"]
            if not isinstance(attributes_obj, dict):
                attributes_obj = {}
                group["attributes"] = attributes_obj

            for concept, field in concept_field_map.items():
                if field not in row:
                    continue
                value = row[field]
                _merge_attribute(attributes_obj, concept, value)
                concept_values.setdefault(concept, []).append(value)

            related_records_obj = group["related_records"]
            if not isinstance(related_records_obj, list):
                related_records_obj = []
                group["related_records"] = related_records_obj
            related_records_obj.append(
                {
                    "domain": subquery.domain,
                    "table": subquery.table,
                    "plan_id": subquery.plan_id,
                    "data": row,
                }
            )

            provenance_obj = group["provenance"]
            if not isinstance(provenance_obj, list):
                provenance_obj = []
                group["provenance"] = provenance_obj
            provenance_obj.append(
                {
                    "domain": subquery.domain,
                    "table": subquery.table,
                    "plan_id": subquery.plan_id,
                    "fields": list(subquery.select_fields),
                    "concept_field_map": [
                        {"concept": concept, "field": field}
                        for concept, field in subquery.concept_field_map
                    ],
                }
            )

    results = list(entity_groups.values())
    results.sort(
        key=lambda item: len(item.get("related_records", []))
        if isinstance(item.get("related_records"), list)
        else 0,
        reverse=True,
    )

    if intent.query_type in {"path_tracking", "state_tracking"}:
        for item in results:
            related = item.get("related_records", [])
            if not isinstance(related, list):
                continue
            item["path"] = _build_path_records(related)

    explanations = {
        "semantic_mapping": [item.to_dict() for item in plan.semantic_candidates],
        "query_plan": [item.to_dict() for item in plan.subqueries],
        "execution_order": list(plan.execution_order),
        "notes": list(plan.notes),
        "partial_errors": partial_errors,
    }
    explanations["markdown"] = render_cross_domain_explanations_markdown(
        query_text=query_text,
        intent=intent,
        plan=plan,
        executed=executed,
        partial_errors=partial_errors,
    )

    payload: dict[str, object] = {
        "query": query_text,
        "intent": intent.to_dict(),
        "domains_involved": sorted(domains_involved),
        "results": results[: max(1, limit)],
        "explanations": explanations,
    }

    if intent.query_type == "aggregation":
        payload["aggregation"] = _build_aggregation_result(
            intent=intent,
            concept_values=concept_values,
            total_entities=len(results),
        )

    return payload


def render_cross_domain_explanations_markdown(
    *,
    query_text: str,
    intent: QueryIntent,
    plan: CrossDomainQueryPlan,
    executed: list[ExecutedSubQuery],
    partial_errors: list[dict[str, object]],
) -> str:
    lines: list[str] = []
    lines.append("# Cross-Domain Query Explanation")
    lines.append("")
    lines.append(f"- Query: `{query_text}`")
    lines.append(f"- Query type: `{intent.query_type}`")

    if intent.target_attributes:
        targets = ", ".join(f"`{item}`" for item in intent.target_attributes)
        lines.append(f"- Target attributes: {targets}")
    if intent.constraints:
        constraints = ", ".join(
            f"`{item.concept} {item.operator} {item.value}`" for item in intent.constraints
        )
        lines.append(f"- Parsed constraints: {constraints}")
    if intent.aggregations:
        aggs = ", ".join(f"`{_aggregation_label(item)}`" for item in intent.aggregations)
        lines.append(f"- Aggregations: {aggs}")

    lines.append("")
    lines.append("## Semantic Mapping")
    if not plan.semantic_candidates:
        lines.append("- No semantic candidates were resolved.")
    else:
        for candidate in plan.semantic_candidates:
            lines.append(
                "- "
                f"`{candidate.concept}` -> `{candidate.domain}.{candidate.table}.{candidate.field}` "
                f"(score={candidate.score:.3f}, reason={candidate.reason})"
            )

    lines.append("")
    lines.append("## Query Plan")
    if not plan.subqueries:
        lines.append("- No executable subqueries were generated.")
    else:
        for subquery in plan.subqueries:
            dependency_text = ""
            if subquery.depends_on:
                dependency_text = f" depends_on={list(subquery.depends_on)}"
            lines.append(
                "- "
                f"`{subquery.plan_id}` @ `{subquery.domain}.{subquery.table}` "
                f"fields={list(subquery.select_fields)}"
                f" constraints={[_constraint_text(item) for item in subquery.constraints]}"
                f"{dependency_text}"
            )

    lines.append("")
    lines.append("## Execution Summary")
    lines.append(f"- Execution order: {list(plan.execution_order)}")
    for item in executed:
        if item.error:
            lines.append(f"- `{item.plan.plan_id}` failed: {item.error}")
        else:
            lines.append(f"- `{item.plan.plan_id}` rows={len(item.rows)}")

    if partial_errors:
        lines.append("")
        lines.append("## Partial Errors")
        for item in partial_errors:
            lines.append(
                "- "
                f"`{item.get('plan_id')}` at `{item.get('domain')}.{item.get('table')}`: "
                f"{item.get('error')}"
            )

    return "\n".join(lines)


def _resolve_entity_id(
    *,
    row: dict[str, object],
    concept_field_map: dict[str, str],
    fallback_scope: str,
) -> str:
    identifier_concepts = [concept for concept in concept_field_map if _is_identifier_concept(concept)]
    for concept in identifier_concepts:
        field = concept_field_map.get(concept, "")
        value = row.get(field)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text

    for field in concept_field_map.values():
        value = row.get(field)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            digest = sha1(f"{fallback_scope}:{field}:{text}".encode()).hexdigest()[:16]
            return f"entity_{digest}"

    digest = sha1(f"{fallback_scope}:{row}".encode()).hexdigest()[:16]
    return f"entity_{digest}"


def _merge_attribute(attributes: dict[str, object], key: str, value: object) -> None:
    if value is None:
        return
    if isinstance(value, str) and not value.strip():
        return
    if key not in attributes:
        attributes[key] = value
        return
    previous = attributes[key]
    if previous == value:
        return
    if isinstance(previous, list):
        if value not in previous:
            previous.append(value)
        return
    attributes[key] = [previous, value]


def _entity_type(intent: QueryIntent) -> str:
    if intent.entities:
        return intent.entities[0].text
    return "entity"


def _is_identifier_concept(value: str) -> bool:
    lowered = value.lower()
    hints = (
        "id",
        "identifier",
        "code",
        "no",
        "key",
        "uuid",
        "\u7f16\u53f7",
        "\u6807\u8bc6",
        "\u4ee3\u7801",
        "\u4e3b\u952e",
        "\u5355\u53f7",
        "\u5b66\u53f7",
    )
    return any(hint in lowered for hint in hints)


def _build_path_records(records: list[object]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        domain = item.get("domain")
        table = item.get("table")
        data = item.get("data")
        if not isinstance(domain, str):
            continue
        if not isinstance(table, str):
            continue
        if not isinstance(data, dict):
            continue
        normalized.append(
            {
                "domain": domain,
                "table": table,
                "time": _extract_time_value(data),
                "data": data,
            }
        )
    normalized.sort(key=lambda item: str(item.get("time", "")))
    return normalized


def _extract_time_value(data: dict[str, object]) -> str:
    time_hints = (
        "time",
        "timestamp",
        "date",
        "created_at",
        "updated_at",
        "event_time",
        "\u65f6\u95f4",
        "\u65e5\u671f",
    )
    for key, value in data.items():
        if not isinstance(key, str):
            continue
        lowered = key.lower()
        if any(hint in lowered for hint in time_hints):
            if value is None:
                continue
            return str(value)
        if any(hint in key for hint in ("\u65f6\u95f4", "\u65e5\u671f")):
            if value is None:
                continue
            return str(value)
    return ""


def _build_aggregation_result(
    *,
    intent: QueryIntent,
    concept_values: dict[str, list[object]],
    total_entities: int,
) -> dict[str, object]:
    if not intent.aggregations:
        return {"count_entities": total_entities}

    output: dict[str, object] = {}
    for agg in intent.aggregations:
        values = [item for item in concept_values.get(agg.concept, []) if item is not None]
        alias = agg.alias or _aggregation_label(agg)
        if agg.function == "count":
            output[alias] = len(values)
            continue

        numeric_values = _to_numeric_values(values)
        if not numeric_values:
            output[alias] = None
            continue
        if agg.function == "sum":
            output[alias] = sum(numeric_values)
        elif agg.function == "avg":
            output[alias] = sum(numeric_values) / len(numeric_values)
        elif agg.function == "min":
            output[alias] = min(numeric_values)
        elif agg.function == "max":
            output[alias] = max(numeric_values)
    return output


def _aggregation_label(agg: QueryAggregation) -> str:
    return f"{agg.function}_{agg.concept}"


def _constraint_text(item) -> str:
    return f"{item.concept} {item.operator} {item.value}"


def _to_numeric_values(values: list[object]) -> list[float]:
    output: list[float] = []
    for value in values:
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            output.append(float(value))
            continue
        if isinstance(value, float):
            output.append(value)
            continue
        if isinstance(value, str):
            text = value.strip()
            if not text:
                continue
            try:
                output.append(float(text))
            except ValueError:
                continue
    return output