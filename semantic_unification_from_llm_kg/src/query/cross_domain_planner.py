from __future__ import annotations

import re
from dataclasses import dataclass

from src.distributed.contracts.models import (
    ArtifactType,
    DomainArtifactManifest,
    GlobalBatchManifest,
)
from src.query.cross_domain_models import (
    CrossDomainQueryPlan,
    QueryConstraint,
    QueryIntent,
    SemanticFieldCandidate,
    SubQueryPlan,
)
from src.query.join_path_resolver import JoinPathResolver


@dataclass(frozen=True)
class DomainSemanticContext:
    domain_id: str
    domain_unified: tuple[dict[str, object], ...]
    field_descriptions: tuple[dict[str, object], ...]


def load_domain_semantic_contexts(
    *,
    manifest: GlobalBatchManifest,
    ipfs_client,
    domain_ids: tuple[str, ...],
) -> dict[str, DomainSemanticContext]:
    domain_set = {item.strip() for item in domain_ids if item.strip()}
    output: dict[str, DomainSemanticContext] = {}
    for node_manifest in manifest.node_manifests:
        for domain_manifest in node_manifest.domain_manifests:
            domain_id = domain_manifest.domain_id
            if domain_set and domain_id not in domain_set:
                continue
            domain_unified_cid = _artifact_cid(domain_manifest, ArtifactType.DOMAIN_UNIFIED)
            descriptions_cid = _artifact_cid(domain_manifest, ArtifactType.FIELD_DESCRIPTIONS)
            if not domain_unified_cid and not descriptions_cid:
                continue

            unified_payload = ipfs_client.cat_json(domain_unified_cid) if domain_unified_cid else []
            desc_payload = ipfs_client.cat_json(descriptions_cid) if descriptions_cid else []
            output[domain_id] = DomainSemanticContext(
                domain_id=domain_id,
                domain_unified=tuple(_coerce_object_list(unified_payload)),
                field_descriptions=tuple(_coerce_description_records(desc_payload)),
            )
    return output


def resolve_semantic_candidates(
    *,
    intent: QueryIntent,
    contexts: dict[str, DomainSemanticContext],
    max_candidates_per_concept: int = 12,
) -> dict[str, list[SemanticFieldCandidate]]:
    concepts = _intent_concepts(intent)
    if not concepts:
        return {}

    grouped: dict[str, dict[tuple[str, str, str], SemanticFieldCandidate]] = {
        concept: {} for concept in concepts
    }

    for concept in concepts:
        concept_tokens = _tokens(concept)
        for context in contexts.values():
            for item in context.domain_unified:
                canonical_name = str(item.get("canonical_name", "")).strip().lower()
                description = str(item.get("description", "")).strip()
                fields = item.get("fields", [])
                if not isinstance(fields, list):
                    continue
                for raw_field in fields:
                    parsed = _parse_field_ref(raw_field, default_domain=context.domain_id)
                    if parsed is None:
                        continue
                    domain, table, field = parsed
                    if domain != context.domain_id:
                        continue
                    score, reason = _score_candidate(
                        concept=concept,
                        concept_tokens=concept_tokens,
                        canonical_name=canonical_name,
                        description=description,
                        field_name=field,
                        table_name=table,
                    )
                    if score <= 0:
                        continue
                    candidate = SemanticFieldCandidate(
                        concept=concept,
                        domain=domain,
                        table=table,
                        field=field,
                        canonical_name=canonical_name,
                        score=score,
                        reason=reason,
                    )
                    key = (domain, table, field)
                    previous = grouped[concept].get(key)
                    if previous is None or candidate.score > previous.score:
                        grouped[concept][key] = candidate

            if grouped[concept]:
                continue
            for desc in context.field_descriptions:
                table_name = str(desc.get("table", "")).strip()
                field_name = str(desc.get("field", "")).strip()
                description = str(desc.get("description", "")).strip()
                if not table_name or not field_name:
                    continue
                score, reason = _score_candidate(
                    concept=concept,
                    concept_tokens=concept_tokens,
                    canonical_name=field_name,
                    description=description,
                    field_name=field_name,
                    table_name=table_name,
                )
                if score <= 0:
                    continue
                candidate = SemanticFieldCandidate(
                    concept=concept,
                    domain=context.domain_id,
                    table=table_name,
                    field=field_name,
                    canonical_name=field_name.lower(),
                    score=score,
                    reason=f"{reason}; from field descriptions",
                )
                key = (context.domain_id, table_name, field_name)
                previous = grouped[concept].get(key)
                if previous is None or candidate.score > previous.score:
                    grouped[concept][key] = candidate

    result: dict[str, list[SemanticFieldCandidate]] = {}
    for concept, dedup in grouped.items():
        ranked = sorted(dedup.values(), key=lambda item: item.score, reverse=True)
        if ranked:
            result[concept] = ranked[:max(1, max_candidates_per_concept)]
    return result


def build_cross_domain_query_plan(
    *,
    query_text: str,
    intent: QueryIntent,
    candidate_map: dict[str, list[SemanticFieldCandidate]],
    target_domains: tuple[str, ...],
    source_domain: str = "",
    limit: int = 100,
) -> CrossDomainQueryPlan:
    domain_set = {item.strip() for item in target_domains if item.strip()}
    if not domain_set:
        domain_set = {candidate.domain for values in candidate_map.values() for candidate in values}

    table_map: dict[str, dict[str, dict[str, SemanticFieldCandidate]]] = {}
    for concept, candidates in candidate_map.items():
        for candidate in candidates:
            if domain_set and candidate.domain not in domain_set:
                continue
            domain_tables = table_map.setdefault(candidate.domain, {})
            table_concepts = domain_tables.setdefault(candidate.table, {})
            previous = table_concepts.get(concept)
            if previous is None or candidate.score > previous.score:
                table_concepts[concept] = candidate

    target_concepts = set(intent.target_attributes)
    if not target_concepts:
        target_concepts = set(candidate_map.keys())
    for agg in intent.aggregations:
        if agg.concept:
            target_concepts.add(agg.concept)

    constraints_by_concept: dict[str, list[QueryConstraint]] = {}
    for item in intent.constraints:
        constraints_by_concept.setdefault(item.concept, []).append(item)

    scored_tables: list[tuple[float, str, str, dict[str, SemanticFieldCandidate]]] = []
    for domain, tables in table_map.items():
        for table, concept_candidates in tables.items():
            table_score = _table_score(
                concept_candidates=concept_candidates,
                target_concepts=target_concepts,
                constraints_by_concept=constraints_by_concept,
            )
            if table_score <= 0:
                continue
            scored_tables.append((table_score, domain, table, concept_candidates))

    if not scored_tables:
        return CrossDomainQueryPlan(
            query_text=query_text,
            intent=intent,
            subqueries=(),
            execution_order=(),
            semantic_candidates=tuple(
                candidate
                for concept in sorted(candidate_map.keys())
                for candidate in candidate_map[concept]
            ),
            notes=("no executable table candidates were resolved",),
        )

    best_by_domain: dict[str, tuple[float, str, str, dict[str, SemanticFieldCandidate]]] = {}
    for item in sorted(scored_tables, key=lambda row: row[0], reverse=True):
        _, domain, _, _ = item
        if domain not in best_by_domain:
            best_by_domain[domain] = item

    sorted_domains = sorted(
        best_by_domain.values(),
        key=lambda row: _domain_priority(
            domain=row[1],
            concept_candidates=row[3],
            constraints_by_concept=constraints_by_concept,
            source_domain=source_domain,
        ),
        reverse=True,
    )

    base_subqueries: list[SubQueryPlan] = []
    for index, (_, domain, table, concept_candidates) in enumerate(sorted_domains, start=1):
        plan_id = f"sq_{index}_{domain}"
        direct_constraints: list[QueryConstraint] = []
        for concept in concept_candidates:
            direct_constraints.extend(constraints_by_concept.get(concept, []))
        dedup_constraints = _dedup_constraints(direct_constraints)

        dependency_concepts: list[str] = []
        if not dedup_constraints:
            for concept in sorted(concept_candidates.keys()):
                if _is_identifier_concept(concept):
                    dependency_concepts.append(concept)

        select_fields = _build_select_fields(
            concept_candidates=concept_candidates,
            target_concepts=target_concepts,
            constraints=dedup_constraints,
            dependency_concepts=tuple(dependency_concepts),
        )
        concept_field_map = tuple(
            (concept, candidate.field)
            for concept, candidate in sorted(concept_candidates.items())
        )
        base_subqueries.append(
            SubQueryPlan(
                plan_id=plan_id,
                domain=domain,
                table=table,
                select_fields=select_fields,
                constraints=tuple(dedup_constraints),
                concept_field_map=concept_field_map,
                dependency_concepts=tuple(dependency_concepts),
                depends_on=(),
                limit=max(1, min(limit, 500)),
            )
        )

    resolved_subqueries = JoinPathResolver().resolve(
        subqueries=tuple(base_subqueries),
        intent=intent,
        source_domain=source_domain,
    )

    semantic_candidates = tuple(
        candidate
        for concept in sorted(candidate_map.keys())
        for candidate in candidate_map[concept]
    )
    execution_order = tuple(item.plan_id for item in resolved_subqueries)
    notes: list[str] = []
    if source_domain:
        notes.append(f"source_domain={source_domain}")
    if intent.query_type in {"path_tracking", "state_tracking"}:
        notes.append("planner_mode=path_or_state_tracking")
    if intent.query_type == "aggregation":
        notes.append("planner_mode=aggregation")

    return CrossDomainQueryPlan(
        query_text=query_text,
        intent=intent,
        subqueries=resolved_subqueries,
        execution_order=execution_order,
        semantic_candidates=semantic_candidates,
        notes=tuple(notes),
    )


def _coerce_object_list(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def _coerce_description_records(payload: object) -> list[dict[str, object]]:
    records = payload.get("field_descriptions", []) if isinstance(payload, dict) else payload
    return _coerce_object_list(records)


def _artifact_cid(manifest: DomainArtifactManifest, artifact_type: ArtifactType) -> str:
    for artifact in manifest.artifacts:
        if artifact.artifact_type == artifact_type:
            return str(artifact.cid)
    return ""


def _intent_concepts(intent: QueryIntent) -> tuple[str, ...]:
    concepts: list[str] = []
    seen: set[str] = set()
    for item in intent.target_attributes:
        concept = item.strip().lower()
        if concept and concept not in seen:
            seen.add(concept)
            concepts.append(concept)
    for item in intent.constraints:
        concept = item.concept.strip().lower()
        if concept and concept not in seen:
            seen.add(concept)
            concepts.append(concept)
    for item in intent.aggregations:
        concept = item.concept.strip().lower()
        if concept and concept not in seen:
            seen.add(concept)
            concepts.append(concept)
    for item in intent.entities:
        concept = item.canonical_name.strip().lower() if item.canonical_name.strip() else item.text.strip().lower()
        if concept and concept not in seen:
            seen.add(concept)
            concepts.append(concept)
    return tuple(concepts)


def _tokens(text: str) -> set[str]:
    return {token for token in re.findall(r"[0-9A-Za-z_\u4e00-\u9fff]+", text.lower()) if token}


def _score_candidate(
    *,
    concept: str,
    concept_tokens: set[str],
    canonical_name: str,
    description: str,
    field_name: str,
    table_name: str,
) -> tuple[float, str]:
    canonical_tokens = _tokens(canonical_name)
    description_tokens = _tokens(description)
    field_tokens = _tokens(field_name)
    table_tokens = _tokens(table_name)

    score = 0.0
    matched_parts: list[str] = []
    if concept == canonical_name:
        score += 1.2
        matched_parts.append("exact canonical match")

    overlap_canonical = len(concept_tokens & canonical_tokens)
    if overlap_canonical > 0:
        score += overlap_canonical * 0.45
        matched_parts.append("canonical token overlap")

    overlap_description = len(concept_tokens & description_tokens)
    if overlap_description > 0:
        score += overlap_description * 0.25
        matched_parts.append("description token overlap")

    overlap_field = len(concept_tokens & field_tokens)
    if overlap_field > 0:
        score += overlap_field * 0.3
        matched_parts.append("field token overlap")

    overlap_table = len(concept_tokens & table_tokens)
    if overlap_table > 0:
        score += overlap_table * 0.15
        matched_parts.append("table token overlap")

    if _is_identifier_concept(concept) and _is_identifier_concept(field_name):
        score += 0.35
        matched_parts.append("identifier heuristic")

    if score <= 0:
        return 0.0, ""
    reason = ", ".join(matched_parts) if matched_parts else "semantic similarity"
    return round(score, 4), reason


def _parse_field_ref(raw: object, *, default_domain: str) -> tuple[str, str, str] | None:
    if not isinstance(raw, str):
        return None
    token = raw.strip()
    if not token:
        return None
    parts = [item.strip() for item in token.split(".")]
    if len(parts) == 3 and all(parts):
        return parts[0], parts[1], parts[2]
    if len(parts) == 2 and all(parts):
        return default_domain, parts[0], parts[1]
    return None


def _table_score(
    *,
    concept_candidates: dict[str, SemanticFieldCandidate],
    target_concepts: set[str],
    constraints_by_concept: dict[str, list[QueryConstraint]],
) -> float:
    score = 0.0
    for concept, candidate in concept_candidates.items():
        weight = 1.0
        if concept in target_concepts:
            weight += 0.35
        if concept in constraints_by_concept:
            weight += 0.45
        score += candidate.score * weight
    return score


def _domain_priority(
    *,
    domain: str,
    concept_candidates: dict[str, SemanticFieldCandidate],
    constraints_by_concept: dict[str, list[QueryConstraint]],
    source_domain: str,
) -> tuple[int, int, float]:
    source_bonus = 1 if source_domain and domain == source_domain else 0
    constraint_hits = sum(1 for concept in concept_candidates if concept in constraints_by_concept)
    score_sum = sum(item.score for item in concept_candidates.values())
    return (source_bonus, constraint_hits, score_sum)


def _dedup_constraints(items: list[QueryConstraint]) -> list[QueryConstraint]:
    dedup: dict[tuple[str, str, str], QueryConstraint] = {}
    for item in items:
        key = (item.concept, item.operator, str(item.value))
        if key not in dedup:
            dedup[key] = item
    return list(dedup.values())


def _build_select_fields(
    *,
    concept_candidates: dict[str, SemanticFieldCandidate],
    target_concepts: set[str],
    constraints: list[QueryConstraint],
    dependency_concepts: tuple[str, ...],
) -> tuple[str, ...]:
    selected: list[str] = []
    seen: set[str] = set()
    selected_concepts = {
        *target_concepts,
        *(item.concept for item in constraints),
        *dependency_concepts,
    }
    for concept, candidate in sorted(concept_candidates.items()):
        if selected_concepts and concept not in selected_concepts:
            continue
        if candidate.field in seen:
            continue
        seen.add(candidate.field)
        selected.append(candidate.field)
    if not selected:
        for candidate in sorted(concept_candidates.values(), key=lambda item: item.score, reverse=True):
            if candidate.field in seen:
                continue
            seen.add(candidate.field)
            selected.append(candidate.field)
            if len(selected) >= 6:
                break
    return tuple(selected[:12])


def _is_identifier_concept(value: str) -> bool:
    lowered = value.lower()
    hints = {
        "id",
        "identifier",
        "code",
        "no",
        "key",
        "uuid",
        "编号",
        "标识",
        "代码",
        "主键",
        "单号",
        "学号",
    }
    return any(hint in lowered for hint in hints)
