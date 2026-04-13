from __future__ import annotations

from dataclasses import replace

from src.query.cross_domain_models import QueryConstraint, QueryIntent, SubQueryPlan


class JoinPathResolver:
    def resolve(
        self,
        *,
        subqueries: tuple[SubQueryPlan, ...],
        intent: QueryIntent,
        source_domain: str = "",
    ) -> tuple[SubQueryPlan, ...]:
        if not subqueries:
            return ()

        anchor_plan_id = self._choose_anchor_plan_id(subqueries=subqueries, source_domain=source_domain)
        plan_by_id = {item.plan_id: item for item in subqueries}
        concept_providers = self._build_initial_concept_providers(subqueries=subqueries)

        ordered_ids = self._topological_order_ids(
            subqueries=subqueries,
            anchor_plan_id=anchor_plan_id,
        )
        resolved: list[SubQueryPlan] = []
        for plan_id in ordered_ids:
            plan = plan_by_id[plan_id]
            direct_concepts = {item.concept for item in plan.constraints}
            dependency_concepts: list[str] = list(plan.dependency_concepts)
            depends_on: list[str] = list(plan.depends_on)

            for concept, _field in plan.concept_field_map:
                if concept in direct_concepts:
                    concept_providers.setdefault(concept, plan.plan_id)
                    continue
                # Only identifier-like concepts should propagate as join keys.
                if not _is_identifier_concept(concept):
                    continue
                provider_id = concept_providers.get(concept)
                if not provider_id or provider_id == plan.plan_id:
                    continue
                if concept not in dependency_concepts:
                    dependency_concepts.append(concept)
                if provider_id not in depends_on:
                    depends_on.append(provider_id)

            if not depends_on and plan.plan_id != anchor_plan_id and not plan.constraints:
                fallback = self._find_identifier_concept(plan)
                if fallback and anchor_plan_id:
                    dependency_concepts.append(fallback)
                    depends_on.append(anchor_plan_id)

            normalized_dependency_concepts = tuple(sorted({item for item in dependency_concepts if item}))
            normalized_depends_on = tuple(sorted({item for item in depends_on if item and item != plan.plan_id}))
            updated = replace(
                plan,
                dependency_concepts=normalized_dependency_concepts,
                depends_on=normalized_depends_on,
            )
            resolved.append(updated)

            for concept, _field in updated.concept_field_map:
                concept_providers.setdefault(concept, updated.plan_id)

        return tuple(resolved)

    def _choose_anchor_plan_id(
        self,
        *,
        subqueries: tuple[SubQueryPlan, ...],
        source_domain: str,
    ) -> str:
        if source_domain:
            for item in subqueries:
                if item.domain == source_domain:
                    return item.plan_id
        ranked = sorted(
            subqueries,
            key=lambda item: (
                len(item.constraints),
                len(item.concept_field_map),
            ),
            reverse=True,
        )
        return ranked[0].plan_id

    def _build_initial_concept_providers(
        self,
        *,
        subqueries: tuple[SubQueryPlan, ...],
    ) -> dict[str, str]:
        provider: dict[str, str] = {}
        for plan in subqueries:
            constrained_concepts = {item.concept for item in plan.constraints}
            for concept, _field in plan.concept_field_map:
                if concept in constrained_concepts:
                    provider.setdefault(concept, plan.plan_id)
        return provider

    def _topological_order_ids(
        self,
        *,
        subqueries: tuple[SubQueryPlan, ...],
        anchor_plan_id: str,
    ) -> list[str]:
        ids = [item.plan_id for item in subqueries]
        if anchor_plan_id in ids:
            ids.remove(anchor_plan_id)
            return [anchor_plan_id, *ids]
        return ids

    def _find_identifier_concept(self, plan: SubQueryPlan) -> str:
        for concept, _field in plan.concept_field_map:
            if _is_identifier_concept(concept):
                return concept
        return ""


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
    return any(token in lowered for token in hints)


def propagated_constraints_for_plan(
    *,
    base_constraints: tuple[QueryConstraint, ...],
    dependency_concepts: tuple[str, ...],
    propagated_values: dict[str, set[str]],
) -> tuple[QueryConstraint, ...]:
    merged: dict[tuple[str, str, str], QueryConstraint] = {}
    constrained_concepts = {item.concept for item in base_constraints}
    for item in base_constraints:
        key = (item.concept, item.operator, str(item.value))
        merged[key] = item

    for concept in dependency_concepts:
        if concept in constrained_concepts:
            continue
        values = propagated_values.get(concept, set())
        if not values:
            continue
        propagated = QueryConstraint(
            concept=concept,
            operator="in",
            value=tuple(sorted(values)),
            raw_text="propagated by join_path_resolver",
        )
        key = (propagated.concept, propagated.operator, str(propagated.value))
        merged[key] = propagated
    return tuple(merged.values())
