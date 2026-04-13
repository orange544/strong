from __future__ import annotations

import re

from src.query.cross_domain_models import (
    QueryAggregation,
    QueryConstraint,
    QueryEntity,
    QueryIntent,
    QueryType,
)

_CONCEPT_TOKEN = r"[0-9A-Za-z_\u4e00-\u9fff]{1,32}"
_SCALAR_TOKEN = r"[0-9A-Za-z_\-:\.\u4e00-\u9fff]{1,64}"

_STOP_ATTRIBUTE_TOKENS = {
    "information",
    "record",
    "records",
    "detail",
    "details",
    "data",
    "status",
    "\u4fe1\u606f",
    "\u8bb0\u5f55",
    "\u8be6\u60c5",
    "\u6570\u636e",
    "\u60c5\u51b5",
    "\u72b6\u6001",
    "\u8f68\u8ff9",
    "\u94fe\u8def",
}

_QUERY_TYPE_HINTS: list[tuple[set[str], QueryType]] = [
    (
        {
            "path",
            "trace",
            "route",
            "\u8def\u5f84",
            "\u8f68\u8ff9",
            "\u94fe\u8def",
            "\u6d41\u8f6c",
            "\u8ffd\u8e2a",
        },
        "path_tracking",
    ),
    (
        {
            "state",
            "status",
            "stage",
            "progress",
            "\u72b6\u6001",
            "\u9636\u6bb5",
            "\u8fdb\u5ea6",
        },
        "state_tracking",
    ),
    (
        {
            "aggregation",
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "\u7edf\u8ba1",
            "\u6570\u91cf",
            "\u603b\u6570",
            "\u6c42\u548c",
            "\u5e73\u5747",
            "\u6700\u5c0f",
            "\u6700\u5927",
        },
        "aggregation",
    ),
    (
        {
            "join",
            "related",
            "relation",
            "associate",
            "\u5173\u8054",
            "\u5173\u7cfb",
        },
        "association_lookup",
    ),
]

_AGGREGATION_PATTERNS: list[tuple[str, str]] = [
    (rf"\bcount\s*\(\s*(?P<concept>{_CONCEPT_TOKEN})\s*\)", "count"),
    (rf"\bsum\s*\(\s*(?P<concept>{_CONCEPT_TOKEN})\s*\)", "sum"),
    (rf"\bavg\s*\(\s*(?P<concept>{_CONCEPT_TOKEN})\s*\)", "avg"),
    (rf"\bmin\s*\(\s*(?P<concept>{_CONCEPT_TOKEN})\s*\)", "min"),
    (rf"\bmax\s*\(\s*(?P<concept>{_CONCEPT_TOKEN})\s*\)", "max"),
    (rf"\bcount\s+(?P<concept>{_CONCEPT_TOKEN})", "count"),
    (rf"\bsum\s+(?P<concept>{_CONCEPT_TOKEN})", "sum"),
    (rf"\bavg\s+(?P<concept>{_CONCEPT_TOKEN})", "avg"),
    (rf"\bmin\s+(?P<concept>{_CONCEPT_TOKEN})", "min"),
    (rf"\bmax\s+(?P<concept>{_CONCEPT_TOKEN})", "max"),
    (rf"(?P<concept>{_CONCEPT_TOKEN})\s*\u6570\u91cf", "count"),
    (rf"(?P<concept>{_CONCEPT_TOKEN})\s*\u603b\u6570", "count"),
    (rf"(?P<concept>{_CONCEPT_TOKEN})\s*\u6c42\u548c", "sum"),
    (rf"(?P<concept>{_CONCEPT_TOKEN})\s*\u5e73\u5747", "avg"),
    (rf"(?P<concept>{_CONCEPT_TOKEN})\s*\u6700\u5c0f", "min"),
    (rf"(?P<concept>{_CONCEPT_TOKEN})\s*\u6700\u5927", "max"),
]

_CONSTRAINT_PATTERNS: list[tuple[str, str]] = [
    (
        rf"(?P<concept>{_CONCEPT_TOKEN})\s*(?:>=|=>|at\s+least|is\s+at\s+least|\u5927\u4e8e\u7b49\u4e8e|"
        rf"\u4e0d\u5c11\u4e8e|\u4e0d\u5c0f\u4e8e)\s*(?P<value>[-]?\d+(?:\.\d+)?)",
        "gte",
    ),
    (
        rf"(?P<concept>{_CONCEPT_TOKEN})\s*(?:<=|=<|at\s+most|is\s+at\s+most|\u5c0f\u4e8e\u7b49\u4e8e|"
        rf"\u4e0d\u5927\u4e8e|\u81f3\u591a)\s*(?P<value>[-]?\d+(?:\.\d+)?)",
        "lte",
    ),
    (
        rf"(?P<concept>{_CONCEPT_TOKEN})\s*(?:>|greater\s+than|more\s+than|"
        rf"\u5927\u4e8e|\u9ad8\u4e8e)\s*(?P<value>[-]?\d+(?:\.\d+)?)",
        "gt",
    ),
    (
        rf"(?P<concept>{_CONCEPT_TOKEN})\s*(?:<|less\s+than|lower\s+than|"
        rf"\u5c0f\u4e8e|\u4f4e\u4e8e)\s*(?P<value>[-]?\d+(?:\.\d+)?)",
        "lt",
    ),
    (
        rf"(?P<concept>{_CONCEPT_TOKEN})\s*(?:!=|<>|is\s+not|\u4e0d\u7b49\u4e8e|"
        rf"\u4e0d\u662f)\s*(?P<value>{_SCALAR_TOKEN})",
        "neq",
    ),
    (
        rf"(?P<concept>{_CONCEPT_TOKEN})\s*(?:=|==|is|\u7b49\u4e8e|\u4e3a|\u662f)\s*"
        rf"(?P<value>{_SCALAR_TOKEN})",
        "eq",
    ),
    (
        rf"(?P<concept>{_CONCEPT_TOKEN})\s*(?:like|contains|\u5305\u542b)\s*"
        rf"(?P<value>{_SCALAR_TOKEN})",
        "like",
    ),
    (
        rf"(?P<concept>{_CONCEPT_TOKEN})\s*(?:in|\u5c5e\u4e8e)\s*\((?P<value>[^\)]{{1,256}})\)",
        "in",
    ),
]


def parse_query_intent(
    query_text: str,
    *,
    available_domains: tuple[str, ...] = (),
) -> QueryIntent:
    text = query_text.strip()
    if not text:
        raise RuntimeError("query_text must be non-empty")

    normalized_domains = tuple(item.strip() for item in available_domains if item.strip())
    query_type = _infer_query_type(text)
    constraints = _extract_constraints(text)
    aggregations = _extract_aggregations(text)
    target_attributes = _extract_target_attributes(text)
    entities = _extract_entities(text)
    domains_hint = _extract_domains_hint(text, normalized_domains)

    if query_type == "aggregation" and not target_attributes:
        target_attributes = [item.concept for item in aggregations]
    if query_type == "state_tracking" and not target_attributes:
        target_attributes = ["status", "state", "timestamp", "event_time"]

    return QueryIntent(
        raw_query=text,
        query_type=query_type,
        entities=tuple(entities),
        target_attributes=tuple(target_attributes),
        constraints=tuple(constraints),
        aggregations=tuple(aggregations),
        domains_hint=tuple(domains_hint),
    )


def _infer_query_type(query_text: str) -> QueryType:
    lowered = query_text.lower()
    for keywords, query_type in _QUERY_TYPE_HINTS:
        if any(keyword in lowered for keyword in keywords):
            return query_type
    return "entity_lookup"


def _extract_constraints(query_text: str) -> list[QueryConstraint]:
    seen: set[tuple[str, str, str]] = set()
    constraints: list[QueryConstraint] = []
    for pattern, operator in _CONSTRAINT_PATTERNS:
        for match in re.finditer(pattern, query_text, flags=re.IGNORECASE):
            concept = _normalize_concept(match.group("concept"))
            raw_value = match.group("value").strip()
            if not concept or not raw_value:
                continue

            value: str | int | float | bool | tuple[str | int | float | bool, ...]
            if operator == "in":
                parts = [item.strip() for item in re.split(r"[,，、\s]+", raw_value) if item.strip()]
                if not parts:
                    continue
                value = tuple(_parse_scalar_value(item) for item in parts)
            else:
                value = _parse_scalar_value(raw_value)

            dedup_key = (concept, operator, str(value))
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            constraints.append(
                QueryConstraint(
                    concept=concept,
                    operator=operator,  # type: ignore[arg-type]
                    value=value,
                    raw_text=match.group(0).strip(),
                )
            )
    return constraints


def _extract_aggregations(query_text: str) -> list[QueryAggregation]:
    aggregations: list[QueryAggregation] = []
    seen: set[tuple[str, str]] = set()
    for pattern, function in _AGGREGATION_PATTERNS:
        for match in re.finditer(pattern, query_text, flags=re.IGNORECASE):
            concept = _normalize_concept(match.group("concept"))
            if not concept:
                continue
            key = (function, concept)
            if key in seen:
                continue
            seen.add(key)
            aggregations.append(
                QueryAggregation(
                    function=function,  # type: ignore[arg-type]
                    concept=concept,
                    alias=f"{function}_{concept}",
                )
            )
    return aggregations


def _extract_target_attributes(query_text: str) -> list[str]:
    cleaned = query_text
    for phrase in (
        "\u8bf7\u67e5\u8be2",
        "\u8bf7\u95ee",
        "\u67e5\u8be2",
        "\u67e5\u627e",
        "\u83b7\u53d6",
        "\u68c0\u7d22",
        "query",
        "find",
        "get",
        "search",
    ):
        cleaned = cleaned.replace(phrase, " ")
    cleaned = cleaned.strip()

    segment = cleaned
    if "\u7684" in cleaned:
        segment = cleaned.split("\u7684", 1)[1]
    elif " of " in cleaned.lower():
        segment = cleaned.split(" of ", 1)[1]

    segment = re.split(
        r"(?:\bwhere\b|\u5176\u4e2d|\u6761\u4ef6\u4e3a)",
        segment,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    segment = re.split(r"[。！？!?\.]", segment, maxsplit=1)[0]
    if re.search(
        r"(?:=|>=|<=|>|<|\bwhere\b|\blike\b|\bin\b|\u7b49\u4e8e|\u4e3a|\u662f|\u5305\u542b|\u5c5e\u4e8e)",
        segment,
        flags=re.IGNORECASE,
    ):
        return []

    raw_parts = re.split(r"[，,；;和及与\s]+", segment)
    attributes: list[str] = []
    seen: set[str] = set()
    for raw_part in raw_parts:
        part = raw_part.strip()
        if not part:
            continue
        normalized = _normalize_concept(part)
        if not normalized:
            continue
        if normalized in _STOP_ATTRIBUTE_TOKENS:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        attributes.append(normalized)
    return attributes[:12]


def _extract_entities(query_text: str) -> list[QueryEntity]:
    entities: list[QueryEntity] = []
    seen: set[str] = set()
    patterns = [
        r"(?:query|find|get|search)\s*(?P<entity>[^,.!?;]{1,48})\s*(?:of|in|for|where|$)",
        (
            r"(?:\u67e5\u8be2|\u67e5\u627e|\u83b7\u53d6|\u68c0\u7d22)\s*"
            r"(?P<entity>[^\u7684\u5728\u4e2d\u91cc，。！？!?;,；]{1,48})"
            r"\s*(?:\u7684|\u5728|\u4e2d|\u91cc)?"
        ),
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, query_text, flags=re.IGNORECASE):
            raw_entity = match.group("entity").strip()
            if not raw_entity:
                continue
            cleaned = re.sub(
                rf"{_CONCEPT_TOKEN}\s*(?:=|==|!=|<>|is|is\s+not|\u7b49\u4e8e|\u4e3a|\u662f)\s*\S+",
                "",
                raw_entity,
                flags=re.IGNORECASE,
            )
            entity = _normalize_concept(cleaned)
            if not entity:
                continue
            if entity in seen:
                continue
            seen.add(entity)
            entities.append(QueryEntity(text=entity))
    return entities[:4]


def _extract_domains_hint(query_text: str, available_domains: tuple[str, ...]) -> list[str]:
    lowered = query_text.lower()
    return [domain for domain in available_domains if domain.lower() in lowered]


def _normalize_concept(text: str) -> str:
    cleaned = re.sub(r"\s+", "_", text.strip().lower())
    cleaned = re.sub(r"[^0-9a-zA-Z_\u4e00-\u9fff]", "", cleaned)
    return cleaned[:64]


def _parse_scalar_value(text: str) -> str | int | float | bool:
    lowered = text.lower().strip()
    if lowered in {"true", "yes"}:
        return True
    if lowered in {"false", "no"}:
        return False
    if re.fullmatch(r"[-]?\d+", lowered):
        try:
            return int(lowered)
        except ValueError:
            return lowered
    if re.fullmatch(r"[-]?\d+\.\d+", lowered):
        try:
            return float(lowered)
        except ValueError:
            return lowered
    return text.strip()
