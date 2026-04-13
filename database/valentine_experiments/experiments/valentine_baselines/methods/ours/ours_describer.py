from __future__ import annotations

import re
from collections import Counter
from typing import Any

_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")
_DATE_PATTERN = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$")
_NUMBER_PATTERN = re.compile(r"^-?\d+(\.\d+)?$")
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

_TAG_KEYWORDS: dict[str, set[str]] = {
    "id": {"id", "identifier", "uid", "key", "code"},
    "name": {"name", "title", "fullname", "full", "nickname"},
    "time": {"date", "time", "year", "month", "day", "timestamp"},
    "money": {"price", "cost", "amount", "salary", "fee", "budget"},
    "location": {"city", "country", "state", "province", "address", "zip", "postal"},
    "contact": {"email", "mail", "phone", "mobile", "tel"},
    "category": {"type", "category", "genre", "class", "kind"},
    "text": {"description", "summary", "comment", "content", "text", "note"},
}


def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in _TOKEN_PATTERN.findall(text)]


def _infer_value_type(values: list[str]) -> str:
    if not values:
        return "unknown"

    total = len(values)
    number_hits = sum(1 for value in values if _NUMBER_PATTERN.fullmatch(value.strip()) is not None)
    date_hits = sum(1 for value in values if _DATE_PATTERN.fullmatch(value.strip()) is not None)
    email_hits = sum(1 for value in values if _EMAIL_PATTERN.fullmatch(value.strip()) is not None)

    if email_hits / total >= 0.6:
        return "email"
    if date_hits / total >= 0.6:
        return "date"
    if number_hits / total >= 0.8:
        return "numeric"
    if number_hits / total <= 0.2:
        return "text"
    return "mixed"


def _infer_tags(column_name: str, values: list[str]) -> list[str]:
    name_tokens = set(_tokenize(column_name))
    tags: list[str] = []
    for tag, candidates in _TAG_KEYWORDS.items():
        if name_tokens & candidates:
            tags.append(tag)

    if values:
        if any(_EMAIL_PATTERN.fullmatch(value.strip()) for value in values[:10]):
            tags.append("contact")
        if any(_DATE_PATTERN.fullmatch(value.strip()) for value in values[:10]):
            tags.append("time")
    unique_tags: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        if tag in seen:
            continue
        seen.add(tag)
        unique_tags.append(tag)
    return unique_tags


def _extract_keywords(values: list[str], *, max_keywords: int) -> list[str]:
    if not values or max_keywords <= 0:
        return []

    counter: Counter[str] = Counter()
    for value in values:
        for token in _tokenize(value):
            if len(token) <= 1:
                continue
            counter[token] += 1
    return [token for token, _ in counter.most_common(max_keywords)]


def describe_sampled_columns(
    sampled_columns: list[dict[str, Any]],
    *,
    max_keywords: int,
) -> list[dict[str, Any]]:
    descriptions: list[dict[str, Any]] = []
    for row in sampled_columns:
        table_name = str(row.get("table_name", ""))
        column_name = str(row.get("column_name", ""))
        values = [str(value) for value in row.get("sample_values", []) if str(value).strip()]
        value_type = _infer_value_type(values)
        tags = _infer_tags(column_name, values)
        keywords = _extract_keywords(values, max_keywords=max_keywords)
        sample_preview = " | ".join(values[:3])
        description = (
            f"column={column_name}; value_type={value_type}; "
            f"tags={','.join(tags) if tags else 'none'}; "
            f"preview={sample_preview if sample_preview else 'empty'}"
        )
        descriptions.append(
            {
                "table_name": table_name,
                "column_name": column_name,
                "value_type": value_type,
                "tags": tags,
                "keywords": keywords,
                "description": description,
            }
        )
    return descriptions

