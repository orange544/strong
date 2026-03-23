from __future__ import annotations

import re
from typing import Any


def parse_query(query_text: str) -> dict[str, Any]:
    text = (query_text or "").strip()
    if not text:
        raise ValueError("query_text is empty")

    if text.lower().startswith("canonical:"):
        concept = text.split(":", 1)[1].strip()
        return {
            "query_type": "canonical_concept",
            "raw_query": text,
            "canonical_name": concept,
        }

    if re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", text):
        db_name, table_name, field_name = text.split(".")
        return {
            "query_type": "qualified_field",
            "raw_query": text,
            "db_name": db_name,
            "table_name": table_name,
            "field_name": field_name,
        }

    if re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+", text):
        table_name, field_name = text.split(".")
        return {
            "query_type": "table_field",
            "raw_query": text,
            "table_name": table_name,
            "field_name": field_name,
        }

    if re.fullmatch(r"[A-Za-z0-9_]+", text):
        return {
            "query_type": "field_keyword",
            "raw_query": text,
            "keyword": text,
        }

    return {
        "query_type": "natural_language",
        "raw_query": text,
        "keyword": text,
    }
