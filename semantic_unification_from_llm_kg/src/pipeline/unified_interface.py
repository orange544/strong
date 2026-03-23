from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.db.plugin_registry import DatabaseSource
from src.db.unified.adapter_factory import AdapterFactory
from src.db.unified.field_unit import FieldUnit
from src.db.unified.unified_extractor import UnifiedExtractor

PIPELINE_TARGET_VERSION = "2026.03.v1"
FIELD_SAMPLE_SCHEMA_VERSION = "field-sample/1.0"

_ALLOWED_FIELD_ORIGINS = {
    "column",
    "document_key",
    "nested_key",
    "node_property",
    "relationship_property",
    "redis_hash_field",
    "redis_json_property",
    "redis_object_property",
    "hbase_family_qualifier",
    "unknown",
}


def extract_field_units_by_source(
    sources: Mapping[str, DatabaseSource],
    *,
    extractor: UnifiedExtractor | None = None,
    max_fields_per_domain: int = 0,
) -> dict[str, list[FieldUnit]]:
    active_extractor = extractor if extractor is not None else UnifiedExtractor()
    extracted: dict[str, list[FieldUnit]] = {}

    for source_name, source in sources.items():
        normalized_name = source_name.strip()
        if not normalized_name:
            raise RuntimeError("database source name must be a non-empty string")
        if not source.dsn.strip():
            raise RuntimeError(f"Database source '{normalized_name}' has an empty DSN")

        try:
            units = active_extractor.extract_from_source(source)
        except KeyError as exc:
            supported = ", ".join(AdapterFactory().supported_database_types())
            raise RuntimeError(
                f"Unsupported database driver '{source.driver}' for source '{normalized_name}'. "
                f"Supported drivers: {supported}"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Failed to extract field units for source '{normalized_name}': {exc}"
            ) from exc

        if max_fields_per_domain > 0:
            units = units[:max_fields_per_domain]
        extracted[normalized_name] = units

    return extracted


def field_unit_to_sample_record(unit: FieldUnit) -> dict[str, Any]:
    field_ref = f"{unit.source_name}.{unit.container_name}.{unit.field_path}"
    return {
        "schema_version": FIELD_SAMPLE_SCHEMA_VERSION,
        "pipeline_target_version": PIPELINE_TARGET_VERSION,
        "source_name": unit.source_name,
        "database_type": unit.database_type,
        "container_name": unit.container_name,
        "field_path": unit.field_path,
        "original_field": unit.original_field,
        "field_origin": unit.field_origin,
        "logical_type": unit.logical_type,
        "samples": list(unit.samples),
        # Compatibility fields for existing downstream logic.
        "db_name": unit.source_name,
        "table": unit.container_name,
        "field": unit.field_path,
        "type": unit.logical_type,
        "field_ref": field_ref,
    }


def field_units_to_sample_records(units: list[FieldUnit]) -> list[dict[str, Any]]:
    return [field_unit_to_sample_record(unit) for unit in units]


def sample_record_to_field_unit(sample: Mapping[str, Any]) -> FieldUnit:
    source_name = _coerce_non_empty_str(
        sample.get("source_name", sample.get("db_name", "")),
        context="sample.source_name",
    )
    database_type = _coerce_non_empty_str(
        sample.get("database_type", "sqlite"),
        context="sample.database_type",
    )
    container_name = _coerce_non_empty_str(
        sample.get("container_name", sample.get("table", "")),
        context="sample.container_name",
    )
    field_path = _coerce_non_empty_str(
        sample.get("field_path", sample.get("field", "")),
        context="sample.field_path",
    )

    original_field_raw = sample.get("original_field")
    original_field = (
        str(original_field_raw).strip()
        if isinstance(original_field_raw, str) and original_field_raw.strip()
        else field_path
    )

    field_origin_raw = sample.get("field_origin", "unknown")
    field_origin = str(field_origin_raw).strip() if isinstance(field_origin_raw, str) else "unknown"
    if field_origin not in _ALLOWED_FIELD_ORIGINS:
        field_origin = "unknown"

    logical_type_raw = sample.get("logical_type", sample.get("type", "unknown"))
    logical_type = (
        str(logical_type_raw).strip()
        if isinstance(logical_type_raw, str) and logical_type_raw.strip()
        else "unknown"
    )

    samples_obj = sample.get("samples", [])
    if not isinstance(samples_obj, list):
        raise RuntimeError("sample.samples must be a list")
    normalized_samples = _normalize_samples(samples_obj)

    return FieldUnit(
        source_name=source_name,
        database_type=database_type,
        container_name=container_name,
        field_path=field_path,
        original_field=original_field,
        field_origin=field_origin,  # type: ignore[arg-type]
        logical_type=logical_type,
        samples=tuple(normalized_samples),
    )


def build_db_data_from_field_units(
    domain_field_units: Mapping[str, list[FieldUnit]],
) -> dict[str, dict[str, list[str]]]:
    db_data: dict[str, dict[str, list[str]]] = {}
    for db_name, units in domain_field_units.items():
        containers: dict[str, list[str]] = {}
        seen: dict[str, set[str]] = {}
        for unit in units:
            container = unit.container_name
            field_name = unit.field_path
            if container not in containers:
                containers[container] = []
                seen[container] = set()
            if field_name in seen[container]:
                continue
            seen[container].add(field_name)
            containers[container].append(field_name)
        db_data[db_name] = containers
    return db_data


def _normalize_samples(samples: list[Any]) -> list[str]:
    normalized: list[str] = []
    for item in samples:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _coerce_non_empty_str(value: object, *, context: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"{context} must be a string")
    normalized = value.strip()
    if not normalized:
        raise RuntimeError(f"{context} must be a non-empty string")
    return normalized
