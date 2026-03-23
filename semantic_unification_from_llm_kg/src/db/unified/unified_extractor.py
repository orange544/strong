from __future__ import annotations

from collections.abc import Mapping

from src.db.plugin_registry import DatabaseSource
from src.db.unified.adapter_factory import AdapterFactory
from src.db.unified.field_unit import FieldUnit


class UnifiedExtractor:
    def __init__(self, adapter_factory: AdapterFactory | None = None) -> None:
        self._adapter_factory = adapter_factory if adapter_factory is not None else AdapterFactory()

    def extract_from_source(self, source: DatabaseSource) -> list[FieldUnit]:
        adapter = self._adapter_factory.create(source)
        return adapter.extract_field_units()

    def extract_from_sources(self, sources: Mapping[str, DatabaseSource]) -> dict[str, list[FieldUnit]]:
        extracted: dict[str, list[FieldUnit]] = {}

        for source_name, source in sources.items():
            normalized_source_name = source_name.strip()
            if not normalized_source_name:
                raise ValueError("source key must not be empty")

            source_object_name = source.name.strip()
            if source_object_name != normalized_source_name:
                raise ValueError(
                    "source key and DatabaseSource.name mismatch: "
                    f"'{normalized_source_name}' != '{source_object_name}'"
                )

            extracted[normalized_source_name] = self.extract_from_source(source)

        return extracted
