from __future__ import annotations

from abc import ABC, abstractmethod

from src.db.plugin_registry import DatabaseSource
from src.db.unified.field_unit import DatabaseType, FieldUnit, normalize_database_type


class BaseAdapter(ABC):
    def __init__(self, source: DatabaseSource) -> None:
        source_name = source.name.strip()
        if not source_name:
            raise ValueError("source.name must not be empty")

        dsn = source.dsn.strip()
        if not dsn:
            raise ValueError("source.dsn must not be empty")

        self._source = source
        self._database_type: DatabaseType = normalize_database_type(source.driver)

    @property
    def source(self) -> DatabaseSource:
        return self._source

    @property
    def database_type(self) -> DatabaseType:
        return self._database_type

    @abstractmethod
    def extract_field_units(self) -> list[FieldUnit]:
        """Return normalized field units for upper-layer LLM consumption."""
