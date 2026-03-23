from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final, Literal

DatabaseType = Literal[
    "mysql",
    "postgresql",
    "oracle",
    "sqlite",
    "mongodb",
    "neo4j",
    "redis",
    "clickhouse",
    "tidb",
    "cassandra",
    "hbase",
]

FieldOrigin = Literal[
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
]

_DB_TYPE_ALIASES: Final[dict[str, DatabaseType]] = {
    "mysql": "mysql",
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "oracle": "oracle",
    "sqlite": "sqlite",
    "mongo": "mongodb",
    "mongodb": "mongodb",
    "neo4j": "neo4j",
    "redis": "redis",
    "clickhouse": "clickhouse",
    "tidb": "tidb",
    "cassandra": "cassandra",
    "hbase": "hbase",
}


def normalize_database_type(database_type: str) -> DatabaseType:
    normalized = database_type.strip().lower()
    if not normalized:
        raise ValueError("database type must not be empty")

    canonical = _DB_TYPE_ALIASES.get(normalized)
    if canonical is None:
        raise ValueError(f"unsupported database type: {database_type}")
    return canonical


@dataclass(frozen=True, slots=True)
class FieldUnit:
    source_name: str
    database_type: DatabaseType
    container_name: str
    field_path: str
    original_field: str
    field_origin: FieldOrigin
    logical_type: str = "unknown"
    samples: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        source_name = self.source_name.strip()
        if not source_name:
            raise ValueError("source_name must not be empty")

        container_name = self.container_name.strip()
        if not container_name:
            raise ValueError("container_name must not be empty")

        field_path = self.field_path.strip()
        if not field_path:
            raise ValueError("field_path must not be empty")

        original_field = self.original_field.strip()
        if not original_field:
            raise ValueError("original_field must not be empty")

        logical_type = self.logical_type.strip()
        if not logical_type:
            logical_type = "unknown"

        cleaned_samples: list[str] = []
        for sample in self.samples:
            if not isinstance(sample, str):
                raise ValueError("samples must contain only str values")
            normalized = sample.strip()
            if normalized:
                cleaned_samples.append(normalized)

        object.__setattr__(self, "source_name", source_name)
        object.__setattr__(self, "database_type", normalize_database_type(self.database_type))
        object.__setattr__(self, "container_name", container_name)
        object.__setattr__(self, "field_path", field_path)
        object.__setattr__(self, "original_field", original_field)
        object.__setattr__(self, "logical_type", logical_type)
        object.__setattr__(self, "samples", tuple(cleaned_samples))
