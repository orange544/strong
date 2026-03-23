from __future__ import annotations

from src.db.plugin_registry import DatabaseSource
from src.db.unified.base_adapter import BaseAdapter
from src.db.unified.field_unit import DatabaseType, normalize_database_type
from src.db.unified.non_relational_adapter import (
    CassandraAdapter,
    HBaseAdapter,
    MongoDBAdapter,
    Neo4jAdapter,
    RedisAdapter,
)
from src.db.unified.relational_adapter import (
    ClickHouseRelationalAdapter,
    MySQLTiDBRelationalAdapter,
    OracleRelationalAdapter,
    PostgreSQLRelationalAdapter,
    SQLiteRelationalAdapter,
)


class AdapterFactory:
    def __init__(self) -> None:
        self._registry: dict[DatabaseType, type[BaseAdapter]] = {}
        self.register("sqlite", SQLiteRelationalAdapter)
        self._register_relational_todo_adapters()
        self._register_non_relational_adapters()

    def _register_relational_todo_adapters(self) -> None:
        self.register("mysql", MySQLTiDBRelationalAdapter)
        self.register("tidb", MySQLTiDBRelationalAdapter)
        self.register("postgresql", PostgreSQLRelationalAdapter)
        self.register("oracle", OracleRelationalAdapter)
        self.register("clickhouse", ClickHouseRelationalAdapter)

    def _register_non_relational_adapters(self) -> None:
        self.register("mongodb", MongoDBAdapter)
        self.register("neo4j", Neo4jAdapter)
        self.register("redis", RedisAdapter)
        self.register("cassandra", CassandraAdapter)
        self.register("hbase", HBaseAdapter)

    def register(
        self,
        database_type: str,
        adapter_cls: type[BaseAdapter],
        *,
        replace: bool = False,
    ) -> None:
        normalized_database_type = normalize_database_type(database_type)
        if not issubclass(adapter_cls, BaseAdapter):
            raise TypeError("adapter_cls must inherit from BaseAdapter")

        if normalized_database_type in self._registry and not replace:
            raise ValueError(f"adapter '{normalized_database_type}' already registered")

        self._registry[normalized_database_type] = adapter_cls

    def create(self, source: DatabaseSource) -> BaseAdapter:
        database_type = normalize_database_type(source.driver)
        adapter_cls = self._registry.get(database_type)
        if adapter_cls is None:
            raise KeyError(f"database adapter not found: {database_type}")
        return adapter_cls(source)

    def supported_database_types(self) -> tuple[DatabaseType, ...]:
        return tuple(sorted(self._registry.keys()))
