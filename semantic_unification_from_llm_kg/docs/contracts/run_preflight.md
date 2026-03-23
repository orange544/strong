# `run.py` Real-Env Preflight

This document describes preflight checks executed by `src/pipeline/run.py` before sampling.

## What Is Checked

1. Driver support (`DB_SOURCES_JSON` driver is in adapter factory).
2. Python adapter dependencies for configured drivers.
3. Optional: SQLite file existence.
4. Optional: TCP connectivity to source host/port.

## Config

All keys are environment variables consumed by `src/configs/config.py`:

- `RUN_PREFLIGHT_ENABLED` (default: `true`)
- `RUN_PREFLIGHT_CHECK_SQLITE_PATH` (default: `true`)
- `RUN_PREFLIGHT_CHECK_TCP` (default: `false`)
- `RUN_PREFLIGHT_TCP_TIMEOUT_SEC` (default: `2.0`)

## Dependency Baseline

For multi-engine runtime, these Python packages are expected:

- `pymysql` (MySQL/TiDB)
- `psycopg[binary]` (PostgreSQL)
- `oracledb` (Oracle)
- `clickhouse-driver` (ClickHouse)
- `pymongo` (MongoDB)
- `neo4j` (Neo4j)
- `redis` (Redis)
- `cassandra-driver` (Cassandra)
- `happybase` (HBase)
