# Movie Domain Databases (No Docker)

This directory contains movie-domain initialization scripts for the following database types:

- MySQL
- PostgreSQL
- Oracle
- SQLite
- MongoDB
- Neo4j
- Redis
- ClickHouse
- TiDB
- Cassandra
- HBase

## Directory Layout

- `mysql/movie_mysql.sql`
- `postgresql/movie_postgresql.sql`
- `oracle/movie_oracle.sql`
- `sqlite/movie_sqlite.sql`
- `mongodb/movie_mongodb.js`
- `neo4j/movie_neo4j.cypher`
- `redis/movie_redis_init.txt`
- `clickhouse/movie_clickhouse.sql`
- `tidb/movie_tidb.sql`
- `cassandra/movie_cassandra.cql`
- `hbase/movie_hbase.hql`

Each subdirectory also has an `apply.ps1` script.
`apply.ps1` for MySQL / PostgreSQL / MongoDB / Neo4j / Redis now executes schema + sample data in UTF-8 mode.

## One-Command Setup

Run from this directory:

```powershell
cd "D:\Program Files\BISHE\program\database\movie"
.\setup_movie_databases.ps1
```

Run Neo4j initialization when credentials are correct:

```powershell
.\setup_movie_databases.ps1 -RunNeo4j -Neo4jPassword "your_password"
```

Optional engines (Oracle / ClickHouse / TiDB / Cassandra / HBase):

```powershell
.\setup_movie_databases.ps1 -IncludeOptionalEngines
```

Run Oracle (requires connection string):

```powershell
.\setup_movie_databases.ps1 -IncludeOptionalEngines -OracleConnection "user/password@localhost:1521/XEPDB1"
```

## Credentials

Default local credentials in setup script:

- MySQL: `root / 123456`
- PostgreSQL: `postgres / 123456`
- Redis: `123456`
- Neo4j: `neo4j / 123456`

Override example:

```powershell
.\setup_movie_databases.ps1 -MySqlPassword "your_pwd" -PostgresPassword "your_pwd" -RedisPassword "your_pwd"
```

## Notes

- Oracle/ClickHouse/TiDB/Cassandra/HBase client tools are installed locally (see paths below).
- SQLite creates `sqlite/movie_sqlite.db`.
- Redis uses key structures (hash/set/list/zset) instead of relational tables.
- Sample data has been refreshed to focus on representative 2021-2025 classic movies.

## Client Paths (installed under D:\Program Files)

- Oracle SQL*Plus: `D:\Program Files\Oracle\instantclient_23_0\sqlplus.exe`
- ClickHouse client wrapper: `D:\Program Files\ClickHouse\Client\clickhouse-client.cmd`
- Cassandra cqlsh: `D:\Program Files\Apache\apache-cassandra-5.0.6\bin\cqlsh.cmd`
- HBase shell: `D:\Program Files\Apache\hbase-2.6.4\bin\hbase.cmd`
