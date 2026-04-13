# 高校A/B 固定样本建库脚本（gaoxiaonew）

本目录按照文档 `高校A_B数据库固定样本值_88条_含非关系型_最终版.md` 构建高校 A/B 数据库与固定样本。

## 目录结构

- `school_a/mysql`：A 校 MySQL
  - `university_base_db`
  - `faculty_hr_db`
  - `teaching_affairs_db`
  - `student_training_db`
  - `admission_employment_db`
- `school_a/postgresql`：A 校 PostgreSQL（`research_output_db`）
- `school_a/mongodb`：A 校 MongoDB（`university_document_db`）
- `school_a/neo4j`：A 校 Neo4j（`university_kg_db`）
- `school_a/redis`：A 校 Redis 键空间
- `school_a/elasticsearch`：A 校 Elasticsearch 索引（`shared_resource_index`）

- `school_b/mysql`：B 校 MySQL
  - `base_info_db`
  - `teaching_affairs_db`
  - `student_training_db`
  - `admission_employment_db`
- `school_b/postgresql`：B 校 PostgreSQL
  - `faculty_research_db`
  - `research_output_db`
- `school_b/mongodb`：B 校 MongoDB（`university_document_db`）
- `school_b/neo4j`：B 校 Neo4j（`university_kg_db`）
- `school_b/redis`：B 校 Redis 键空间
- `school_b/elasticsearch`：B 校 Elasticsearch 索引（`university_search_db`）

- `common/build_redis.py`：Redis UTF-8 文本命令加载器

## 一键执行

```powershell
cd "D:\Program Files\BISHE\program\database\gaoxiaonew"

# 仅建 A 校
.\setup_school_a.ps1

# 仅建 B 校
.\setup_school_b.ps1

# 全量（A+B）
.\setup_gaoxiaonew_all.ps1
```

## 默认连接参数

- MongoDB：`127.0.0.1:27017`
- MySQL：`127.0.0.1:3306`，`root/123456`
- PostgreSQL：`127.0.0.1:5432`，`postgres/123456`
- Neo4j：`bolt://127.0.0.1:17687`，`neo4j/12345678`
- Redis：`127.0.0.1:6379`，密码 `123456`，A/B 默认分别使用 `db1`/`db2`
- Elasticsearch：`http://127.0.0.1:9200`

## 说明

- 所有样本值均为固定值，不做随机生成。
- `null` 字段按数据库空值处理。
- Neo4j 按“先节点后关系”写入并支持幂等重跑。
- Redis 按文档 `key-value` 写入，`ttl=0` 通过不过期键实现。