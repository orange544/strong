# BISHE Program Monorepo

本仓库是一个多子系统工程，围绕以下三条主线协同开发：

- `blockchain/go-norn-main`：Go 实现的区块链节点（PoVF）与 IPFS-Chain 桥接 CLI。
- `semantic_unification_from_llm_kg`：Python 语义统一流水线（采样、字段描述、语义统一、KG 生成、可选链上锚定）。
- `database/movie`：多数据库引擎的电影领域初始化脚本与样例数据。

---

## 1. 目录结构

```text
program/
├─ blockchain/
│  └─ go-norn-main/
│     ├─ cmd/              # norn, ipfs-chain 等入口
│     ├─ core/ p2p/ rpc/   # 链节点核心模块
│     ├─ pubsub/ crypto/ utils/
│     └─ scripts/          # IPFS 启停脚本
├─ semantic_unification_from_llm_kg/
│  ├─ src/                 # 配置、DB 访问、LLM、KG、pipeline
│  ├─ tests/               # unit/contracts/golden
│  ├─ scripts/             # baseline/demo 辅助脚本
│  └─ main.py              # 流水线入口
└─ database/
   └─ movie/
      ├─ <engine>/apply.ps1
      └─ setup_movie_databases.ps1
```

---

## 2. 环境要求

- 操作系统：Windows（PowerShell）
- Go：`1.20+`（由 `go-norn-main/go.mod` 约束）
- Python：`3.12.x`
- Python 包管理：`uv`
- 可选：
  - Docker Desktop（用于 `ipfs-up.ps1` 启动 IPFS）
  - 本地 `ipfs` 可执行（用于 `ipfs-native-up.ps1`）
  - 各数据库客户端（用于 `database/movie` 的可选引擎）

---

## 3. 快速开始

### 3.1 初始化电影数据库（建议先做）

```powershell
cd "D:\Program Files\BISHE\program\database\movie"
.\setup_movie_databases.ps1
```

可选参数示例：

```powershell
# 包含可选引擎（Oracle/ClickHouse/TiDB/Cassandra/HBase）
.\setup_movie_databases.ps1 -IncludeOptionalEngines

# 启用 Neo4j 初始化
.\setup_movie_databases.ps1 -RunNeo4j -Neo4jPassword "your_password"
```

### 3.2 运行 Python 采样模式（最低依赖路径）

```powershell
cd "D:\Program Files\BISHE\program\semantic_unification_from_llm_kg"
uv sync
python main.py --mode sample
```

采样模式默认不走链上锚定；如果你希望采样结果上传到 IPFS：

```powershell
python main.py --mode sample --upload-ipfs
```

### 3.3 运行全流程模式（需要 IPFS + 链服务）

`--mode all` 会执行语义统一、KG 生成，并调用 `ipfs-chain put` 做链上登记。运行前请确保：

- IPFS API 可达（默认 `http://127.0.0.1:5001`）
- go-norn RPC 可达（默认 `127.0.0.1:45558`）
- `ipfs-chain` 二进制可用（或可通过 `GO_NORN_ROOT` 自动构建）

参考命令：

```powershell
# 1) 构建链侧工具
cd "D:\Program Files\BISHE\program\blockchain\go-norn-main"
go build ./cmd/norn
go build -o ./bin/ipfs-chain ./cmd/ipfs-chain

# 2) 启动 IPFS（Docker 方式）
.\scripts\ipfs-up.ps1

# 3) 启动链节点（具体节点配置见 go-norn-main/README.md）
# 4) 回到语义流水线目录执行全流程
cd "D:\Program Files\BISHE\program\semantic_unification_from_llm_kg"
python main.py --mode all
```

---

## 4. 日常开发命令

### 4.1 Go（`blockchain/go-norn-main`）

```powershell
go build ./cmd/norn
go build -o ./bin/ipfs-chain ./cmd/ipfs-chain
go test ./...
```

### 4.2 Python（`semantic_unification_from_llm_kg`）

```powershell
uv sync
uv run ruff check src tests
uv run mypy --explicit-package-bases src tests
uv run pytest --cov=src --cov-fail-under=80 -q
```

### 4.3 Database（`database/movie`）

```powershell
.\setup_movie_databases.ps1
```

---

## 5. 配置说明（重点）

Python 子项目默认从 `semantic_unification_from_llm_kg/.env` 加载配置，常用项：

- 数据源：
  - `DB_SOURCES_JSON`（推荐）
  - `DB_PATHS_JSON` 或 `DB_PATH_<DOMAIN>`（兼容路径）
- LLM：
  - `LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL_NAME`
  - `LLM_DESC_*`, `LLM_UNIFY_*`
- IPFS/链：
  - `IPFS_API_URL`
  - `IPFS_CHAIN_BIN`
  - `GO_NORN_ROOT`
  - `CHAIN_RPC_ADDR`
  - `CHAIN_RECEIVER_ADDRESS`

注意：当前 `DatabasePluginRegistry` 默认内置 `sqlite` 插件。若要直接接入其他数据库驱动，需要在 `src/db/` 扩展并注册对应插件。

---

## 6. 常见边界场景与排查

1. `main.py --mode all` 失败，提示 `ipfs-chain` 不存在  
检查 `IPFS_CHAIN_BIN` 路径；或设置 `GO_NORN_ROOT` 让程序自动 `go build`。

2. Windows 路径带空格导致命令异常  
始终使用双引号包裹完整路径，例如：`cd "D:\Program Files\BISHE\program\..."`。

3. 采样/全流程报数据库驱动不支持  
确认 `DB_SOURCES_JSON` 中 `driver` 是否已在插件注册表内；未注册驱动会抛出 `Unsupported database driver`。

4. 数据库初始化脚本部分引擎失败  
`setup_movie_databases.ps1` 会汇总状态；可选引擎依赖本地客户端，缺失时会失败或跳过，优先保证核心引擎先通过。

---

## 7. 安全注意事项

- 严禁提交真实密钥、数据库密码、链私钥到仓库（尤其是 `.env`）。
- `database/movie` 中默认口令仅适用于本地实验环境，联调/生产前必须替换。
- LLM/IPFS/DB 返回的数据都应视为不可信输入，在集成边界做校验与错误处理。
- 避免提交运行产物（`outputs/`, 日志, 本地数据库文件, 二进制产物）。

---

## 8. 提交规范

建议遵循：

- 提交信息格式：`scope: imperative summary`
- 示例：`pipeline: harden runtime guards`
- 单次提交聚焦单一子系统，避免跨子系统混杂改动。
