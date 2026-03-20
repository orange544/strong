# BISHE Program

本目录已完成 `LLM-test/llm_kg` 与 `program/blockchain` 的集成，形成统一运行工程。

## 目录结构

```text
program/
├─ blockchain/                # 既有区块链系统（不改动）
├─ llm_kg/                    # LLM + KG + IPFS + 链上锚定主工程
│  ├─ data/dbs/*.db           # 数据域数据库
│  ├─ outputs/                # 运行输出
│  ├─ src/
│  │  ├─ configs/config.py    # 统一配置（DB/IPFS/LLM/CHAIN/Neo4j）
│  │  ├─ db/                  # 数据库抽样
│  │  ├─ llm/                 # 字段描述与语义统一
│  │  ├─ kg/                  # KG Cypher 生成
│  │  ├─ storage/             # IPFS + Blockchain Anchor
│  │  ├─ pipeline/run.py      # 主流程编排
│  │  └─ service/             # 查询等服务
│  └─ main.py                 # CLI 入口
└─ scripts/run_llmkg.ps1      # 一键运行脚本
```

## 快速运行

```powershell
cd "D:\Program Files\BISHE\program"
.\scripts\run_llmkg.ps1 -StopAfterDescriptions
```

全流程（默认不走链）：

```powershell
cd "D:\Program Files\BISHE\program"
.\scripts\run_llmkg.ps1
```

全流程 + 链上锚定：

```powershell
cd "D:\Program Files\BISHE\program"
.\scripts\run_llmkg.ps1 -WithChain -StartIpfs
```

查询脚本（行李追踪链路）：

```powershell
cd "D:\Program Files\BISHE\program\llm_kg"
python .\query_baggage.py --bag-tag TAG123456
python .\query_from_b.py --case-no CASE001
```

## 常改配置

- 数据库目录优先级：`llm_kg/data/movie` -> `llm_kg/data/dbs` -> `database/simulated/db-files` -> `llm_kg/data/raw/db-files`
- 配置文件：`llm_kg/src/configs/config.py`
- 关键项：
  - `DB_PATHS`
  - `LLM_CONFIG`
  - `CHAIN_CONFIG`
  - `NEO4J_CONFIG`
"# strong" 
