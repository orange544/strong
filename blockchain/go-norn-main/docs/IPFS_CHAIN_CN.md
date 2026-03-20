# go-norn + IPFS 链上链下存储集成

本文档说明如何在本地完成：
- 链下：把原始内容写入 IPFS，得到 CID。
- 链上：把 CID 作为 value 写入 go-norn 链上 KV（`receiver + key -> cid`）。
- 读取：从链上读 CID，再从 IPFS 取回原始内容。

## 1. 前置条件

- Go 1.20+
- Docker Desktop（用于运行 IPFS）
- 当前仓库：`go-norn-main`

## 2. 启动 IPFS

你可以任选一种方式：

### 方式 A：Docker（推荐）

在仓库根目录执行：

```powershell
.\scripts\ipfs-up.ps1
```

默认端口：
- IPFS API: `http://127.0.0.1:5001`
- IPFS Gateway: `http://127.0.0.1:8080`

停止 IPFS：

```powershell
.\scripts\ipfs-down.ps1
```

### 方式 B：本机原生 daemon（无 Docker）

如果你已经安装了 Kubo 或 IPFS Desktop：

```powershell
.\scripts\ipfs-native-up.ps1
```

停止：

```powershell
.\scripts\ipfs-native-down.ps1
```

## 3. 启动 go-norn 节点

示例（在 `cmd/norn` 下）：

```powershell
go build -o norn.exe .
.\norn.exe -d .\data -g -c .\config.yml --metrics
```

注意：
- `-g` 仅用于创世节点首次启动。
- 下面桥接命令默认连接 `127.0.0.1:45558`，请与 `cmd/norn/config.yml` 中 `rpc.address` 一致。

## 4. 构建桥接命令

在仓库根目录执行：

```powershell
go build -o .\bin\ipfs-chain.exe .\cmd\ipfs-chain
```

## 5. 写入流程（IPFS -> 链上）

### 5.1 文本写入

```powershell
.\bin\ipfs-chain.exe put `
  -receiver f5c5822480a49523033fca24eb35bb5b8238b70d `
  -key demo-text `
  -text "hello from ipfs and norn" `
  -rpc 127.0.0.1:45558 `
  -ipfs http://127.0.0.1:5001
```

### 5.2 文件写入

```powershell
.\bin\ipfs-chain.exe put `
  -receiver f5c5822480a49523033fca24eb35bb5b8238b70d `
  -key demo-file `
  -file .\README.md `
  -rpc 127.0.0.1:45558 `
  -ipfs http://127.0.0.1:5001
```

命令输出会包含：
- `CID`（链下数据地址）
- `TxHash`（把 CID 上链的交易哈希）

## 6. 读取流程（链上 -> IPFS）

### 6.1 读回到终端

```powershell
.\bin\ipfs-chain.exe get `
  -address f5c5822480a49523033fca24eb35bb5b8238b70d `
  -key demo-text `
  -rpc 127.0.0.1:45558 `
  -ipfs http://127.0.0.1:5001
```

### 6.2 读回到文件

```powershell
.\bin\ipfs-chain.exe get `
  -address f5c5822480a49523033fca24eb35bb5b8238b70d `
  -key demo-file `
  -out .\restored-readme.md `
  -rpc 127.0.0.1:45558 `
  -ipfs http://127.0.0.1:5001
```

## 7. 地址说明

- `receiver/address` 必须是 20 字节十六进制（40 个 hex 字符），可带或不带 `0x` 前缀。
- 示例里用了 `cmd/norn/config.yml` 的 `consensus.address`。

## 8. 交互关系

1. `put`：
   - 调用 IPFS `/api/v0/add` 上传内容，拿到 CID。
   - 调用链的 `SendTransactionWithData(type=set|append, receiver, key, value=cid)` 上链。
2. `get`：
   - 调用链的 `ReadContractAddress(address, key)` 得到 CID。
   - 调用 IPFS `/api/v0/cat?arg=<cid>` 取回真实内容。
