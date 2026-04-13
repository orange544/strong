param(
  [string]$ProjectRoot = "",
  [string]$NornRoot = "",
  [string]$MySqlHost = "127.0.0.1",
  [int]$MySqlPort = 3306,
  [string]$MySqlUser = "root",
  [string]$MySqlPassword = "123456",
  [string]$MySqlDatabase = "movie_mysql_db",
  [string]$PgHost = "127.0.0.1",
  [int]$PgPort = 5432,
  [string]$PgUser = "postgres",
  [string]$PgPassword = "123456",
  [string]$PgDatabase = "movie_postgresql_db",
  [switch]$RequireIpfsRunning,
  [switch]$RequireNornRunning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
  param([string]$Message)
  Write-Host ("== " + $Message + " ==") -ForegroundColor Cyan
}

function Write-Ok {
  param([string]$Message)
  Write-Host ("[OK] " + $Message) -ForegroundColor Green
}

function Write-WarnText {
  param([string]$Message)
  Write-Host ("[WARN] " + $Message) -ForegroundColor Yellow
}

function Write-ErrText {
  param([string]$Message)
  Write-Host ("[ERROR] " + $Message) -ForegroundColor Red
}

function Test-TcpPort {
  param(
    [string]$HostName,
    [int]$Port,
    [int]$TimeoutMs = 1500
  )
  try {
    $client = New-Object System.Net.Sockets.TcpClient
    $iar = $client.BeginConnect($HostName, $Port, $null, $null)
    $ok = $iar.AsyncWaitHandle.WaitOne($TimeoutMs, $false)
    if (-not $ok) {
      $client.Close()
      return $false
    }
    $client.EndConnect($iar) | Out-Null
    $client.Close()
    return $true
  } catch {
    return $false
  }
}

if (-not $ProjectRoot) {
  $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
}
if (-not $NornRoot) {
  $NornRoot = (Resolve-Path (Join-Path $ProjectRoot "..\blockchain\go-norn-main")).Path
}

Write-Step "Path Check"
Write-Host ("ProjectRoot: " + $ProjectRoot)
Write-Host ("NornRoot: " + $NornRoot)
if (-not (Test-Path (Join-Path $ProjectRoot "pyproject.toml"))) {
  throw "Invalid ProjectRoot: pyproject.toml not found"
}
if (-not (Test-Path (Join-Path $ProjectRoot ".env"))) {
  Write-WarnText ".env not found under project root"
}
if (-not (Test-Path $NornRoot)) {
  throw "NornRoot not found: $NornRoot"
}
Write-Ok "Paths look valid"

Write-Step "Tooling Check"
$uvCmd = Get-Command uv -ErrorAction SilentlyContinue
if (-not $uvCmd) {
  throw "uv command not found in PATH"
}
Write-Ok ("uv found: " + $uvCmd.Source)

$goCmd = Get-Command go -ErrorAction SilentlyContinue
if (-not $goCmd) {
  Write-WarnText "go command not found. Build steps will fail unless binaries already exist."
} else {
  Write-Ok ("go found: " + $goCmd.Source)
}

Write-Step "Python Env Check"
Push-Location $ProjectRoot
try {
  $pyResult = uv run python -c "import sys; print(sys.version)"
  Write-Ok ("python: " + ($pyResult | Out-String).Trim())
} finally {
  Pop-Location
}

Write-Step "LLM Config Check (.env / env)"
Push-Location $ProjectRoot
try {
  $llmJsonText = @'
import json
import requests
from src.configs.config import LLM_DESC_CONFIG, LLM_UNIFY_CONFIG

def check(name, cfg):
    base = str(cfg.get("base_url", "")).strip().rstrip("/")
    model = str(cfg.get("model_name", "")).strip()
    key = str(cfg.get("api_key", ""))
    has_key = bool(key)
    url = ""
    status = None
    error = ""
    if base:
        if base.endswith("/v1"):
            url = base + "/models"
        else:
            url = base + "/v1/models"
        try:
            r = requests.get(url, timeout=6)
            status = r.status_code
        except Exception as exc:
            error = str(exc)
    return {
        "name": name,
        "base_url": base,
        "model_name": model,
        "has_api_key": has_key,
        "models_url": url,
        "models_status": status,
        "models_error": error,
    }

result = {
    "desc": check("desc", LLM_DESC_CONFIG),
    "unify": check("unify", LLM_UNIFY_CONFIG),
}
print(json.dumps(result, ensure_ascii=False))
'@ | uv run python -
  $llmObj = $llmJsonText | ConvertFrom-Json
  $table = @(
    [pscustomobject]@{
      name = "desc"
      base_url = $llmObj.desc.base_url
      model = $llmObj.desc.model_name
      has_api_key = $llmObj.desc.has_api_key
      models_status = $llmObj.desc.models_status
      models_error = $llmObj.desc.models_error
    }
    [pscustomobject]@{
      name = "unify"
      base_url = $llmObj.unify.base_url
      model = $llmObj.unify.model_name
      has_api_key = $llmObj.unify.has_api_key
      models_status = $llmObj.unify.models_status
      models_error = $llmObj.unify.models_error
    }
  )
  $table | Format-Table -AutoSize

  if (-not $llmObj.desc.has_api_key -or -not $llmObj.unify.has_api_key) {
    Write-WarnText "LLM API key missing in desc/unify config"
  } else {
    Write-Ok "LLM API key configured for desc/unify"
  }
} finally {
  Pop-Location
}

Write-Step "MySQL Connectivity Check"
Push-Location $ProjectRoot
try {
  $mysqlCheck = @'
import pymysql
import sys
host, port, user, pwd, db = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
conn = pymysql.connect(host=host, port=port, user=user, password=pwd, database=db, connect_timeout=6)
with conn.cursor() as cur:
    cur.execute("SELECT 1")
    print(cur.fetchone()[0])
conn.close()
'@ | uv run python - $MySqlHost $MySqlPort $MySqlUser $MySqlPassword $MySqlDatabase
  if ((($mysqlCheck | Out-String).Trim()) -ne "1") {
    throw "mysql test query did not return 1"
  }
  Write-Ok "MySQL connected and query works"
} finally {
  Pop-Location
}

Write-Step "PostgreSQL Connectivity Check"
Push-Location $ProjectRoot
try {
  $pgCheck = @'
import psycopg
import sys
host, port, user, pwd, db = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
conn = psycopg.connect(host=host, port=port, user=user, password=pwd, dbname=db, connect_timeout=6)
with conn.cursor() as cur:
    cur.execute("SELECT 1")
    print(cur.fetchone()[0])
conn.close()
'@ | uv run python - $PgHost $PgPort $PgUser $PgPassword $PgDatabase
  if ((($pgCheck | Out-String).Trim()) -ne "1") {
    throw "postgresql test query did not return 1"
  }
  Write-Ok "PostgreSQL connected and query works"
} finally {
  Pop-Location
}

Write-Step "IPFS Check"
Push-Location $ProjectRoot
try {
  $ipfsVer = @'
import requests
r = requests.post("http://127.0.0.1:5001/api/v0/version", timeout=6)
r.raise_for_status()
print(r.text)
'@ | uv run python -
  Write-Ok "IPFS API reachable"
  Write-Host (($ipfsVer | Out-String).Trim())
} catch {
  if ($RequireIpfsRunning) {
    throw
  }
  Write-WarnText "IPFS is not reachable yet"
} finally {
  Pop-Location
}

Write-Step "go-norn Check"
$nornExe = Join-Path $NornRoot "cmd\norn\norn.exe"
$ipfsChainExe = Join-Path $NornRoot "bin\ipfs-chain.exe"
if (Test-Path $nornExe) {
  Write-Ok ("norn binary found: " + $nornExe)
} else {
  Write-WarnText ("norn binary missing: " + $nornExe)
}
if (Test-Path $ipfsChainExe) {
  Write-Ok ("ipfs-chain binary found: " + $ipfsChainExe)
} else {
  Write-WarnText ("ipfs-chain binary missing: " + $ipfsChainExe)
}

$nornPortOpen = Test-TcpPort -HostName "127.0.0.1" -Port 45558
if ($nornPortOpen) {
  Write-Ok "go-norn RPC port 45558 is listening"
} else {
  if ($RequireNornRunning) {
    throw "go-norn RPC 127.0.0.1:45558 is not listening"
  }
  Write-WarnText "go-norn RPC 127.0.0.1:45558 is not listening yet"
}

Write-Step "Preflight Summary"
Write-Ok "Preflight completed"
