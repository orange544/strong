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
  [string]$DomainAId = "domain_a",
  [string]$DomainBId = "domain_b",
  [int]$AgentAPort = 18081,
  [int]$AgentBPort = 18082,
  [int]$OrchestratorPort = 19081,
  [string]$AgentAToken = "tokenA",
  [string]$AgentBToken = "tokenB",
  [string]$ChainReceiver = "f5c5822480a49523033fca24eb35bb5b8238b70d",
  [int]$MaxFieldsPerSource = 80,
  [int]$PollTimeoutSec = 1200,
  [switch]$InitDatabases,
  [switch]$KeepServices,
  [switch]$ReuseExistingServices,
  [switch]$SkipBuildChainBinaries,
  [switch]$SkipStartIpfs,
  [switch]$SkipStartNorn
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

function Test-TcpPort {
  param(
    [string]$HostName,
    [int]$Port,
    [int]$TimeoutMs = 1200
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

function Wait-TcpPort {
  param(
    [string]$HostName,
    [int]$Port,
    [int]$TimeoutSec,
    [string]$Name
  )
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    if (Test-TcpPort -HostName $HostName -Port $Port) {
      Write-Ok ($Name + " is ready on " + $HostName + ":" + $Port)
      return
    }
    Start-Sleep -Milliseconds 500
  }
  throw ($Name + " did not become ready on " + $HostName + ":" + $Port)
}

function Convert-ToSingleQuotedLiteral {
  param([string]$Text)
  return "'" + $Text.Replace("'", "''") + "'"
}

function Get-ListeningProcessIds {
  param([int]$Port)
  $ids = @()
  try {
    $rows = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue
  } catch {
    $rows = @()
  }
  foreach ($row in $rows) {
    $ownerProcessId = [int]$row.OwningProcess
    if ($ownerProcessId -gt 0 -and ($ids -notcontains $ownerProcessId)) {
      $ids += $ownerProcessId
    }
  }
  return ,$ids
}

function Assert-AtMostOneListener {
  param(
    [int]$Port,
    [string]$Name
  )
  $ids = Get-ListeningProcessIds -Port $Port
  if ($ids.Count -gt 1) {
    throw ($Name + " port " + $Port + " has multiple listeners (" + ($ids -join ",") + "). Stop stale services and rerun.")
  }
}

function Test-HttpEndpointReachable {
  param(
    [string]$Uri,
    [hashtable]$Headers,
    [int]$TimeoutSec = 3
  )
  try {
    Invoke-WebRequest -UseBasicParsing -Method Get -Uri $Uri -Headers $Headers -TimeoutSec $TimeoutSec | Out-Null
    return $true
  } catch {
    $resp = $_.Exception.Response
    if ($null -eq $resp) {
      return $false
    }
    try {
      $code = [int]$resp.StatusCode
    } catch {
      return $false
    }
    if ($code -ge 400 -and $code -le 499) {
      # 4xx still proves HTTP server is reachable; for readiness checks this is enough.
      return $true
    }
    return $false
  }
}

function Wait-HttpEndpoint {
  param(
    [string]$Uri,
    [hashtable]$Headers,
    [int]$TimeoutSec,
    [string]$Name
  )
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    if (Test-HttpEndpointReachable -Uri $Uri -Headers $Headers -TimeoutSec 3) {
      Write-Ok ($Name + " HTTP endpoint is reachable: " + $Uri)
      return
    }
    Start-Sleep -Milliseconds 500
  }
  throw ($Name + " HTTP endpoint did not become reachable: " + $Uri)
}

function Start-LoggedProcess {
  param(
    [string]$Name,
    [string]$FilePath,
    [string[]]$Arguments,
    [string]$WorkingDirectory,
    [string]$LogDirectory
  )
  $stdoutFile = Join-Path $LogDirectory ($Name + ".out.log")
  $stderrFile = Join-Path $LogDirectory ($Name + ".err.log")
  return Start-Process `
    -FilePath $FilePath `
    -ArgumentList $Arguments `
    -WorkingDirectory $WorkingDirectory `
    -RedirectStandardOutput $stdoutFile `
    -RedirectStandardError $stderrFile `
    -PassThru
}

function Get-IpfsJsonObject {
  param(
    [string]$ProjectRootPath,
    [string]$Cid
  )
  Push-Location $ProjectRootPath
  try {
    $jsonText = @'
import requests
import sys
import json
cid = sys.argv[1]
r = requests.post("http://127.0.0.1:5001/api/v0/cat", params={"arg": cid}, timeout=120)
r.raise_for_status()
obj = r.json()
print(json.dumps(obj, ensure_ascii=True))
'@ | uv run python - $Cid
    return ($jsonText | ConvertFrom-Json)
  } finally {
    Pop-Location
  }
}

function Save-Json {
  param(
    [object]$Value,
    [string]$FilePath,
    [int]$Depth = 30
  )
  $dir = Split-Path -Parent $FilePath
  if (-not (Test-Path $dir)) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
  }
  $Value | ConvertTo-Json -Depth $Depth | Set-Content -Path $FilePath -Encoding UTF8
}

function Write-StageProgress {
  param(
    [string]$ReportDirectory,
    [string]$Stage,
    [string]$Status,
    [string]$Message = "",
    [hashtable]$Meta = @{}
  )
  if ($null -eq $script:StageProgressTimeline) {
    $script:StageProgressTimeline = @()
  }
  $entry = [ordered]@{
    timestamp = (Get-Date).ToString("o")
    stage = $Stage
    status = $Status
    message = $Message
  }
  foreach ($key in ($Meta.Keys | Sort-Object)) {
    $entry[$key] = $Meta[$key]
  }
  $entryObj = [pscustomobject]$entry
  $script:StageProgressTimeline += $entryObj
  Save-Json -Value $script:StageProgressTimeline -FilePath (Join-Path $ReportDirectory "stage_progress_timeline.json")
  Save-Json -Value $entryObj -FilePath (Join-Path $ReportDirectory "stage_progress_latest.json")
}

function Find-DomainArtifactRecord {
  param(
    [object[]]$DomainArtifacts,
    [string]$DomainId,
    [string]$ArtifactType
  )
  return ($DomainArtifacts | Where-Object {
      $_.domain -eq $DomainId -and $_.type -eq $ArtifactType
    } | Select-Object -First 1)
}

function Export-DomainStageArtifacts {
  param(
    [string]$ProjectRootPath,
    [string]$ReportDirectory,
    [object[]]$DomainArtifacts,
    [string[]]$DomainIds,
    [string]$ArtifactType,
    [string]$StageName,
    [string]$FileSuffix
  )
  Write-Step ("Export Stage: " + $StageName)
  Write-StageProgress `
    -ReportDirectory $ReportDirectory `
    -Stage $StageName `
    -Status "running" `
    -Message ("exporting artifact type " + $ArtifactType)

  $items = @()
  foreach ($domainId in $DomainIds) {
    $record = Find-DomainArtifactRecord `
      -DomainArtifacts $DomainArtifacts `
      -DomainId $domainId `
      -ArtifactType $ArtifactType
    $cid = ""
    $recordCount = 0
    if ($record -ne $null) {
      $cid = [string]$record.cid
      if ($record.record_count -is [int]) {
        $recordCount = [int]$record.record_count
      }
    }

    $outputFile = ""
    $itemStatus = "skipped"
    $errorMessage = ""
    if ($cid) {
      try {
        $payload = Get-IpfsJsonObject -ProjectRootPath $ProjectRootPath -Cid $cid
        $outputFile = Join-Path $ReportDirectory ($domainId + $FileSuffix)
        Save-Json -Value $payload -FilePath $outputFile
        $itemStatus = "exported"
      } catch {
        $itemStatus = "failed"
        $errorMessage = $_.Exception.Message
      }
    } else {
      $errorMessage = "artifact cid missing"
    }

    $items += [pscustomobject]@{
      domain = $domainId
      artifact_type = $ArtifactType
      cid = $cid
      record_count = $recordCount
      output_file = $outputFile
      status = $itemStatus
      error_message = $errorMessage
    }
  }

  $failed = @($items | Where-Object { $_.status -eq "failed" })
  $summary = [pscustomobject]@{
    stage = $StageName
    artifact_type = $ArtifactType
    created_at = (Get-Date).ToString("o")
    domain_count = $DomainIds.Count
    exported_count = @($items | Where-Object { $_.status -eq "exported" }).Count
    skipped_count = @($items | Where-Object { $_.status -eq "skipped" }).Count
    failed_count = $failed.Count
    items = $items
  }
  Save-Json -Value $summary -FilePath (Join-Path $ReportDirectory ("stage_" + $StageName + "_summary.json"))

  if ($failed.Count -gt 0) {
    $failedDomains = @($failed | Select-Object -ExpandProperty domain)
    Write-StageProgress `
      -ReportDirectory $ReportDirectory `
      -Stage $StageName `
      -Status "failed" `
      -Message ("failed domains: " + ($failedDomains -join ", ")) `
      -Meta @{
        artifact_type = $ArtifactType
        failed_count = $failed.Count
      }
    throw ("stage '" + $StageName + "' has failed domains: " + ($failedDomains -join ", "))
  }

  Write-StageProgress `
    -ReportDirectory $ReportDirectory `
    -Stage $StageName `
    -Status "completed" `
    -Message ("exported " + $summary.exported_count + " artifacts")
  return $summary
}

if (-not $ProjectRoot) {
  $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
}
if (-not $NornRoot) {
  $NornRoot = (Resolve-Path (Join-Path $ProjectRoot "..\blockchain\go-norn-main")).Path
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$runId = "demo_run_" + $timestamp
$reportDir = Join-Path $ProjectRoot ("tmp\report\" + $runId)
$logDir = Join-Path $reportDir "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$started = @{
  norn = $null
  agentA = $null
  agentB = $null
  orch = $null
}

try {
  Write-Step "Run Context"
  Write-Host ("ProjectRoot: " + $ProjectRoot)
  Write-Host ("NornRoot: " + $NornRoot)
  Write-Host ("ReportDir: " + $reportDir)
  Write-StageProgress `
    -ReportDirectory $reportDir `
    -Stage "run_context" `
    -Status "completed" `
    -Message "context initialized" `
    -Meta @{
      run_id = $runId
      report_dir = $reportDir
    }

  Write-Step "Preflight"
  Write-StageProgress -ReportDirectory $reportDir -Stage "preflight" -Status "running" -Message "starting dependency checks"
  & (Join-Path $PSScriptRoot "demo_preflight.ps1") `
    -ProjectRoot $ProjectRoot `
    -NornRoot $NornRoot `
    -MySqlHost $MySqlHost `
    -MySqlPort $MySqlPort `
    -MySqlUser $MySqlUser `
    -MySqlPassword $MySqlPassword `
    -MySqlDatabase $MySqlDatabase `
    -PgHost $PgHost `
    -PgPort $PgPort `
    -PgUser $PgUser `
    -PgPassword $PgPassword `
    -PgDatabase $PgDatabase
  Write-StageProgress -ReportDirectory $reportDir -Stage "preflight" -Status "completed" -Message "dependency checks passed"

  $pythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
  if (-not (Test-Path $pythonExe)) {
    throw "python runtime not found in project venv: $pythonExe"
  }

  if ($InitDatabases) {
    Write-Step "Initialize Database Sample Schemas"
    & (Join-Path $ProjectRoot "..\database\movie\mysql\apply.ps1") `
      -DbHost $MySqlHost -Port $MySqlPort -User $MySqlUser -Password $MySqlPassword
    & (Join-Path $ProjectRoot "..\database\movie\postgresql\apply.ps1") `
      -DbHost $PgHost -Port $PgPort -User $PgUser -Password $PgPassword -Database $PgDatabase
    Write-Ok "Database sample schemas initialized"
  }

  if (-not $SkipBuildChainBinaries) {
    Write-Step "Build go-norn Binaries"
    Push-Location $NornRoot
    try {
      go build -o ".\cmd\norn\norn.exe" ".\cmd\norn"
      go build -o ".\bin\ipfs-chain.exe" ".\cmd\ipfs-chain"
      Write-Ok "Built norn and ipfs-chain binaries"
    } finally {
      Pop-Location
    }
  }

  if (-not $SkipStartIpfs) {
    Write-Step "Ensure IPFS Running"
    if (Test-TcpPort -HostName "127.0.0.1" -Port 5001) {
      Write-Ok "IPFS API already listening on 127.0.0.1:5001"
    } else {
      & (Join-Path $NornRoot "scripts\ipfs-up.ps1")
      Wait-TcpPort -HostName "127.0.0.1" -Port 5001 -TimeoutSec 40 -Name "IPFS API"
    }
  } else {
    Write-WarnText "SkipStartIpfs enabled"
  }

  if (-not $SkipStartNorn) {
    Write-Step "Ensure go-norn Running"
    if (Test-TcpPort -HostName "127.0.0.1" -Port 45558) {
      Write-Ok "go-norn RPC already listening on 127.0.0.1:45558"
    } else {
      $nornExe = Join-Path $NornRoot "cmd\norn\norn.exe"
      $nornCfg = Join-Path $NornRoot "cmd\norn\config.yml"
      $nornData = Join-Path $NornRoot "demo_data"
      if (-not (Test-Path $nornExe)) {
        throw "norn binary not found: $nornExe"
      }
      if (-not (Test-Path $nornCfg)) {
        throw "norn config not found: $nornCfg"
      }

      $needGenesis = -not (Test-Path $nornData)
      if ($needGenesis) {
        New-Item -ItemType Directory -Force -Path $nornData | Out-Null
      }
      $args = @("-d", $nornData, "-c", $nornCfg, "--metrics")
      if ($needGenesis) {
        $args = @("-d", $nornData, "-g", "-c", $nornCfg, "--metrics")
      }
      $started.norn = Start-LoggedProcess `
        -Name "norn" `
        -FilePath $nornExe `
        -Arguments $args `
        -WorkingDirectory $NornRoot `
        -LogDirectory $logDir
      Wait-TcpPort -HostName "127.0.0.1" -Port 45558 -TimeoutSec 40 -Name "go-norn RPC"
    }
  } else {
    Write-WarnText "SkipStartNorn enabled"
  }

  Write-Step "Start Local Agents + Orchestrator"
  Assert-AtMostOneListener -Port $AgentAPort -Name "Local Agent A"
  Assert-AtMostOneListener -Port $AgentBPort -Name "Local Agent B"
  Assert-AtMostOneListener -Port $OrchestratorPort -Name "Orchestrator"

  if ((Test-TcpPort -HostName "127.0.0.1" -Port $AgentAPort) -and (-not $ReuseExistingServices)) {
    throw "Port $AgentAPort is already in use. Use -ReuseExistingServices if intentional."
  }
  if ((Test-TcpPort -HostName "127.0.0.1" -Port $AgentBPort) -and (-not $ReuseExistingServices)) {
    throw "Port $AgentBPort is already in use. Use -ReuseExistingServices if intentional."
  }
  if ((Test-TcpPort -HostName "127.0.0.1" -Port $OrchestratorPort) -and (-not $ReuseExistingServices)) {
    throw "Port $OrchestratorPort is already in use. Use -ReuseExistingServices if intentional."
  }

  if (-not (Test-TcpPort -HostName "127.0.0.1" -Port $AgentAPort)) {
    $dsnA = "mysql://$MySqlUser`:$MySqlPassword@$MySqlHost`:$MySqlPort/$MySqlDatabase"
    $jsonA = '{"' + $DomainAId + '":{"driver":"mysql","dsn":"' + $dsnA + '"}}'
    $prevDbSources = $env:DB_SOURCES_JSON
    try {
      $env:DB_SOURCES_JSON = $jsonA
      $started.agentA = Start-LoggedProcess `
        -Name "local_agent_a" `
        -FilePath $pythonExe `
        -Arguments @(
          "run_local_agent.py",
          "--node-id",
          "node_a",
          "--host",
          "127.0.0.1",
          "--port",
          "$AgentAPort",
          "--access-token",
          "$AgentAToken"
        ) `
        -WorkingDirectory $ProjectRoot `
        -LogDirectory $logDir
    } finally {
      if ($null -eq $prevDbSources) {
        Remove-Item Env:DB_SOURCES_JSON -ErrorAction SilentlyContinue
      } else {
        $env:DB_SOURCES_JSON = $prevDbSources
      }
    }
    Wait-TcpPort -HostName "127.0.0.1" -Port $AgentAPort -TimeoutSec 30 -Name "Local Agent A"
    Wait-HttpEndpoint `
      -Uri ("http://127.0.0.1:" + $AgentAPort + "/v1/jobs/ping_status") `
      -Headers @{ "X-Agent-Token" = $AgentAToken } `
      -TimeoutSec 30 `
      -Name "Local Agent A"
  } else {
    Write-Ok "Reusing existing Local Agent A"
    Wait-HttpEndpoint `
      -Uri ("http://127.0.0.1:" + $AgentAPort + "/v1/jobs/ping_status") `
      -Headers @{ "X-Agent-Token" = $AgentAToken } `
      -TimeoutSec 15 `
      -Name "Local Agent A"
  }

  if (-not (Test-TcpPort -HostName "127.0.0.1" -Port $AgentBPort)) {
    $dsnB = "postgresql://$PgUser`:$PgPassword@$PgHost`:$PgPort/$PgDatabase"
    $jsonB = '{"' + $DomainBId + '":{"driver":"postgresql","dsn":"' + $dsnB + '"}}'
    $prevDbSources = $env:DB_SOURCES_JSON
    try {
      $env:DB_SOURCES_JSON = $jsonB
      $started.agentB = Start-LoggedProcess `
        -Name "local_agent_b" `
        -FilePath $pythonExe `
        -Arguments @(
          "run_local_agent.py",
          "--node-id",
          "node_b",
          "--host",
          "127.0.0.1",
          "--port",
          "$AgentBPort",
          "--access-token",
          "$AgentBToken"
        ) `
        -WorkingDirectory $ProjectRoot `
        -LogDirectory $logDir
    } finally {
      if ($null -eq $prevDbSources) {
        Remove-Item Env:DB_SOURCES_JSON -ErrorAction SilentlyContinue
      } else {
        $env:DB_SOURCES_JSON = $prevDbSources
      }
    }
    Wait-TcpPort -HostName "127.0.0.1" -Port $AgentBPort -TimeoutSec 30 -Name "Local Agent B"
    Wait-HttpEndpoint `
      -Uri ("http://127.0.0.1:" + $AgentBPort + "/v1/jobs/ping_status") `
      -Headers @{ "X-Agent-Token" = $AgentBToken } `
      -TimeoutSec 30 `
      -Name "Local Agent B"
  } else {
    Write-Ok "Reusing existing Local Agent B"
    Wait-HttpEndpoint `
      -Uri ("http://127.0.0.1:" + $AgentBPort + "/v1/jobs/ping_status") `
      -Headers @{ "X-Agent-Token" = $AgentBToken } `
      -TimeoutSec 15 `
      -Name "Local Agent B"
  }

  if (-not (Test-TcpPort -HostName "127.0.0.1" -Port $OrchestratorPort)) {
    $ipfsChainExe = Join-Path $NornRoot "bin\ipfs-chain.exe"
    $started.orch = Start-LoggedProcess `
      -Name "orchestrator" `
      -FilePath $pythonExe `
      -Arguments @(
        "run_orchestrator.py",
        "--host",
        "127.0.0.1",
        "--port",
        "$OrchestratorPort",
        "--enable-chain-registration",
        "--chain-bin",
        "`"$ipfsChainExe`"",
        "--chain-receiver",
        "$ChainReceiver",
        "--chain-rpc",
        "127.0.0.1:45558",
        "--chain-ipfs",
        "http://127.0.0.1:5001",
        "--chain-timeout",
        "20"
      ) `
      -WorkingDirectory $ProjectRoot `
      -LogDirectory $logDir
    Wait-TcpPort -HostName "127.0.0.1" -Port $OrchestratorPort -TimeoutSec 35 -Name "Orchestrator"
    Wait-HttpEndpoint `
      -Uri ("http://127.0.0.1:" + $OrchestratorPort + "/v1/batches/ping_status") `
      -Headers @{} `
      -TimeoutSec 20 `
      -Name "Orchestrator"
  } else {
    Write-Ok "Reusing existing Orchestrator"
    Wait-HttpEndpoint `
      -Uri ("http://127.0.0.1:" + $OrchestratorPort + "/v1/batches/ping_status") `
      -Headers @{} `
      -TimeoutSec 15 `
      -Name "Orchestrator"
  }

  $orch = "http://127.0.0.1:" + $OrchestratorPort

  Write-Step "Register Domains"
  Invoke-RestMethod -Method Post -Uri ($orch + "/v1/domains/register") -ContentType "application/json" -Body (@{
    domain_id = $DomainAId
    endpoint = "http://127.0.0.1:$AgentAPort"
    access_token = $AgentAToken
  } | ConvertTo-Json) | Out-Null
  Invoke-RestMethod -Method Post -Uri ($orch + "/v1/domains/register") -ContentType "application/json" -Body (@{
    domain_id = $DomainBId
    endpoint = "http://127.0.0.1:$AgentBPort"
    access_token = $AgentBToken
  } | ConvertTo-Json) | Out-Null
  Write-Ok "Domain registrations completed"

  Write-Step "Create Distributed Batch (LLM enabled)"
  Write-StageProgress `
    -ReportDirectory $reportDir `
    -Stage "batch_create" `
    -Status "running" `
    -Message "submitting distributed batch request"
  $batch = Invoke-RestMethod -Method Post -Uri ($orch + "/v1/batches") -ContentType "application/json" -Body (@{
    run_id = $runId
    domain_ids = @($DomainAId, $DomainBId)
    mock_llm = $false
    max_fields_per_source = $MaxFieldsPerSource
    share_mode = "include_samples"
    poll_interval_sec = 0.5
    poll_timeout_sec = $PollTimeoutSec
  } | ConvertTo-Json)
  $batchId = [string]$batch.batch_id
  if (-not $batchId) {
    throw "batch_id missing in create batch response"
  }
  Write-Host ("batch_id=" + $batchId)
  Save-Json -Value $batch -FilePath (Join-Path $reportDir "batch_create_response.json")
  Write-StageProgress `
    -ReportDirectory $reportDir `
    -Stage "batch_create" `
    -Status "completed" `
    -Message "batch created" `
    -Meta @{ batch_id = $batchId }

  Write-Step "Poll Batch Status"
  Write-StageProgress `
    -ReportDirectory $reportDir `
    -Stage "batch_poll" `
    -Status "running" `
    -Message "waiting for batch completion" `
    -Meta @{ batch_id = $batchId }
  $status = $null
  $deadline = (Get-Date).AddSeconds($PollTimeoutSec)
  $lastBatchStatus = ""
  while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 1000
    $status = Invoke-RestMethod -Method Get -Uri ($orch + "/v1/batches/" + $batchId)
    Save-Json -Value $status -FilePath (Join-Path $reportDir "batch_status_live.json")
    Write-Host ("status=" + $status.status)
    $currentBatchStatus = [string]$status.status
    if ($currentBatchStatus -and $currentBatchStatus -ne $lastBatchStatus) {
      $lastBatchStatus = $currentBatchStatus
      Write-StageProgress `
        -ReportDirectory $reportDir `
        -Stage "batch_poll" `
        -Status "progress" `
        -Message ("batch status changed to " + $currentBatchStatus) `
        -Meta @{
          batch_id = $batchId
          batch_status = $currentBatchStatus
        }
    }
    if ($status.status -in @("succeeded", "failed")) {
      break
    }
  }
  if ($null -eq $status) {
    throw "failed to fetch batch status"
  }
  Save-Json -Value $status -FilePath (Join-Path $reportDir "batch_status.json")
  if ($status.status -ne "succeeded") {
    Write-StageProgress `
      -ReportDirectory $reportDir `
      -Stage "batch_poll" `
      -Status "failed" `
      -Message ("batch failed: " + [string]$status.error_message) `
      -Meta @{ batch_id = $batchId }
    throw ("batch failed: " + $status.error_message)
  }
  Write-Ok "Batch succeeded"
  Write-StageProgress `
    -ReportDirectory $reportDir `
    -Stage "batch_poll" `
    -Status "completed" `
    -Message "batch succeeded" `
    -Meta @{ batch_id = $batchId }

  Write-Step "Fetch Global Manifest"
  Write-StageProgress -ReportDirectory $reportDir -Stage "global_manifest" -Status "running" -Message "fetching batch manifest"
  $manifest = Invoke-RestMethod -Method Get -Uri ($orch + "/v1/batches/" + $batchId + "/manifest")
  Save-Json -Value $manifest -FilePath (Join-Path $reportDir "global_manifest.json")
  Write-StageProgress -ReportDirectory $reportDir -Stage "global_manifest" -Status "completed" -Message "manifest saved"
  $manifest.artifacts | Select-Object type, cid, record_count | Format-Table -AutoSize

  $domainArtifacts = foreach ($node in $manifest.node_manifests) {
    foreach ($dm in $node.domain_manifests) {
      foreach ($a in $dm.artifacts) {
        [pscustomobject]@{
          domain = $dm.domain_id
          type = $a.type
          cid = $a.cid
          record_count = $a.record_count
        }
      }
    }
  }
  Save-Json -Value $domainArtifacts -FilePath (Join-Path $reportDir "domain_artifacts.json")
  $domainArtifacts | Sort-Object domain, type | Format-Table -AutoSize

  $sampleStats = $domainArtifacts | Where-Object { $_.type -eq "samples" } | Select-Object domain, record_count
  $emptySampleDomains = @($sampleStats | Where-Object { [int]$_.record_count -le 0 } | Select-Object -ExpandProperty domain)
  if ($emptySampleDomains.Count -gt 0) {
    Write-WarnText ("domains with zero sampled fields: " + ($emptySampleDomains -join ", "))
  }

  # Export per-domain artifacts stage-by-stage so partial outputs are available even on later failures.
  $domainIds = @($DomainAId, $DomainBId)
  Export-DomainStageArtifacts `
    -ProjectRootPath $ProjectRoot `
    -ReportDirectory $reportDir `
    -DomainArtifacts $domainArtifacts `
    -DomainIds $domainIds `
    -ArtifactType "samples" `
    -StageName "01_sampling" `
    -FileSuffix "_samples.json" | Out-Null
  Export-DomainStageArtifacts `
    -ProjectRootPath $ProjectRoot `
    -ReportDirectory $reportDir `
    -DomainArtifacts $domainArtifacts `
    -DomainIds $domainIds `
    -ArtifactType "field_descriptions" `
    -StageName "02_field_descriptions" `
    -FileSuffix "_field_descriptions.json" | Out-Null
  Export-DomainStageArtifacts `
    -ProjectRootPath $ProjectRoot `
    -ReportDirectory $reportDir `
    -DomainArtifacts $domainArtifacts `
    -DomainIds $domainIds `
    -ArtifactType "domain_unified" `
    -StageName "03_domain_unified" `
    -FileSuffix "_domain_unified.json" | Out-Null
  Export-DomainStageArtifacts `
    -ProjectRootPath $ProjectRoot `
    -ReportDirectory $reportDir `
    -DomainArtifacts $domainArtifacts `
    -DomainIds $domainIds `
    -ArtifactType "domain_kg" `
    -StageName "04_domain_kg" `
    -FileSuffix "_domain_kg.json" | Out-Null

  Write-Step "Download Core Artifacts From IPFS"
  Write-StageProgress -ReportDirectory $reportDir -Stage "05_global_artifacts" -Status "running" -Message "fetching unified/alignment artifacts"
  $alignmentCid = ($manifest.artifacts | Where-Object { $_.type -eq "alignment_index" } | Select-Object -First 1).cid
  $unifiedFieldsCid = ($manifest.artifacts | Where-Object { $_.type -eq "unified_fields" } | Select-Object -First 1).cid
  $alignmentCypherCid = ($manifest.artifacts | Where-Object { $_.type -eq "alignment_cypher" } | Select-Object -First 1).cid

  if (-not $alignmentCid) { throw "alignment_index CID missing in manifest" }
  if (-not $unifiedFieldsCid) { throw "unified_fields CID missing in manifest" }
  if (-not $alignmentCypherCid) { throw "alignment_cypher CID missing in manifest" }

  $alignmentObj = Get-IpfsJsonObject -ProjectRootPath $ProjectRoot -Cid $alignmentCid
  $unifiedFieldsObj = Get-IpfsJsonObject -ProjectRootPath $ProjectRoot -Cid $unifiedFieldsCid
  $alignmentCypherObj = Get-IpfsJsonObject -ProjectRootPath $ProjectRoot -Cid $alignmentCypherCid

  Save-Json -Value $alignmentObj -FilePath (Join-Path $reportDir "alignment_index.json")
  Save-Json -Value $unifiedFieldsObj -FilePath (Join-Path $reportDir "unified_fields.json")
  Save-Json -Value $alignmentCypherObj -FilePath (Join-Path $reportDir "alignment_cypher.json")
  Save-Json -Value ([pscustomobject]@{
      stage = "05_global_artifacts"
      unified_fields_cid = $unifiedFieldsCid
      alignment_index_cid = $alignmentCid
      alignment_cypher_cid = $alignmentCypherCid
      unified_fields_count = @($unifiedFieldsObj).Count
      alignment_rows = @($alignmentObj).Count
      alignment_cypher_count = @($alignmentCypherObj).Count
    }) -FilePath (Join-Path $reportDir "stage_05_global_artifacts_summary.json")
  Write-StageProgress -ReportDirectory $reportDir -Stage "05_global_artifacts" -Status "completed" -Message "global artifacts exported"

  Write-Host ("alignment_rows=" + @($alignmentObj).Count)
  Write-Host ("unified_fields=" + @($unifiedFieldsObj).Count)
  Write-Host ("alignment_cypher_statements=" + @($alignmentCypherObj).Count)

  if (@($alignmentObj).Count -eq 0) {
    $samplesSummary = ($sampleStats | ForEach-Object { $_.domain + ":" + [string]$_.record_count }) -join ", "
    Write-StageProgress `
      -ReportDirectory $reportDir `
      -Stage "06_alignment_validation" `
      -Status "failed" `
      -Message ("alignment index is empty. sample_counts={" + $samplesSummary + "}")
    throw ("alignment index is empty; no cross-domain alignments were produced. sample_counts={" + $samplesSummary + "}")
  }
  Write-StageProgress -ReportDirectory $reportDir -Stage "06_alignment_validation" -Status "completed" -Message "alignment index is non-empty"

  $queryCanonical = [string]$alignmentObj[0].canonical_name
  if (-not $queryCanonical) {
    throw "first alignment row has empty canonical_name"
  }
  Write-Host ("query_canonical=" + $queryCanonical)

  Write-Step "Run Federated Query"
  Write-StageProgress -ReportDirectory $reportDir -Stage "07_federated_query" -Status "running" -Message "executing federated query"
  $federated = Invoke-RestMethod -Method Post -Uri ($orch + "/v1/query/federated") -ContentType "application/json" -Body (@{
    query_text = $queryCanonical
    source_domain = $DomainAId
    target_domain_ids = @($DomainBId)
    limit = 20
  } | ConvertTo-Json)
  Save-Json -Value $federated -FilePath (Join-Path $reportDir "federated_query.json")
  Write-Host ("anchors=" + @($federated.anchors).Count)
  Write-Host ("alignment_candidates=" + @($federated.alignment_candidates).Count)
  Write-Host ("hits=" + @($federated.hits).Count)
  $federated.hits | Select-Object -First 20 domain_id, canonical_name, table, field, score, relation_type, alignment_score | Format-Table -AutoSize
  Write-StageProgress `
    -ReportDirectory $reportDir `
    -Stage "07_federated_query" `
    -Status "completed" `
    -Message "federated query exported" `
    -Meta @{
      hit_count = @($federated.hits).Count
      anchor_count = @($federated.anchors).Count
    }

  Write-Step "Verify On-Chain Manifest Record (go-norn)"
  Write-StageProgress -ReportDirectory $reportDir -Stage "08_onchain_verify" -Status "running" -Message "resolving on-chain manifest"
  $chainKey = [string]$status.manifest_chain_key
  if (-not $chainKey) {
    throw "manifest_chain_key is empty. Orchestrator may not be running with --enable-chain-registration."
  }
  $ipfsChainExe = Join-Path $NornRoot "bin\ipfs-chain.exe"
  if (-not (Test-Path $ipfsChainExe)) {
    throw "ipfs-chain binary not found: $ipfsChainExe"
  }
  $chainOutFile = Join-Path $reportDir "onchain_manifest.json"
  & $ipfsChainExe get `
    -address $ChainReceiver `
    -key $chainKey `
    -out $chainOutFile `
    -rpc "127.0.0.1:45558" `
    -ipfs "http://127.0.0.1:5001"
  if (-not (Test-Path $chainOutFile)) {
    throw "failed to export on-chain manifest file"
  }
  Write-Ok ("on-chain manifest file: " + $chainOutFile)
  Write-StageProgress `
    -ReportDirectory $reportDir `
    -Stage "08_onchain_verify" `
    -Status "completed" `
    -Message "on-chain manifest exported" `
    -Meta @{ output_file = $chainOutFile }

  Write-Step "Demo Summary"
  Write-StageProgress -ReportDirectory $reportDir -Stage "09_summary" -Status "running" -Message "writing summary"
  $summary = [pscustomobject]@{
    run_id = $runId
    batch_id = $batchId
    report_dir = $reportDir
    alignment_rows = @($alignmentObj).Count
    unified_fields = @($unifiedFieldsObj).Count
    query_hits = @($federated.hits).Count
    manifest_chain_cid = [string]$status.manifest_chain_cid
    manifest_chain_key = [string]$status.manifest_chain_key
    manifest_tx_hash = [string]$status.manifest_tx_hash
  }
  Save-Json -Value $summary -FilePath (Join-Path $reportDir "summary.json")
  $summary | Format-List
  Write-StageProgress -ReportDirectory $reportDir -Stage "09_summary" -Status "completed" -Message "summary exported"
  Write-Ok ("Artifacts exported under: " + $reportDir)
}
catch {
  try {
    Write-StageProgress `
      -ReportDirectory $reportDir `
      -Stage "run_failed" `
      -Status "failed" `
      -Message $_.Exception.Message
  } catch {
    # keep original failure path if stage progress writing also fails
  }
  throw
}
finally {
  if ($KeepServices) {
    Write-WarnText "KeepServices enabled: leaving started services running"
  } else {
    Write-Step "Cleanup Started Services"
    foreach ($entry in @("orch", "agentA", "agentB", "norn")) {
      $proc = $started[$entry]
      if ($null -eq $proc) {
        continue
      }
      try {
        if (-not $proc.HasExited) {
          Stop-Process -Id $proc.Id -Force
          Write-Ok ("Stopped " + $entry + " pid=" + $proc.Id)
        }
      } catch {
        Write-WarnText ("Failed to stop " + $entry + ": " + $_.Exception.Message)
      }
    }
  }
}
