param(
  [string]$OracleConnection = 'system/Oracle123!@127.0.0.1:1521/FREEPDB1'
)

$results = New-Object System.Collections.Generic.List[object]

function Invoke-Step {
  param(
    [string]$Step,
    [string]$ScriptPath,
    [hashtable]$Arguments = @{}
  )

  if (-not (Test-Path $ScriptPath)) {
    $results.Add([pscustomobject]@{ Step = $Step; Status = 'SKIPPED'; Message = 'Script not found' })
    return
  }

  try {
    & $ScriptPath @Arguments
    $results.Add([pscustomobject]@{ Step = $Step; Status = 'OK'; Message = 'Initialized with sample data' })
  }
  catch {
    $results.Add([pscustomobject]@{ Step = $Step; Status = 'FAILED'; Message = $_.Exception.Message })
  }
}

$base = $PSScriptRoot

Invoke-Step -Step 'DB3 Oracle Student' -ScriptPath (Join-Path $base 'db3_oracle_student\apply.ps1') -Arguments @{
  AdminConnection = $OracleConnection
}

Invoke-Step -Step 'DB11 Oracle Dorm' -ScriptPath (Join-Path $base 'db11_oracle_dorm\apply.ps1') -Arguments @{
  AdminConnection = $OracleConnection
}

Invoke-Step -Step 'DB12 Oracle Card' -ScriptPath (Join-Path $base 'db12_oracle_card\apply.ps1') -Arguments @{
  AdminConnection = $OracleConnection
}

Invoke-Step -Step 'DB17 Oracle Teacher' -ScriptPath (Join-Path $base 'db17_oracle_teacher\apply.ps1') -Arguments @{
  AdminConnection = $OracleConnection
}

$results | Format-Table -AutoSize

$failed = $results | Where-Object { $_.Status -eq 'FAILED' }
if ($failed.Count -gt 0) {
  exit 1
}

