param(
  [switch]$RunA = $true,
  [switch]$RunB = $true
)

$results = New-Object System.Collections.Generic.List[object]

function Invoke-Step {
  param(
    [string]$Step,
    [string]$ScriptPath
  )

  if (-not (Test-Path $ScriptPath)) {
    $results.Add([pscustomobject]@{ Step = $Step; Status = 'SKIPPED'; Message = 'Script not found' })
    return
  }

  try {
    & $ScriptPath
    if ($LASTEXITCODE -ne 0) {
      throw "Script failed with exit code $LASTEXITCODE"
    }
    $results.Add([pscustomobject]@{ Step = $Step; Status = 'OK'; Message = 'Completed' })
  }
  catch {
    $results.Add([pscustomobject]@{ Step = $Step; Status = 'FAILED'; Message = $_.Exception.Message })
  }
}

$base = $PSScriptRoot

if ($RunA) {
  Invoke-Step -Step 'Setup School A' -ScriptPath (Join-Path $base 'setup_school_a.ps1')
}
if ($RunB) {
  Invoke-Step -Step 'Setup School B' -ScriptPath (Join-Path $base 'setup_school_b.ps1')
}

$results | Format-Table -AutoSize

$failed = @($results | Where-Object { $_.Status -eq 'FAILED' })
if ($failed.Count -gt 0) {
  exit 1
}


