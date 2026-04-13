param(
  [string]$MySqlHost = '127.0.0.1',
  [int]$MySqlPort = 3306,
  [string]$MySqlUser = 'root',
  [string]$MySqlPassword = '123456',

  [string]$PostgresHost = '127.0.0.1',
  [int]$PostgresPort = 5432,
  [string]$PostgresUser = 'postgres',
  [string]$PostgresPassword = '123456'
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

Invoke-Step -Step 'DB6 PostgreSQL Graduate' -ScriptPath (Join-Path $base 'db6_postgresql_graduate\apply.ps1') -Arguments @{
  DbHost = $PostgresHost
  Port = $PostgresPort
  User = $PostgresUser
  Password = $PostgresPassword
}

Invoke-Step -Step 'DB7 MySQL Exam' -ScriptPath (Join-Path $base 'db7_mysql_exam\apply.ps1') -Arguments @{
  DbHost = $MySqlHost
  Port = $MySqlPort
  User = $MySqlUser
  Password = $MySqlPassword
}

Invoke-Step -Step 'DB9 PostgreSQL StudentAffairs' -ScriptPath (Join-Path $base 'db9_postgresql_student_affairs\apply.ps1') -Arguments @{
  DbHost = $PostgresHost
  Port = $PostgresPort
  User = $PostgresUser
  Password = $PostgresPassword
}

Invoke-Step -Step 'DB10 MySQL Aid' -ScriptPath (Join-Path $base 'db10_mysql_aid\apply.ps1') -Arguments @{
  DbHost = $MySqlHost
  Port = $MySqlPort
  User = $MySqlUser
  Password = $MySqlPassword
}

$results | Format-Table -AutoSize

$failed = $results | Where-Object { $_.Status -eq 'FAILED' }
if ($failed.Count -gt 0) {
  exit 1
}
