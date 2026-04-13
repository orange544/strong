param(
  [string]$MySqlHost = '127.0.0.1',
  [int]$MySqlPort = 3306,
  [string]$MySqlUser = 'root',
  [string]$MySqlPassword = '123456',

  [string]$PostgresHost = '127.0.0.1',
  [int]$PostgresPort = 5432,
  [string]$PostgresUser = 'postgres',
  [string]$PostgresPassword = '123456',

  [string]$CassandraHost = '127.0.0.1',
  [int]$CassandraPort = 9042,
  [string]$CassandraUser = '',
  [string]$CassandraPassword = ''
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
    $results.Add([pscustomobject]@{ Step = $Step; Status = 'OK'; Message = 'Initialized' })
  }
  catch {
    $results.Add([pscustomobject]@{ Step = $Step; Status = 'FAILED'; Message = $_.Exception.Message })
  }
}

$base = $PSScriptRoot

Invoke-Step -Step 'T1 PostgreSQL' -ScriptPath (Join-Path $base 't1_postgresql\apply.ps1') -Arguments @{
  DbHost = $PostgresHost
  Port = $PostgresPort
  User = $PostgresUser
  Password = $PostgresPassword
}

Invoke-Step -Step 'T2 MySQL' -ScriptPath (Join-Path $base 't2_mysql\apply.ps1') -Arguments @{
  DbHost = $MySqlHost
  Port = $MySqlPort
  User = $MySqlUser
  Password = $MySqlPassword
}

Invoke-Step -Step 'T1 Cassandra' -ScriptPath (Join-Path $base 't1_cassandra\apply.ps1') -Arguments @{
  DbHost = $CassandraHost
  Port = $CassandraPort
  User = $CassandraUser
  Password = $CassandraPassword
}

$results | Format-Table -AutoSize

$failed = $results | Where-Object { $_.Status -eq 'FAILED' }
if ($failed.Count -gt 0) {
  exit 1
}
