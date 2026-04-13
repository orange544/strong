param(
  [string]$MySqlHost = '127.0.0.1',
  [int]$MySqlPort = 3306,
  [string]$MySqlUser = 'root',
  [string]$MySqlPassword = '123456',

  [string]$PostgresHost = '127.0.0.1',
  [int]$PostgresPort = 5432,
  [string]$PostgresUser = 'postgres',
  [string]$PostgresPassword = '123456',

  [string]$MongoHost = '127.0.0.1',
  [int]$MongoPort = 27017,
  [string]$MongoUser = '',
  [string]$MongoPassword = '',
  [string]$MongoAuthDb = 'admin'
)

$results = New-Object System.Collections.Generic.List[object]

function Invoke-Step {
  param(
    [string]$Name,
    [string]$ScriptPath,
    [hashtable]$Arguments = @{}
  )

  if (-not (Test-Path $ScriptPath)) {
    $results.Add([pscustomobject]@{ Step = $Name; Status = 'SKIPPED'; Message = 'Script not found' })
    return
  }

  try {
    & $ScriptPath @Arguments
    $results.Add([pscustomobject]@{ Step = $Name; Status = 'OK'; Message = 'Initialized' })
  }
  catch {
    $results.Add([pscustomobject]@{ Step = $Name; Status = 'FAILED'; Message = $_.Exception.Message })
  }
}

$base = $PSScriptRoot

Invoke-Step -Name 'M1 MySQL' -ScriptPath (Join-Path $base 'm1_mysql\apply.ps1') -Arguments @{
  DbHost = $MySqlHost
  Port = $MySqlPort
  User = $MySqlUser
  Password = $MySqlPassword
}

Invoke-Step -Name 'M2 MySQL' -ScriptPath (Join-Path $base 'm2_mysql\apply.ps1') -Arguments @{
  DbHost = $MySqlHost
  Port = $MySqlPort
  User = $MySqlUser
  Password = $MySqlPassword
}

Invoke-Step -Name 'P1 PostgreSQL' -ScriptPath (Join-Path $base 'p1_postgresql\apply.ps1') -Arguments @{
  DbHost = $PostgresHost
  Port = $PostgresPort
  User = $PostgresUser
  Password = $PostgresPassword
}

Invoke-Step -Name 'P1 MongoDB' -ScriptPath (Join-Path $base 'p1_mongodb\apply.ps1') -Arguments @{
  DbHost = $MongoHost
  Port = $MongoPort
  User = $MongoUser
  Password = $MongoPassword
  AuthDb = $MongoAuthDb
}

$results | Format-Table -AutoSize

$failed = $results | Where-Object { $_.Status -eq 'FAILED' }
if ($failed.Count -gt 0) {
  exit 1
}
