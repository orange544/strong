param(
  [string]$PostgresHost = '127.0.0.1',
  [int]$PostgresPort = 5432,
  [string]$PostgresUser = 'postgres',
  [string]$PostgresPassword = '123456',

  [string]$RedisHost = '127.0.0.1',
  [int]$RedisPort = 6379,
  [string]$RedisPassword = '123456',

  [string]$MongoHost = '127.0.0.1',
  [int]$MongoPort = 27017,
  [string]$MongoUser = '',
  [string]$MongoPassword = '',
  [string]$MongoAuthDb = 'admin'
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

Invoke-Step -Step 'H1 PostgreSQL' -ScriptPath (Join-Path $base 'h1_postgresql\apply.ps1') -Arguments @{
  DbHost = $PostgresHost
  Port = $PostgresPort
  User = $PostgresUser
  Password = $PostgresPassword
}

Invoke-Step -Step 'H1 Redis' -ScriptPath (Join-Path $base 'h1_redis\apply.ps1') -Arguments @{
  DbHost = $RedisHost
  Port = $RedisPort
  Password = $RedisPassword
}

Invoke-Step -Step 'D1 MongoDB' -ScriptPath (Join-Path $base 'd1_mongodb\apply.ps1') -Arguments @{
  DbHost = $MongoHost
  Port = $MongoPort
  User = $MongoUser
  Password = $MongoPassword
  AuthDb = $MongoAuthDb
}

Invoke-Step -Step 'D1 SQLite' -ScriptPath (Join-Path $base 'd1_sqlite\apply.ps1')

$results | Format-Table -AutoSize

$failed = $results | Where-Object { $_.Status -eq 'FAILED' }
if ($failed.Count -gt 0) {
  exit 1
}
