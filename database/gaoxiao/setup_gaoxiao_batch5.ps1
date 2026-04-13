param(
  [string]$MySqlHost = '127.0.0.1',
  [int]$MySqlPort = 3306,
  [string]$MySqlUser = 'root',
  [string]$MySqlPassword = '123456',

  [string]$PostgresHost = '127.0.0.1',
  [int]$PostgresPort = 5432,
  [string]$PostgresUser = 'postgres',
  [string]$PostgresPassword = '123456',

  [string]$RedisHost = '127.0.0.1',
  [int]$RedisPort = 6379,
  [string]$RedisPassword = '123456',
  [int]$RedisDb = 13
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

Invoke-Step -Step 'DB13 Redis Realtime' -ScriptPath (Join-Path $base 'db13_redis_realtime\apply.ps1') -Arguments @{
  DbHost = $RedisHost
  Port = $RedisPort
  Password = $RedisPassword
  DbIndex = $RedisDb
}

Invoke-Step -Step 'DB24 PostgreSQL Asset' -ScriptPath (Join-Path $base 'db24_postgresql_asset\apply.ps1') -Arguments @{
  DbHost = $PostgresHost
  Port = $PostgresPort
  User = $PostgresUser
  Password = $PostgresPassword
}

Invoke-Step -Step 'DB25 MySQL Lab' -ScriptPath (Join-Path $base 'db25_mysql_lab\apply.ps1') -Arguments @{
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
