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

Invoke-Step -Step 'DB19 MySQL Performance' -ScriptPath (Join-Path $base 'db19_mysql_performance\apply.ps1') -Arguments @{
  DbHost = $MySqlHost
  Port = $MySqlPort
  User = $MySqlUser
  Password = $MySqlPassword
}

Invoke-Step -Step 'DB20 PostgreSQL TeacherDev' -ScriptPath (Join-Path $base 'db20_postgresql_teacher_dev\apply.ps1') -Arguments @{
  DbHost = $PostgresHost
  Port = $PostgresPort
  User = $PostgresUser
  Password = $PostgresPassword
}

Invoke-Step -Step 'DB21 PostgreSQL Research' -ScriptPath (Join-Path $base 'db21_postgresql_research\apply.ps1') -Arguments @{
  DbHost = $PostgresHost
  Port = $PostgresPort
  User = $PostgresUser
  Password = $PostgresPassword
}

Invoke-Step -Step 'DB22 MongoDB Achievement' -ScriptPath (Join-Path $base 'db22_mongodb_achievement\apply.ps1') -Arguments @{
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
