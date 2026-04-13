param(
  [string]$MySqlHost = '127.0.0.1',
  [int]$MySqlPort = 3306,
  [string]$MySqlUser = 'root',
  [string]$MySqlPassword = '123456',

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

Invoke-Step -Step 'DB14 MySQL Access' -ScriptPath (Join-Path $base 'db14_mysql_access\apply.ps1') -Arguments @{
  DbHost = $MySqlHost
  Port = $MySqlPort
  User = $MySqlUser
  Password = $MySqlPassword
}

Invoke-Step -Step 'DB15 MySQL Library' -ScriptPath (Join-Path $base 'db15_mysql_library\apply.ps1') -Arguments @{
  DbHost = $MySqlHost
  Port = $MySqlPort
  User = $MySqlUser
  Password = $MySqlPassword
}

Invoke-Step -Step 'DB16 MongoDB EResource' -ScriptPath (Join-Path $base 'db16_mongodb_eresource\apply.ps1') -Arguments @{
  DbHost = $MongoHost
  Port = $MongoPort
  User = $MongoUser
  Password = $MongoPassword
  AuthDb = $MongoAuthDb
}

Invoke-Step -Step 'DB18 MySQL Staff' -ScriptPath (Join-Path $base 'db18_mysql_staff\apply.ps1') -Arguments @{
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
