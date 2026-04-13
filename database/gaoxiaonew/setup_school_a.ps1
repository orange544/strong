param(
  [string]$MySqlHost = '127.0.0.1',
  [int]$MySqlPort = 3306,
  [string]$MySqlUser = 'root',
  [string]$MySqlPassword = '123456',

  [string]$PostgresHost = '127.0.0.1',
  [int]$PostgresPort = 5432,
  [string]$PostgresUser = 'postgres',
  [string]$PostgresPassword = '123456',
  [string]$PostgresDatabase = 'research_output_db',

  [string]$MongoHost = '127.0.0.1',
  [int]$MongoPort = 27017,
  [string]$MongoUser = '',
  [string]$MongoPassword = '',
  [string]$MongoAuthDb = 'admin',

  [string]$Neo4jUri = 'bolt://127.0.0.1:17687',
  [string]$Neo4jUser = 'neo4j',
  [string]$Neo4jPassword = '12345678',

  [string]$RedisHost = '127.0.0.1',
  [int]$RedisPort = 6379,
  [string]$RedisPassword = '123456',
  [int]$RedisDbIndex = 1,

  [string]$ElasticsearchEndpoint = 'http://127.0.0.1:9200'
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

Invoke-Step -Step 'School A MySQL' -ScriptPath (Join-Path $base 'school_a\mysql\apply.ps1') -Arguments @{
  DbHost = $MySqlHost
  Port = $MySqlPort
  User = $MySqlUser
  Password = $MySqlPassword
}

Invoke-Step -Step 'School A PostgreSQL' -ScriptPath (Join-Path $base 'school_a\postgresql\apply.ps1') -Arguments @{
  DbHost = $PostgresHost
  Port = $PostgresPort
  User = $PostgresUser
  Password = $PostgresPassword
  Database = $PostgresDatabase
}

Invoke-Step -Step 'School A MongoDB' -ScriptPath (Join-Path $base 'school_a\mongodb\apply.ps1') -Arguments @{
  DbHost = $MongoHost
  Port = $MongoPort
  User = $MongoUser
  Password = $MongoPassword
  AuthDb = $MongoAuthDb
}

$oldJavaToolOptions = $env:JAVA_TOOL_OPTIONS
$env:JAVA_TOOL_OPTIONS = '-Xms64m -Xmx256m'
try {
  Invoke-Step -Step 'School A Neo4j' -ScriptPath (Join-Path $base 'school_a\neo4j\apply.ps1') -Arguments @{
    Uri = $Neo4jUri
    User = $Neo4jUser
    Password = $Neo4jPassword
  }
}
finally {
  if ($null -eq $oldJavaToolOptions) {
    Remove-Item Env:JAVA_TOOL_OPTIONS -ErrorAction SilentlyContinue
  }
  else {
    $env:JAVA_TOOL_OPTIONS = $oldJavaToolOptions
  }
}

Invoke-Step -Step 'School A Redis' -ScriptPath (Join-Path $base 'school_a\redis\apply.ps1') -Arguments @{
  DbHost = $RedisHost
  Port = $RedisPort
  Password = $RedisPassword
  DbIndex = $RedisDbIndex
}

Invoke-Step -Step 'School A Elasticsearch' -ScriptPath (Join-Path $base 'school_a\elasticsearch\apply.ps1') -Arguments @{
  Endpoint = $ElasticsearchEndpoint
}

$results | Format-Table -AutoSize

$failed = @($results | Where-Object { $_.Status -eq 'FAILED' })
if ($failed.Count -gt 0) {
  throw 'setup_school_a failed.'
}



