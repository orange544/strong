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
  [string]$MongoAuthDb = 'admin',

  [string]$RedisHost = '127.0.0.1',
  [int]$RedisPort = 6379,
  [string]$RedisPassword = '123456',

  [string]$Neo4jUri = 'bolt://127.0.0.1:7687',
  [string]$Neo4jUser = 'neo4j',
  [string]$Neo4jPassword = '123456',

  [string]$OracleConnection = '',

  [string]$ClickHouseHost = '127.0.0.1',
  [int]$ClickHousePort = 9000,
  [string]$ClickHouseUser = 'default',
  [string]$ClickHousePassword = '123456',

  [string]$TiDbHost = '127.0.0.1',
  [int]$TiDbPort = 4000,
  [string]$TiDbUser = 'root',
  [string]$TiDbPassword = '123456',

  [string]$CassandraHost = '127.0.0.1',
  [int]$CassandraPort = 9042,
  [string]$CassandraUser = '',
  [string]$CassandraPassword = '',

  [switch]$RunNeo4j,
  [switch]$IncludeOptionalEngines
)

$results = New-Object System.Collections.Generic.List[object]

function Invoke-Step {
  param(
    [string]$Name,
    [string]$ScriptPath,
    [hashtable]$Arguments = @{}
  )

  if (-not (Test-Path $ScriptPath)) {
    $results.Add([pscustomobject]@{ Engine = $Name; Status = 'SKIPPED'; Message = 'Script not found' })
    return
  }

  try {
    & $ScriptPath @Arguments
    $results.Add([pscustomobject]@{ Engine = $Name; Status = 'OK'; Message = 'Initialized' })
  }
  catch {
    $results.Add([pscustomobject]@{ Engine = $Name; Status = 'FAILED'; Message = $_.Exception.Message })
  }
}

$base = $PSScriptRoot

Invoke-Step -Name 'SQLite' -ScriptPath (Join-Path $base 'sqlite\apply.ps1')
Invoke-Step -Name 'MongoDB' -ScriptPath (Join-Path $base 'mongodb\apply.ps1') -Arguments @{ DbHost = $MongoHost; Port = $MongoPort; User = $MongoUser; Password = $MongoPassword; AuthDb = $MongoAuthDb }
Invoke-Step -Name 'Redis' -ScriptPath (Join-Path $base 'redis\apply.ps1') -Arguments @{ DbHost = $RedisHost; Port = $RedisPort; Password = $RedisPassword }
Invoke-Step -Name 'MySQL' -ScriptPath (Join-Path $base 'mysql\apply.ps1') -Arguments @{ DbHost = $MySqlHost; Port = $MySqlPort; User = $MySqlUser; Password = $MySqlPassword }
Invoke-Step -Name 'PostgreSQL' -ScriptPath (Join-Path $base 'postgresql\apply.ps1') -Arguments @{ DbHost = $PostgresHost; Port = $PostgresPort; User = $PostgresUser; Password = $PostgresPassword }

if ($RunNeo4j) {
  Invoke-Step -Name 'Neo4j' -ScriptPath (Join-Path $base 'neo4j\apply.ps1') -Arguments @{ Uri = $Neo4jUri; User = $Neo4jUser; Password = $Neo4jPassword }
}

if ($IncludeOptionalEngines) {
  if ($OracleConnection) {
    Invoke-Step -Name 'Oracle' -ScriptPath (Join-Path $base 'oracle\apply.ps1') -Arguments @{ Connection = $OracleConnection }
  } else {
    $results.Add([pscustomobject]@{ Engine = 'Oracle'; Status = 'SKIPPED'; Message = 'Set -OracleConnection to run' })
  }
  Invoke-Step -Name 'ClickHouse' -ScriptPath (Join-Path $base 'clickhouse\apply.ps1') -Arguments @{ DbHost = $ClickHouseHost; Port = $ClickHousePort; User = $ClickHouseUser; Password = $ClickHousePassword }
  Invoke-Step -Name 'TiDB' -ScriptPath (Join-Path $base 'tidb\apply.ps1') -Arguments @{ DbHost = $TiDbHost; Port = $TiDbPort; User = $TiDbUser; Password = $TiDbPassword }
  Invoke-Step -Name 'Cassandra' -ScriptPath (Join-Path $base 'cassandra\apply.ps1') -Arguments @{ DbHost = $CassandraHost; Port = $CassandraPort; User = $CassandraUser; Password = $CassandraPassword }
  Invoke-Step -Name 'HBase' -ScriptPath (Join-Path $base 'hbase\apply.ps1')
}

$results | Format-Table -AutoSize

$failed = $results | Where-Object { $_.Status -eq 'FAILED' }
if ($failed.Count -gt 0) {
  exit 1
}

