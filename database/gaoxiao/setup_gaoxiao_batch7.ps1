param(
  [string]$ElasticsearchEndpoint = 'http://127.0.0.1:9200',
  [string]$OracleConnection = 'system/Oracle123!@127.0.0.1:1521/FREEPDB1',
  [string]$Neo4jUri = 'bolt://127.0.0.1:17687',
  [string]$Neo4jUser = 'neo4j',
  [string]$Neo4jPassword = '12345678'
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

Invoke-Step -Step 'DB8 Elasticsearch Question' -ScriptPath (Join-Path $base 'db8_elasticsearch_question\apply.ps1') -Arguments @{
  Endpoint = $ElasticsearchEndpoint
}

Invoke-Step -Step 'DB23 Oracle Finance' -ScriptPath (Join-Path $base 'db23_oracle_finance\apply.ps1') -Arguments @{
  AdminConnection = $OracleConnection
}

Invoke-Step -Step 'DB26 Neo4j Semantic' -ScriptPath (Join-Path $base 'db26_neo4j_semantic\apply.ps1') -Arguments @{
  Uri = $Neo4jUri
  User = $Neo4jUser
  Password = $Neo4jPassword
}

$results | Format-Table -AutoSize

$failed = $results | Where-Object { $_.Status -eq 'FAILED' }
if ($failed.Count -gt 0) {
  exit 1
}
