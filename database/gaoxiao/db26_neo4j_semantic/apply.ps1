param(
  [string]$Uri = 'bolt://127.0.0.1:17687',
  [string]$User = 'neo4j',
  [string]$Password = '12345678'
)

$candidates = @()
$cypherCmd = Get-Command cypher-shell -ErrorAction SilentlyContinue
if ($cypherCmd) {
  $candidates += $cypherCmd.Source
}
$candidates += 'D:\Program Files\neo4j-community-5.26.0\bin\cypher-shell.bat'
$candidates += 'C:\Program Files\neo4j-community-5.26.0\bin\cypher-shell.bat'
$cypher = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $cypher) {
  throw 'cypher-shell not found.'
}

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$schemaFile = Join-Path $PSScriptRoot 'db26_neo4j.cypher'
$sampleFile = Join-Path $PSScriptRoot 'db26_neo4j_sample_data.cypher'
if (-not (Test-Path $schemaFile)) {
  throw 'DB26 Neo4j schema file not found.'
}
if (-not (Test-Path $sampleFile)) {
  throw 'DB26 Neo4j sample data file not found.'
}

function Invoke-CypherFile {
  param([string]$FilePath)

  $scriptText = Get-Content -Raw -Encoding UTF8 $FilePath
  if ($scriptText.Length -gt 0 -and [int][char]$scriptText[0] -eq 65279) {
    $scriptText = $scriptText.Substring(1)
  }

  $tempFile = Join-Path $env:TEMP ("db26_neo4j_" + [guid]::NewGuid().ToString('N') + '.cypher')
  [System.IO.File]::WriteAllText($tempFile, $scriptText, [System.Text.UTF8Encoding]::new($false))
  try {
    & $cypher -a $Uri -u $User -p $Password -f $tempFile
  }
  finally {
    Remove-Item $tempFile -ErrorAction SilentlyContinue
  }
  if ($LASTEXITCODE -ne 0) {
    throw "DB26 Neo4j execution failed: $FilePath"
  }
}

function Get-CypherCount {
  param([string]$Query)

  $output = & $cypher -a $Uri -u $User -p $Password --format plain $Query 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "DB26 Neo4j verify query failed: $Query"
  }

  $allText = ($output | Out-String).Trim()
  $matches = [regex]::Matches($allText, '\d+')
  if ($matches.Count -lt 1) {
    throw "DB26 Neo4j verify parse failed: $Query"
  }

  [int]$matches[$matches.Count - 1].Value
}

Invoke-CypherFile -FilePath $schemaFile
Invoke-CypherFile -FilePath $sampleFile

$verify = New-Object System.Collections.Generic.List[object]
$verify.Add([pscustomobject]@{ Item = 'System nodes'; Count = (Get-CypherCount "MATCH (n:System) RETURN count(n);") })
$verify.Add([pscustomobject]@{ Item = 'Database nodes'; Count = (Get-CypherCount "MATCH (n:Database) RETURN count(n);") })
$verify.Add([pscustomobject]@{ Item = 'Field nodes'; Count = (Get-CypherCount "MATCH (n:Field) RETURN count(n);") })
$verify.Add([pscustomobject]@{ Item = 'BusinessConcept nodes'; Count = (Get-CypherCount "MATCH (n:BusinessConcept) RETURN count(n);") })
$verify.Add([pscustomobject]@{ Item = 'Relationships'; Count = (Get-CypherCount "MATCH ()-[r]->() RETURN count(r);") })

foreach ($row in $verify) {
  if ($row.Count -lt 1) {
    throw "DB26 Neo4j has empty verification item: $($row.Item)"
  }
}

$verify | Format-Table -AutoSize
Write-Host 'DB26 Neo4j schema and sample graph initialized.'
