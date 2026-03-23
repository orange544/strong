param(
  [string]$Uri = 'bolt://127.0.0.1:7687',
  [string]$User = 'neo4j',
  [string]$Password = '123456'
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

$schemaFile = Join-Path $PSScriptRoot 'movie_neo4j.cypher'
$sampleFile = Join-Path $PSScriptRoot 'movie_neo4j_sample_data.cypher'
if (-not (Test-Path $sampleFile)) {
  throw 'Neo4j sample data file not found.'
}

function Invoke-CypherFile([string]$FilePath) {
  $scriptText = Get-Content -Raw -Encoding UTF8 $FilePath
  if ($scriptText.Length -gt 0 -and [int][char]$scriptText[0] -eq 65279) {
    $scriptText = $scriptText.Substring(1)
  }
  $tempFile = Join-Path $env:TEMP ("movie_neo4j_" + [guid]::NewGuid().ToString('N') + '.cypher')
  [System.IO.File]::WriteAllText($tempFile, $scriptText, [System.Text.UTF8Encoding]::new($false))
  try {
    & $cypher -a $Uri -u $User -p $Password -f $tempFile
  }
  finally {
    Remove-Item $tempFile -ErrorAction SilentlyContinue
  }
  if ($LASTEXITCODE -ne 0) {
    throw "Neo4j execution failed: $FilePath"
  }
}

Invoke-CypherFile -FilePath $schemaFile
Invoke-CypherFile -FilePath $sampleFile

Write-Host 'Neo4j schema and sample graph initialized on default database.'
