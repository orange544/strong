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

$schemaFile = Join-Path $PSScriptRoot 'school_a_neo4j.cypher'
$sampleFile = Join-Path $PSScriptRoot 'school_a_neo4j_sample_data.cypher'
if (-not (Test-Path $schemaFile)) {
  throw 'School A Neo4j schema file not found.'
}
if (-not (Test-Path $sampleFile)) {
  throw 'School A Neo4j sample data file not found.'
}

function Invoke-CypherFile {
  param([string]$FilePath)

  $scriptText = Get-Content -Raw -Encoding UTF8 $FilePath
  if ($scriptText.Length -gt 0 -and [int][char]$scriptText[0] -eq 65279) {
    $scriptText = $scriptText.Substring(1)
  }

  $tempFile = Join-Path $env:TEMP ("school_a_neo4j_" + [guid]::NewGuid().ToString('N') + '.cypher')
  [System.IO.File]::WriteAllText($tempFile, $scriptText, [System.Text.UTF8Encoding]::new($false))
  try {
    & $cypher -a $Uri -u $User -p $Password -f $tempFile
  }
  finally {
    Remove-Item $tempFile -ErrorAction SilentlyContinue
  }
  if ($LASTEXITCODE -ne 0) {
    throw "School A Neo4j execution failed: $FilePath"
  }
}

function Get-CypherCount {
  param([string]$Query)

  $output = & $cypher -a $Uri -u $User -p $Password --format plain $Query 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "School A Neo4j verify query failed: $Query"
  }

  $lines = @($output | ForEach-Object { ($_ | Out-String).Trim() } | Where-Object { $_ -match '^\d+$' })
  if ($lines.Count -lt 1) {
    throw "School A Neo4j verify parse failed: $Query"
  }

  [int]$lines[0]
}

Invoke-CypherFile -FilePath $schemaFile
Invoke-CypherFile -FilePath $sampleFile

$verify = New-Object System.Collections.Generic.List[object]
$verify.Add([pscustomobject]@{ Item = 'Teacher'; Count = (Get-CypherCount "MATCH (n:Teacher {teacher_id:'T000123'}) RETURN count(n);") })
$verify.Add([pscustomobject]@{ Item = 'Project'; Count = (Get-CypherCount "MATCH (n:Project {project_id:'RP2024-00125'}) RETURN count(n);") })
$verify.Add([pscustomobject]@{ Item = 'LEADS'; Count = (Get-CypherCount "MATCH (:Teacher {teacher_id:'T000123'})-[r:LEADS]->(:Project {project_id:'RP2024-00125'}) RETURN count(r);") })
$verify.Add([pscustomobject]@{ Item = 'SourceField'; Count = (Get-CypherCount "MATCH (n:SourceField {field_id:'faculty_hr_db.teacher.teacher_name'}) RETURN count(n);") })
$verify.Add([pscustomobject]@{ Item = 'SemanticConcept'; Count = (Get-CypherCount "MATCH (n:SemanticConcept {concept_id:'Teacher.Name'}) RETURN count(n);") })
$verify.Add([pscustomobject]@{ Item = 'MAPS_TO'; Count = (Get-CypherCount "MATCH (:SourceField {field_id:'faculty_hr_db.teacher.teacher_name'})-[r:MAPS_TO]->(:SemanticConcept {concept_id:'Teacher.Name'}) RETURN count(r);") })

foreach ($row in $verify) {
  if ($row.Count -lt 1) {
    throw "School A Neo4j has empty verification item: $($row.Item)"
  }
}

$verify | Format-Table -AutoSize
Write-Host 'School A Neo4j graph initialized: university_kg_db'