param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 5432,
  [string]$User = 'postgres',
  [string]$Password = '123456',
  [string]$FacultyResearchDatabase = 'faculty_research_db',
  [string]$ResearchOutputDatabase = 'research_output_db'
)

$candidates = @()
$psqlCmd = Get-Command psql -ErrorAction SilentlyContinue
if ($psqlCmd) {
  $candidates += $psqlCmd.Source
}
$candidates += 'D:\Program Files\PostgreSQL\18\bin\psql.exe'
$candidates += 'C:\Program Files\PostgreSQL\18\bin\psql.exe'
$candidates += 'D:\Program Files\PostgreSQL\17\bin\psql.exe'
$candidates += 'C:\Program Files\PostgreSQL\17\bin\psql.exe'
$candidates += 'D:\Program Files\PostgreSQL\16\bin\psql.exe'
$candidates += 'C:\Program Files\PostgreSQL\16\bin\psql.exe'
$psql = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $psql) {
  throw 'psql client not found.'
}

$facultySchemaFile = Join-Path $PSScriptRoot 'school_b_faculty_research_postgresql.sql'
$facultySampleFile = Join-Path $PSScriptRoot 'school_b_faculty_research_postgresql_sample_data.sql'
$researchSchemaFile = Join-Path $PSScriptRoot 'school_b_postgresql.sql'
$researchSampleFile = Join-Path $PSScriptRoot 'school_b_postgresql_sample_data.sql'

$requiredFiles = @($facultySchemaFile, $facultySampleFile, $researchSchemaFile, $researchSampleFile)
foreach ($f in $requiredFiles) {
  if (-not (Test-Path $f)) {
    throw "School B PostgreSQL file not found: $f"
  }
}

function Ensure-Database {
  param([string]$DatabaseName)

  $exists = & $psql -h $DbHost -p $Port -U $User -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$DatabaseName';"
  if (($exists | Out-String).Trim() -ne '1') {
    & $psql -h $DbHost -p $Port -U $User -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $DatabaseName;"
    if ($LASTEXITCODE -ne 0) {
      throw "Failed to create School B PostgreSQL database: $DatabaseName"
    }
  }
}

function Invoke-SqlFile {
  param(
    [string]$DatabaseName,
    [string]$FilePath,
    [string]$Hint
  )

  & $psql -h $DbHost -p $Port -U $User -d $DatabaseName -v ON_ERROR_STOP=1 -f $FilePath
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to apply School B PostgreSQL $Hint on database: $DatabaseName"
  }
}

function Assert-TableHasRows {
  param(
    [string]$DatabaseName,
    [string]$TableName,
    [int]$ExpectedCount
  )

  $countText = & $psql -h $DbHost -p $Port -U $User -d $DatabaseName -tAc "SELECT COUNT(*) FROM $TableName;"
  if ($LASTEXITCODE -ne 0) {
    throw "School B PostgreSQL verification query failed: $DatabaseName.$TableName"
  }

  $countValue = 0
  $raw = ($countText | Out-String).Trim()
  if (-not [int]::TryParse($raw, [ref]$countValue)) {
    throw "School B PostgreSQL verification parse failed: $DatabaseName.$TableName"
  }
  if ($countValue -ne $ExpectedCount) {
    throw "School B PostgreSQL sample count mismatch: $DatabaseName.$TableName, expected=$ExpectedCount, actual=$countValue"
  }
}

$env:PGPASSWORD = $Password
$env:PGCLIENTENCODING = 'UTF8'
try {
  Ensure-Database -DatabaseName $FacultyResearchDatabase
  Ensure-Database -DatabaseName $ResearchOutputDatabase

  Invoke-SqlFile -DatabaseName $FacultyResearchDatabase -FilePath $facultySchemaFile -Hint 'faculty schema'
  Invoke-SqlFile -DatabaseName $FacultyResearchDatabase -FilePath $facultySampleFile -Hint 'faculty sample data'

  Invoke-SqlFile -DatabaseName $ResearchOutputDatabase -FilePath $researchSchemaFile -Hint 'research schema'
  Invoke-SqlFile -DatabaseName $ResearchOutputDatabase -FilePath $researchSampleFile -Hint 'research sample data'

  $facultyTables = @(
    @{ Name = 'faculty_member'; Expected = 4 },
    @{ Name = 'faculty_output_metric'; Expected = 2 },
    @{ Name = 'faculty_rank_record'; Expected = 1 }
  )
  foreach ($table in $facultyTables) {
    Assert-TableHasRows -DatabaseName $FacultyResearchDatabase -TableName $table.Name -ExpectedCount $table.Expected
  }

  $researchTables = @(
    @{ Name = 'grant_award'; Expected = 2 },
    @{ Name = 'publication_record'; Expected = 2 },
    @{ Name = 'ip_asset'; Expected = 1 }
  )
  foreach ($table in $researchTables) {
    Assert-TableHasRows -DatabaseName $ResearchOutputDatabase -TableName $table.Name -ExpectedCount $table.Expected
  }
}
finally {
  Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
  Remove-Item Env:PGCLIENTENCODING -ErrorAction SilentlyContinue
}

Write-Host "School B PostgreSQL databases initialized: $FacultyResearchDatabase, $ResearchOutputDatabase"
