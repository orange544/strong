param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 5432,
  [string]$User = 'postgres',
  [string]$Password = '123456',
  [string]$Database = 'research_output_db'
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

$schemaFile = Join-Path $PSScriptRoot 'school_a_postgresql.sql'
$sampleFile = Join-Path $PSScriptRoot 'school_a_postgresql_sample_data.sql'
if (-not (Test-Path $schemaFile)) {
  throw 'School A PostgreSQL schema file not found.'
}
if (-not (Test-Path $sampleFile)) {
  throw 'School A PostgreSQL sample data file not found.'
}

$env:PGPASSWORD = $Password
$env:PGCLIENTENCODING = 'UTF8'
try {
  $exists = & $psql -h $DbHost -p $Port -U $User -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$Database';"
  if (($exists | Out-String).Trim() -ne '1') {
    & $psql -h $DbHost -p $Port -U $User -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $Database;"
    if ($LASTEXITCODE -ne 0) {
      throw 'Failed to create School A PostgreSQL database.'
    }
  }

  & $psql -h $DbHost -p $Port -U $User -d $Database -v ON_ERROR_STOP=1 -f $schemaFile
  if ($LASTEXITCODE -ne 0) {
    throw 'Failed to apply School A PostgreSQL schema.'
  }

  & $psql -h $DbHost -p $Port -U $User -d $Database -v ON_ERROR_STOP=1 -f $sampleFile
  if ($LASTEXITCODE -ne 0) {
    throw 'Failed to apply School A PostgreSQL sample data.'
  }

  $tables = @(
    @{ Name = 'research_project'; Expected = 2 },
    @{ Name = 'paper_output'; Expected = 2 },
    @{ Name = 'patent_record'; Expected = 1 }
  )
  foreach ($table in $tables) {
    $countText = & $psql -h $DbHost -p $Port -U $User -d $Database -tAc "SELECT COUNT(*) FROM $($table.Name);"
    if ($LASTEXITCODE -ne 0) {
      throw "School A PostgreSQL verification query failed for table: $($table.Name)"
    }

    $countValue = 0
    $raw = ($countText | Out-String).Trim()
    if (-not [int]::TryParse($raw, [ref]$countValue)) {
      throw "School A PostgreSQL verification parse failed for table: $($table.Name)"
    }
    if ($countValue -ne [int]$table.Expected) {
      throw "School A PostgreSQL sample count mismatch: $($table.Name), expected=$($table.Expected), actual=$countValue"
    }
  }
}
finally {
  Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
  Remove-Item Env:PGCLIENTENCODING -ErrorAction SilentlyContinue
}

Write-Host "School A PostgreSQL database initialized: $Database"
