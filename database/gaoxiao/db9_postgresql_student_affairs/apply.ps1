param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 5432,
  [string]$User = 'postgres',
  [string]$Password = '123456',
  [string]$Database = 'gaoxiao_db9_student_affairs'
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

$schemaFile = Join-Path $PSScriptRoot 'db9_postgresql.sql'
$sampleFile = Join-Path $PSScriptRoot 'db9_postgresql_sample_data.sql'
if (-not (Test-Path $schemaFile)) {
  throw 'DB9 PostgreSQL schema file not found.'
}
if (-not (Test-Path $sampleFile)) {
  throw 'DB9 PostgreSQL sample data file not found.'
}

$env:PGPASSWORD = $Password
$env:PGCLIENTENCODING = 'UTF8'
try {
  $exists = & $psql -h $DbHost -p $Port -U $User -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$Database';"
  if (($exists | Out-String).Trim() -ne '1') {
    & $psql -h $DbHost -p $Port -U $User -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $Database;"
    if ($LASTEXITCODE -ne 0) {
      throw 'Failed to create DB9 PostgreSQL database.'
    }
  }

  & $psql -h $DbHost -p $Port -U $User -d $Database -v ON_ERROR_STOP=1 -f $schemaFile
  if ($LASTEXITCODE -ne 0) {
    throw 'Failed to apply DB9 PostgreSQL schema.'
  }

  & $psql -h $DbHost -p $Port -U $User -d $Database -v ON_ERROR_STOP=1 -f $sampleFile
  if ($LASTEXITCODE -ne 0) {
    throw 'Failed to apply DB9 PostgreSQL sample data.'
  }

  $tables = @(
    'student_profile',
    'student_affair',
    'disciplinary_record',
    'leave_application',
    'psychological_warning',
    'difficulty_identification',
    'counselor_binding'
  )

  foreach ($table in $tables) {
    $countText = & $psql -h $DbHost -p $Port -U $User -d $Database -tAc "SELECT COUNT(*) FROM $table;"
    if ($LASTEXITCODE -ne 0) {
      throw "DB9 PostgreSQL verification query failed for table: $table"
    }

    $countValue = 0
    $raw = ($countText | Out-String).Trim()
    if (-not [int]::TryParse($raw, [ref]$countValue)) {
      throw "DB9 PostgreSQL verification parse failed for table: $table"
    }
    if ($countValue -lt 1) {
      throw "DB9 PostgreSQL table has no sample data: $table"
    }
  }
}
finally {
  Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
  Remove-Item Env:PGCLIENTENCODING -ErrorAction SilentlyContinue
}

Write-Host "DB9 PostgreSQL schema and sample data initialized: $Database"
