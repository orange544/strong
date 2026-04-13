param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 5432,
  [string]$User = 'postgres',
  [string]$Password = '123456',
  [string]$Database = 'wuliu_t1_postgresql_db'
)

$candidates = @()
$psqlCmd = Get-Command psql -ErrorAction SilentlyContinue
if ($psqlCmd) {
  $candidates += $psqlCmd.Source
}
$candidates += 'D:\Program Files\PostgreSQL\18\bin\psql.exe'
$candidates += 'C:\Program Files\PostgreSQL\18\bin\psql.exe'
$psql = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $psql) {
  throw 'psql client not found.'
}

$schemaFile = Join-Path $PSScriptRoot 't1_postgresql.sql'
$sampleFile = Join-Path $PSScriptRoot 't1_postgresql_sample_data.sql'
if (-not (Test-Path $sampleFile)) {
  throw 'T1 PostgreSQL sample data file not found.'
}

$env:PGPASSWORD = $Password
$env:PGCLIENTENCODING = 'UTF8'
try {
  $exists = & $psql -h $DbHost -p $Port -U $User -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$Database';"
  if (($exists | Out-String).Trim() -ne '1') {
    & $psql -h $DbHost -p $Port -U $User -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE $Database;"
    if ($LASTEXITCODE -ne 0) {
      throw 'Failed to create PostgreSQL database.'
    }
  }

  & $psql -h $DbHost -p $Port -U $User -d $Database -v ON_ERROR_STOP=1 -f $schemaFile
  if ($LASTEXITCODE -ne 0) {
    throw 'Failed to apply T1 PostgreSQL schema.'
  }

  & $psql -h $DbHost -p $Port -U $User -d $Database -v ON_ERROR_STOP=1 -f $sampleFile
  if ($LASTEXITCODE -ne 0) {
    throw 'Failed to apply T1 PostgreSQL sample data.'
  }
}
finally {
  Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
  Remove-Item Env:PGCLIENTENCODING -ErrorAction SilentlyContinue
}

Write-Host "T1 PostgreSQL schema and sample data initialized: $Database"
