param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 3306,
  [string]$User = 'root',
  [string]$Password = '123456'
)

$mysqlCmd = Get-Command mysql -ErrorAction SilentlyContinue
if (-not $mysqlCmd) {
  throw 'mysql client not found in PATH.'
}

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$schemaFile = Join-Path $PSScriptRoot 'db1_mysql.sql'
$sampleFile = Join-Path $PSScriptRoot 'db1_mysql_sample_data.sql'
if (-not (Test-Path $schemaFile)) {
  throw 'DB1 MySQL schema file not found.'
}
if (-not (Test-Path $sampleFile)) {
  throw 'DB1 MySQL sample data file not found.'
}

$scriptText = (Get-Content -Raw -Encoding UTF8 $schemaFile) + "`n" + (Get-Content -Raw -Encoding UTF8 $sampleFile)
$scriptText | & $mysqlCmd.Source --host=$DbHost --port=$Port --user=$User --password=$Password --default-character-set=utf8mb4
if ($LASTEXITCODE -ne 0) {
  throw 'DB1 MySQL schema/sample initialization failed.'
}

$tables = @(
  'candidate',
  'application_form',
  'admission_result',
  'major_plan',
  'province_batch',
  'freshman_report_task'
)

foreach ($table in $tables) {
  $countSql = "USE gaoxiao_db1_admission; SELECT COUNT(*) FROM $table;"
  $countOutput = $countSql | & $mysqlCmd.Source --host=$DbHost --port=$Port --user=$User --password=$Password --default-character-set=utf8mb4 --batch --skip-column-names
  if ($LASTEXITCODE -ne 0) {
    throw "DB1 MySQL verification query failed for table: $table"
  }

  $countValue = 0
  $text = ($countOutput | Out-String).Trim()
  if (-not [int]::TryParse($text, [ref]$countValue)) {
    throw "DB1 MySQL verification parse failed for table: $table"
  }
  if ($countValue -lt 1) {
    throw "DB1 MySQL table has no sample data: $table"
  }
}

Write-Host 'DB1 MySQL schema and sample data initialized: gaoxiao_db1_admission'
