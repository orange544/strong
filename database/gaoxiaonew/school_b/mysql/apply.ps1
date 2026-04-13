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

$schemaFile = Join-Path $PSScriptRoot 'school_b_mysql.sql'
$sampleFile = Join-Path $PSScriptRoot 'school_b_mysql_sample_data.sql'
if (-not (Test-Path $schemaFile)) {
  throw 'School B MySQL schema file not found.'
}
if (-not (Test-Path $sampleFile)) {
  throw 'School B MySQL sample data file not found.'
}

$scriptText = (Get-Content -Raw -Encoding UTF8 $schemaFile) + "`n" + (Get-Content -Raw -Encoding UTF8 $sampleFile)
$scriptText | & $mysqlCmd.Source --host=$DbHost --port=$Port --user=$User --password=$Password --default-character-set=utf8mb4
if ($LASTEXITCODE -ne 0) {
  throw 'School B MySQL schema/sample initialization failed.'
}

$verifyMap = @(
  @{ Db = 'base_info_db'; Table = 'institution_master'; Expected = 1 },
  @{ Db = 'base_info_db'; Table = 'org_unit'; Expected = 2 },
  @{ Db = 'base_info_db'; Table = 'subject_catalog'; Expected = 1 },
  @{ Db = 'teaching_affairs_db'; Table = 'program_info'; Expected = 2 },
  @{ Db = 'teaching_affairs_db'; Table = 'curriculum_course'; Expected = 3 },
  @{ Db = 'teaching_affairs_db'; Table = 'class_schedule_record'; Expected = 2 },
  @{ Db = 'student_training_db'; Table = 'learner_profile'; Expected = 4 },
  @{ Db = 'student_training_db'; Table = 'student_score'; Expected = 3 },
  @{ Db = 'student_training_db'; Table = 'graduate_degree_info'; Expected = 1 },
  @{ Db = 'admission_employment_db'; Table = 'enrollment_quota'; Expected = 1 },
  @{ Db = 'admission_employment_db'; Table = 'career_outcome'; Expected = 1 }
)

foreach ($item in $verifyMap) {
  $countSql = "USE $($item.Db); SELECT COUNT(*) FROM $($item.Table);"
  $countOutput = $countSql | & $mysqlCmd.Source --host=$DbHost --port=$Port --user=$User --password=$Password --default-character-set=utf8mb4 --batch --skip-column-names
  if ($LASTEXITCODE -ne 0) {
    throw "School B MySQL verification query failed for table: $($item.Db).$($item.Table)"
  }

  $countValue = 0
  $text = ($countOutput | Out-String).Trim()
  if (-not [int]::TryParse($text, [ref]$countValue)) {
    throw "School B MySQL verification parse failed for table: $($item.Db).$($item.Table)"
  }
  if ($countValue -ne [int]$item.Expected) {
    throw "School B MySQL sample count mismatch: $($item.Db).$($item.Table), expected=$($item.Expected), actual=$countValue"
  }
}

Write-Host 'School B MySQL databases initialized: base_info_db, teaching_affairs_db, student_training_db, admission_employment_db'
