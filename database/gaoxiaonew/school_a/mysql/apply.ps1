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

$schemaFile = Join-Path $PSScriptRoot 'school_a_mysql.sql'
$sampleFile = Join-Path $PSScriptRoot 'school_a_mysql_sample_data.sql'
if (-not (Test-Path $schemaFile)) {
  throw 'School A MySQL schema file not found.'
}
if (-not (Test-Path $sampleFile)) {
  throw 'School A MySQL sample data file not found.'
}

$scriptText = (Get-Content -Raw -Encoding UTF8 $schemaFile) + "`n" + (Get-Content -Raw -Encoding UTF8 $sampleFile)
$scriptText | & $mysqlCmd.Source --host=$DbHost --port=$Port --user=$User --password=$Password --default-character-set=utf8mb4
if ($LASTEXITCODE -ne 0) {
  throw 'School A MySQL schema/sample initialization failed.'
}

$verifyMap = @(
  @{ Db = 'university_base_db'; Table = 'university_profile'; Expected = 1 },
  @{ Db = 'university_base_db'; Table = 'college_department'; Expected = 2 },
  @{ Db = 'university_base_db'; Table = 'discipline_info'; Expected = 1 },
  @{ Db = 'faculty_hr_db'; Table = 'teacher'; Expected = 4 },
  @{ Db = 'faculty_hr_db'; Table = 'teacher_research_summary'; Expected = 2 },
  @{ Db = 'faculty_hr_db'; Table = 'teacher_title_history'; Expected = 1 },
  @{ Db = 'teaching_affairs_db'; Table = 'major_catalog'; Expected = 2 },
  @{ Db = 'teaching_affairs_db'; Table = 'course_info'; Expected = 3 },
  @{ Db = 'teaching_affairs_db'; Table = 'course_offering'; Expected = 2 },
  @{ Db = 'student_training_db'; Table = 'student_record'; Expected = 4 },
  @{ Db = 'student_training_db'; Table = 'course_grade'; Expected = 3 },
  @{ Db = 'student_training_db'; Table = 'degree_graduation'; Expected = 1 },
  @{ Db = 'admission_employment_db'; Table = 'admission_plan'; Expected = 1 },
  @{ Db = 'admission_employment_db'; Table = 'graduate_employment'; Expected = 1 }
)

foreach ($item in $verifyMap) {
  $countSql = "USE $($item.Db); SELECT COUNT(*) FROM $($item.Table);"
  $countOutput = $countSql | & $mysqlCmd.Source --host=$DbHost --port=$Port --user=$User --password=$Password --default-character-set=utf8mb4 --batch --skip-column-names
  if ($LASTEXITCODE -ne 0) {
    throw "School A MySQL verification query failed for table: $($item.Db).$($item.Table)"
  }

  $countValue = 0
  $text = ($countOutput | Out-String).Trim()
  if (-not [int]::TryParse($text, [ref]$countValue)) {
    throw "School A MySQL verification parse failed for table: $($item.Db).$($item.Table)"
  }
  if ($countValue -ne [int]$item.Expected) {
    throw "School A MySQL sample count mismatch: $($item.Db).$($item.Table), expected=$($item.Expected), actual=$countValue"
  }
}

Write-Host 'School A MySQL databases initialized: university_base_db, faculty_hr_db, teaching_affairs_db, student_training_db, admission_employment_db'
