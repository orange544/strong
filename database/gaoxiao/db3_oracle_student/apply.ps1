param(
  [string]$AdminConnection = 'system/Oracle123!@127.0.0.1:1521/FREEPDB1',
  [string]$SchemaUser = 'GAOXIAO_DB3',
  [string]$SchemaPassword = 'GaoxiaoDb3_123',
  [string]$ConnectTarget = ''
)

$candidates = @()
$sqlplusCmd = Get-Command sqlplus -ErrorAction SilentlyContinue
if ($sqlplusCmd) {
  $candidates += $sqlplusCmd.Source
}
$candidates += 'D:\Program Files\Oracle\instantclient_23_0\sqlplus.exe'
$candidates += 'C:\Program Files\Oracle\instantclient_23_0\sqlplus.exe'
$sqlplus = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $sqlplus) {
  throw 'sqlplus not found.'
}

$schemaFile = Join-Path $PSScriptRoot 'db3_oracle.sql'
if (-not (Test-Path $schemaFile)) {
  throw 'DB3 Oracle schema file not found.'
}

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom
$env:NLS_LANG = 'AMERICAN_AMERICA.AL32UTF8'

if ([string]::IsNullOrWhiteSpace($ConnectTarget)) {
  if ($AdminConnection -notmatch '@') {
    throw 'Invalid AdminConnection format. Example: system/Oracle123!@127.0.0.1:1521/FREEPDB1'
  }
  $ConnectTarget = $AdminConnection.Split('@', 2)[1]
  $ConnectTarget = $ConnectTarget -replace '\s+as\s+sysdba\s*$', ''
}
$userConnection = "$SchemaUser/$SchemaPassword@$ConnectTarget"

$bootstrapSql = Join-Path $env:TEMP ("db3_bootstrap_" + [guid]::NewGuid().ToString('N') + '.sql')
$verifySql = Join-Path $env:TEMP ("db3_verify_" + [guid]::NewGuid().ToString('N') + '.sql')

@"
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET FEEDBACK OFF
DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = UPPER('$SchemaUser');
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER $SchemaUser IDENTIFIED BY $SchemaPassword';
  END IF;
END;
/
GRANT CONNECT, RESOURCE TO $SchemaUser;
GRANT UNLIMITED TABLESPACE TO $SchemaUser;
EXIT
"@ | Set-Content -Path $bootstrapSql -Encoding ASCII

@"
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
SELECT COUNT(*) FROM student_basic;
SELECT COUNT(*) FROM student_status;
SELECT COUNT(*) FROM major_info;
SELECT COUNT(*) FROM class_info;
SELECT COUNT(*) FROM training_program;
SELECT COUNT(*) FROM program_course_require;
SELECT COUNT(*) FROM registration_record;
SELECT COUNT(*) FROM student_status_change;
SELECT COUNT(*) FROM graduation_audit;
EXIT
"@ | Set-Content -Path $verifySql -Encoding ASCII

try {
  & $sqlplus -L -S $AdminConnection "@$bootstrapSql"
  if ($LASTEXITCODE -ne 0) {
    throw 'DB3 Oracle bootstrap failed.'
  }

  & $sqlplus -L -S $userConnection "@$schemaFile"
  if ($LASTEXITCODE -ne 0) {
    throw 'DB3 Oracle schema/sample initialization failed.'
  }

  $verifyOutput = & $sqlplus -L -S $userConnection "@$verifySql"
  if ($LASTEXITCODE -ne 0) {
    throw 'DB3 Oracle verification query failed.'
  }

  $parsed = New-Object System.Collections.Generic.List[int]
  foreach ($line in ($verifyOutput | Where-Object { $_ -and $_.Trim() -ne '' })) {
    $v = 0
    if ([int]::TryParse($line.Trim(), [ref]$v)) {
      $parsed.Add($v)
    }
  }

  if ($parsed.Count -lt 9) {
    throw 'DB3 Oracle verification result is incomplete.'
  }
  foreach ($v in $parsed) {
    if ($v -lt 1) {
      throw 'DB3 Oracle has table without sample data.'
    }
  }
}
finally {
  Remove-Item $bootstrapSql -ErrorAction SilentlyContinue
  Remove-Item $verifySql -ErrorAction SilentlyContinue
}

Write-Host "DB3 Oracle schema and sample data initialized: $SchemaUser"

