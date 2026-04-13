param(
  [string]$AdminConnection = 'system/Oracle123!@127.0.0.1:1521/FREEPDB1',
  [string]$SchemaUser = 'GAOXIAO_DB12',
  [string]$SchemaPassword = 'GaoxiaoDb12_123'
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

$schemaFile = Join-Path $PSScriptRoot 'db12_oracle.sql'
if (-not (Test-Path $schemaFile)) {
  throw 'DB12 Oracle schema file not found.'
}

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom
$env:NLS_LANG = 'AMERICAN_AMERICA.AL32UTF8'

if ($AdminConnection -notmatch '@') {
  throw 'Invalid AdminConnection format. Example: system/Oracle123!@127.0.0.1:1521/FREEPDB1'
}
$connectTarget = $AdminConnection.Split('@', 2)[1]
$userConnection = "$SchemaUser/$SchemaPassword@$connectTarget"

$bootstrapSql = Join-Path $env:TEMP ("db12_bootstrap_" + [guid]::NewGuid().ToString('N') + '.sql')
$verifySql = Join-Path $env:TEMP ("db12_verify_" + [guid]::NewGuid().ToString('N') + '.sql')

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
SELECT COUNT(*) FROM campus_card_account;
SELECT COUNT(*) FROM card_holder_map;
SELECT COUNT(*) FROM card_recharge;
SELECT COUNT(*) FROM card_transaction;
SELECT COUNT(*) FROM utility_payment;
SELECT COUNT(*) FROM card_loss_reissue;
EXIT
"@ | Set-Content -Path $verifySql -Encoding ASCII

try {
  & $sqlplus -L -S $AdminConnection "@$bootstrapSql"
  if ($LASTEXITCODE -ne 0) {
    throw 'DB12 Oracle bootstrap failed.'
  }

  & $sqlplus -L -S $userConnection "@$schemaFile"
  if ($LASTEXITCODE -ne 0) {
    throw 'DB12 Oracle schema/sample initialization failed.'
  }

  $verifyOutput = & $sqlplus -L -S $userConnection "@$verifySql"
  if ($LASTEXITCODE -ne 0) {
    throw 'DB12 Oracle verification query failed.'
  }

  $parsed = New-Object System.Collections.Generic.List[int]
  foreach ($line in ($verifyOutput | Where-Object { $_ -and $_.Trim() -ne '' })) {
    $v = 0
    if ([int]::TryParse($line.Trim(), [ref]$v)) {
      $parsed.Add($v)
    }
  }

  if ($parsed.Count -lt 6) {
    throw 'DB12 Oracle verification result is incomplete.'
  }
  foreach ($v in $parsed) {
    if ($v -lt 1) {
      throw 'DB12 Oracle has table without sample data.'
    }
  }
}
finally {
  Remove-Item $bootstrapSql -ErrorAction SilentlyContinue
  Remove-Item $verifySql -ErrorAction SilentlyContinue
}

Write-Host "DB12 Oracle schema and sample data initialized: $SchemaUser"

