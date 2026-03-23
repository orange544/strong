param(
  [string]$Connection = ''
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
  throw 'sqlplus not found. Install Oracle client/server first.'
}
if ([string]::IsNullOrWhiteSpace($Connection)) {
  throw 'Please provide -Connection, for example: user/password@localhost:1521/XEPDB1'
}

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom
$env:NLS_LANG = 'AMERICAN_AMERICA.AL32UTF8'

$sqlFile = Join-Path $PSScriptRoot 'movie_oracle.sql'
Get-Content -Raw -Encoding UTF8 $sqlFile | & $sqlplus $Connection
if ($LASTEXITCODE -ne 0) {
  throw 'Oracle schema/sample initialization failed.'
}

Write-Host 'Oracle schema and sample data initialized by movie_oracle.sql'
