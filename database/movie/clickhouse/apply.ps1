param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 9000,
  [string]$User = 'default',
  [string]$Password = ''
)

$candidates = @()
$clickhouseCmd = Get-Command clickhouse-client -ErrorAction SilentlyContinue
if ($clickhouseCmd) {
  $candidates += $clickhouseCmd.Source
}
$candidates += 'D:\Program Files\ClickHouse\Client\clickhouse-client.cmd'
$candidates += 'C:\Program Files\ClickHouse\Client\clickhouse-client.exe'
$clickhouse = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $clickhouse) {
  throw 'clickhouse-client not found.'
}

$sqlFile = Join-Path $PSScriptRoot 'movie_clickhouse.sql'
function Invoke-ClickHouse([string]$UseUser, [string]$UsePassword, [bool]$Quiet = $false) {
  $args = @('--host', $DbHost, '--port', "$Port", '--user', $UseUser, '--multiquery', '--queries-file', $sqlFile)
  if ($UsePassword) {
    $args += @('--password', $UsePassword)
  }
  if ($Quiet) {
    & $clickhouse @args *> $null
  } else {
    & $clickhouse @args
  }
  return $LASTEXITCODE
}

$code = Invoke-ClickHouse -UseUser $User -UsePassword $Password -Quiet $true
if ($code -ne 0 -and ($User -ne 'admin' -or $Password -ne 'Click123!')) {
  $code = Invoke-ClickHouse -UseUser 'admin' -UsePassword 'Click123!' -Quiet $true
}
if ($code -ne 0 -and $Password) {
  $code = Invoke-ClickHouse -UseUser $User -UsePassword '' -Quiet $false
}

if ($code -ne 0) {
  throw 'ClickHouse initialization failed.'
}

Write-Host 'ClickHouse schema initialized: movie_clickhouse_db'


