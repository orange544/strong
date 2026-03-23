param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 9042,
  [string]$User = '',
  [string]$Password = ''
)

$candidates = @()
$cqlshCmd = Get-Command cqlsh -ErrorAction SilentlyContinue
if ($cqlshCmd) {
  $candidates += $cqlshCmd.Source
}
$candidates += 'D:\Program Files\Apache\apache-cassandra-5.0.6\bin\cqlsh.cmd'
$candidates += 'D:\Program Files\Apache\apache-cassandra-5.0.6\bin\cqlsh.bat'
$cqlsh = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $cqlsh) {
  throw 'cqlsh not found.'
}

$cqlFile = Join-Path $PSScriptRoot 'movie_cassandra.cql'
$args = @($DbHost, $Port, '-f', $cqlFile)
if ($User -and $Password) {
  $args = @($DbHost, $Port, '-u', $User, '-p', $Password, '-f', $cqlFile)
}

& $cqlsh @args
if ($LASTEXITCODE -ne 0) {
  throw 'Cassandra initialization failed.'
}

Write-Host 'Cassandra schema initialized: movie_cassandra_ks'


