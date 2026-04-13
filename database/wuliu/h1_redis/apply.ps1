param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 6379,
  [string]$Password = '123456'
)

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
  throw 'python not found in PATH.'
}

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$scriptFile = Join-Path $PSScriptRoot 'h1_redis_sample_data.txt'
if (-not (Test-Path $scriptFile)) {
  throw 'H1 Redis sample data file not found.'
}

$loaderScript = Join-Path $PSScriptRoot 'build_redis.py'
if (-not (Test-Path $loaderScript)) {
  throw 'Redis loader script not found.'
}

& $pythonCmd.Source $loaderScript --host $DbHost --port $Port --password $Password --script $scriptFile
if ($LASTEXITCODE -ne 0) {
  throw 'H1 Redis sample initialization failed.'
}

Write-Host 'H1 Redis cache initialized: wuliu_h1_redis (db0)'
