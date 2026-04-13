param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 6379,
  [string]$Password = '123456',
  [int]$DbIndex = 13
)

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
  throw 'python not found in PATH.'
}

$redisCliCmd = Get-Command redis-cli -ErrorAction SilentlyContinue
if (-not $redisCliCmd) {
  throw 'redis-cli not found in PATH.'
}

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$scriptFile = Join-Path $PSScriptRoot 'db13_redis_sample_data.txt'
$loaderScript = Join-Path $PSScriptRoot 'build_redis.py'
if (-not (Test-Path $scriptFile)) {
  throw 'DB13 Redis sample data file not found.'
}
if (-not (Test-Path $loaderScript)) {
  throw 'DB13 Redis loader script not found.'
}

$redisCli = $redisCliCmd.Source
$redisArgs = @('-h', $DbHost, '-p', "$Port", '-n', "$DbIndex")
if ($Password) {
  $redisArgs += @('-a', $Password)
}

$ping = & $redisCli @redisArgs 'PING'
if ($LASTEXITCODE -ne 0 -or (($ping | Out-String).Trim() -notmatch 'PONG')) {
  throw 'DB13 Redis ping failed.'
}

& $pythonCmd.Source $loaderScript --host $DbHost --port $Port --password $Password --script $scriptFile
if ($LASTEXITCODE -ne 0) {
  throw 'DB13 Redis sample initialization failed.'
}

$patterns = @(
  'door:*:status',
  'door:*:alarm',
  'person:*:last_access',
  'blacklist:*'
)

foreach ($pattern in $patterns) {
  $keys = & $redisCli @redisArgs '--raw' 'KEYS' $pattern
  if ($LASTEXITCODE -ne 0) {
    throw "DB13 Redis verification query failed for pattern: $pattern"
  }

  $count = @($keys | Where-Object { $_ -and $_.Trim() -ne '' }).Count
  if ($count -lt 3) {
    throw "DB13 Redis sample keys not enough for pattern: $pattern"
  }
}

$sampleKeys = @(
  'door:ACC_DEV_001:status',
  'door:ACC_DEV_002:alarm',
  'person:STU_2025_0001:last_access',
  'blacklist:STU_2025_9001'
)

foreach ($k in $sampleKeys) {
  $exists = & $redisCli @redisArgs '--raw' 'EXISTS' $k
  if ($LASTEXITCODE -ne 0) {
    throw "DB13 Redis EXISTS query failed for key: $k"
  }

  $existsVal = 0
  if (-not [int]::TryParse((($exists | Out-String).Trim()), [ref]$existsVal) -or $existsVal -lt 1) {
    throw "DB13 Redis key missing: $k"
  }
}

Write-Host "DB13 Redis keys and sample data initialized: db$DbIndex"
