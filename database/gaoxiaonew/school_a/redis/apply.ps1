param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 6379,
  [string]$Password = '123456',
  [int]$DbIndex = 1
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

$scriptFile = Join-Path $PSScriptRoot 'school_a_redis_sample_data.txt'
$loaderScript = Join-Path $PSScriptRoot '..\..\common\build_redis.py'
if (-not (Test-Path $scriptFile)) {
  throw 'School A Redis sample data file not found.'
}
if (-not (Test-Path $loaderScript)) {
  throw 'Shared Redis loader script not found.'
}

$redisCli = $redisCliCmd.Source
$redisArgs = @('-h', $DbHost, '-p', "$Port", '-n', "$DbIndex")
if ($Password) {
  $redisArgs += @('--no-auth-warning')
  $redisArgs += @('-a', $Password)
}

$ping = & $redisCli @redisArgs 'PING'
if ($LASTEXITCODE -ne 0 -or (($ping | Out-String).Trim() -notmatch 'PONG')) {
  throw 'School A Redis ping failed.'
}

& $pythonCmd.Source $loaderScript --host $DbHost --port $Port --password $Password --script $scriptFile
if ($LASTEXITCODE -ne 0) {
  throw 'School A Redis sample initialization failed.'
}

$sampleKeys = @(
  'fieldmap:admission_employment_db.graduate_employment.destination_type'
)

foreach ($k in $sampleKeys) {
  $exists = & $redisCli @redisArgs '--raw' 'EXISTS' $k
  if ($LASTEXITCODE -ne 0) {
    throw "School A Redis EXISTS query failed for key: $k"
  }

  $existsVal = 0
  if (-not [int]::TryParse((($exists | Out-String).Trim()), [ref]$existsVal) -or $existsVal -lt 1) {
    throw "School A Redis key missing: $k"
  }
}

$value = (& $redisCli @redisArgs '--raw' 'GET' 'fieldmap:admission_employment_db.graduate_employment.destination_type' | Out-String).Trim()
if ($value -ne '毕业去向类型') {
  throw "School A Redis value mismatch for key fieldmap:admission_employment_db.graduate_employment.destination_type, actual=$value"
}

Write-Host "School A Redis keyspace initialized: db$DbIndex"
