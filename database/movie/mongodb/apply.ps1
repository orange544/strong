param(
  [string]$DbHost = '127.0.0.1',
  [int]$Port = 27017,
  [string]$User = '',
  [string]$Password = '',
  [string]$AuthDb = 'admin'
)

$candidates = @()
$mongoshCmd = Get-Command mongosh -ErrorAction SilentlyContinue
if ($mongoshCmd) {
  $candidates += $mongoshCmd.Source
}
$candidates += 'D:\Programs\mongosh\mongosh.exe'
$candidates += 'C:\Program Files\MongoDB\mongosh\bin\mongosh.exe'
$mongosh = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $mongosh) {
  throw 'mongosh not found.'
}

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$schemaFile = Join-Path $PSScriptRoot 'movie_mongodb.js'
$sampleFile = Join-Path $PSScriptRoot 'movie_mongodb_sample_data.js'
if (-not (Test-Path $sampleFile)) {
  throw 'MongoDB sample data file not found.'
}

function Invoke-MongoScript([string]$ScriptFile) {
  $args = @('--host', $DbHost, '--port', "$Port", '--file', $ScriptFile)
  if ($User -and $Password) {
    $args = @('--host', $DbHost, '--port', "$Port", '--username', $User, '--password', $Password, '--authenticationDatabase', $AuthDb, '--file', $ScriptFile)
  }

  & $mongosh @args
  if ($LASTEXITCODE -ne 0) {
    throw "MongoDB script execution failed: $ScriptFile"
  }
}

Invoke-MongoScript -ScriptFile $schemaFile
Invoke-MongoScript -ScriptFile $sampleFile

Write-Host 'MongoDB schema and sample data initialized: movie_mongodb_db'
