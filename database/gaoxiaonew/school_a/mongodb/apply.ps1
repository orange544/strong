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

$schemaFile = Join-Path $PSScriptRoot 'school_a_mongodb.js'
$sampleFile = Join-Path $PSScriptRoot 'school_a_mongodb_sample_data.js'
if (-not (Test-Path $schemaFile)) {
  throw 'School A MongoDB schema file not found.'
}
if (-not (Test-Path $sampleFile)) {
  throw 'School A MongoDB sample data file not found.'
}

function Invoke-MongoScript([string]$ScriptFile) {
  $args = @('--host', $DbHost, '--port', "$Port", '--file', $ScriptFile)
  if ($User -and $Password) {
    $args = @('--host', $DbHost, '--port', "$Port", '--username', $User, '--password', $Password, '--authenticationDatabase', $AuthDb, '--file', $ScriptFile)
  }

  & $mongosh @args
  if ($LASTEXITCODE -ne 0) {
    throw "School A MongoDB script execution failed: $ScriptFile"
  }
}

Invoke-MongoScript -ScriptFile $schemaFile
Invoke-MongoScript -ScriptFile $sampleFile

$verifyJs = "const dbRef = db.getSiblingDB('university_document_db'); print(dbRef.teaching_materials.countDocuments({_id:'TM_CSE3001_2025FALL'}) + dbRef.teaching_materials.countDocuments({_id:'TM_CSE3002_2025FALL'})); print(dbRef.student_growth_archive.countDocuments({_id:'SGA_202100234'}) + dbRef.student_growth_archive.countDocuments({_id:'SGA_202100235'}));"
$verifyArgs = @('--host', $DbHost, '--port', "$Port", '--quiet', '--eval', $verifyJs)
if ($User -and $Password) {
  $verifyArgs = @('--host', $DbHost, '--port', "$Port", '--username', $User, '--password', $Password, '--authenticationDatabase', $AuthDb, '--quiet', '--eval', $verifyJs)
}

$verifyOut = & $mongosh @verifyArgs
if ($LASTEXITCODE -ne 0) {
  throw 'School A MongoDB verification failed.'
}

$nums = @($verifyOut | ForEach-Object { ($_ | Out-String).Trim() } | Where-Object { $_ -match '^\d+$' })
if ($nums.Count -lt 2 -or [int]$nums[0] -lt 2 -or [int]$nums[1] -lt 2) {
  throw 'School A MongoDB collection count verification failed.'
}

Write-Host 'School A MongoDB database initialized: university_document_db'
