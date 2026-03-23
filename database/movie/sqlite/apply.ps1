$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
  throw 'python not found in PATH.'
}

$scriptFile = Join-Path $PSScriptRoot 'build_sqlite.py'
& $pythonCmd.Source $scriptFile
if ($LASTEXITCODE -ne 0) {
  throw 'SQLite initialization failed.'
}

