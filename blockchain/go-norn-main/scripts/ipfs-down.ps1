$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot "deploy\ipfs\docker-compose.yml"

if (-not (Test-Path $composeFile)) {
    throw "compose file not found: $composeFile"
}

Write-Host "Stopping IPFS with compose file: $composeFile"
docker compose -f $composeFile down
