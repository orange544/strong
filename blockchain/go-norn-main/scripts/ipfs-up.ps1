$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot "deploy\ipfs\docker-compose.yml"

if (-not (Test-Path $composeFile)) {
    throw "compose file not found: $composeFile"
}

Write-Host "Starting IPFS with compose file: $composeFile"
docker compose -f $composeFile up -d
docker compose -f $composeFile ps

Write-Host "IPFS API: http://127.0.0.1:5001"
Write-Host "IPFS Gateway: http://127.0.0.1:8080"
