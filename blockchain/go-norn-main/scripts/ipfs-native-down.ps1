$ErrorActionPreference = "Stop"

$procs = Get-Process -Name ipfs -ErrorAction SilentlyContinue
if (-not $procs) {
    Write-Host "No ipfs daemon process found."
    exit 0
}

$procs | Stop-Process -Force
Write-Host "IPFS daemon stopped."
