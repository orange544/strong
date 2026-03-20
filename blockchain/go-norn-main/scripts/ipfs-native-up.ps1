$ErrorActionPreference = "Stop"

$ipfsCmd = Get-Command ipfs -ErrorAction SilentlyContinue

$candidatePaths = @(
    "$env:USERPROFILE\\ipfs.exe",
    "$env:LOCALAPPDATA\\Programs\\IPFS Desktop\\resources\\app.asar.unpacked\\node_modules\\kubo\\kubo\\ipfs.exe"
)

$ipfsExe = $null
if ($ipfsCmd) {
    $ipfsExe = $ipfsCmd.Source
} else {
    foreach ($path in $candidatePaths) {
        if (Test-Path $path) {
            $ipfsExe = $path
            break
        }
    }
}

if (-not $ipfsExe) {
    throw "ipfs executable not found. Install Kubo or IPFS Desktop first."
}

Write-Host "Using IPFS binary: $ipfsExe"

$running = Get-Process -Name ipfs -ErrorAction SilentlyContinue
if (-not $running) {
    Start-Process -FilePath $ipfsExe -ArgumentList "daemon" -WindowStyle Hidden
    Start-Sleep -Seconds 3
}

$version = curl.exe -s -X POST http://127.0.0.1:5001/api/v0/version
if (-not $version) {
    throw "ipfs daemon is not responding at http://127.0.0.1:5001"
}

Write-Host "IPFS daemon is up."
Write-Host "IPFS API: http://127.0.0.1:5001"
Write-Host "IPFS Gateway: http://127.0.0.1:8080"
