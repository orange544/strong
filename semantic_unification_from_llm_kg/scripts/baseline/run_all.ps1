[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Assert-PythonAvailable {
    if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
        throw "python command not found in PATH. Please install Python 3.12+ or update PATH."
    }
}

function Redact-LogFile {
    param(
        [Parameter(Mandatory = $true)][string]$InputPath,
        [Parameter(Mandatory = $true)][string]$OutputPath
    )

    if (-not (Test-Path -Path $InputPath -PathType Leaf)) {
        Set-Content -Path $OutputPath -Value "[baseline] no raw log captured." -Encoding UTF8
        return
    }

    $content = Get-Content -Path $InputPath -Raw -Encoding UTF8

    $secretKeys = @(
        "LLM_API_KEY",
        "LLM_DESC_API_KEY",
        "LLM_UNIFY_API_KEY",
        "CHAIN_RECEIVER_ADDRESS",
        "CHAIN_RPC_ADDR",
        "IPFS_API_URL",
        "CHAIN_IPFS_API"
    )

    foreach ($key in $secretKeys) {
        $value = [Environment]::GetEnvironmentVariable($key)
        if ([string]::IsNullOrWhiteSpace($value)) {
            continue
        }
        $escaped = [Regex]::Escape($value)
        $content = [Regex]::Replace($content, $escaped, "[REDACTED:$key]")
    }

    $patterns = @(
        "(?im)(api[_-]?key\s*[:=]\s*)(\S+)",
        "(?im)(authorization\s*[:=]\s*bearer\s+)(\S+)",
        "(?im)(token\s*[:=]\s*)(\S+)"
    )

    foreach ($pattern in $patterns) {
        $content = [Regex]::Replace($content, $pattern, '$1[REDACTED]')
    }

    Set-Content -Path $OutputPath -Value $content -Encoding UTF8
}

Assert-PythonAvailable

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\..")).Path
$logDir = Join-Path $repoRoot "outputs\baseline_logs"
New-Item -Path $logDir -ItemType Directory -Force | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$rawLogPath = Join-Path $logDir "all_raw_$timestamp.log"
$safeLogPath = Join-Path $logDir "all_$timestamp.log"
$exitCode = 1

Push-Location $repoRoot
try {
    $args = @("-u", "main.py", "--mode", "all")
    Write-Host "[baseline] running full pipeline..."
    Write-Host "[baseline] command: python $($args -join ' ')"
    & python @args 2>&1 | Tee-Object -FilePath $rawLogPath
    $exitCode = $LASTEXITCODE
}
catch {
    $_ | Out-String | Set-Content -Path $rawLogPath -Encoding UTF8
    $exitCode = 1
}
finally {
    Pop-Location
}

Redact-LogFile -InputPath $rawLogPath -OutputPath $safeLogPath
Remove-Item -Path $rawLogPath -Force -ErrorAction SilentlyContinue

Write-Host "[baseline] sanitized log: $safeLogPath"
if ($exitCode -ne 0) {
    Write-Error "[baseline] full pipeline failed. ExitCode=$exitCode"
}

exit $exitCode
