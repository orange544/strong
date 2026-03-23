$candidates = @()
$hbaseCmd = Get-Command hbase -ErrorAction SilentlyContinue
if ($hbaseCmd) {
  $candidates += $hbaseCmd.Source
}
$candidates += 'D:\Program Files\Apache\hbase-2.6.4\bin\hbase.cmd'
$candidates += 'D:\Program Files\Apache\hbase-2.5.13\bin\hbase.cmd'
$hbase = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $hbase) {
  throw 'hbase command not found.'
}

$hqlFile = Join-Path $PSScriptRoot 'movie_hbase.hql'
$hbaseHome = Split-Path (Split-Path $hbase -Parent) -Parent
$thirdparty = Join-Path $hbaseHome 'lib\client-facing-thirdparty\*'
$oldClasspath = $env:HBASE_CLASSPATH
if (Test-Path (Join-Path $hbaseHome 'lib\client-facing-thirdparty')) {
  if ($oldClasspath) {
    $env:HBASE_CLASSPATH = "$thirdparty;$oldClasspath"
  } else {
    $env:HBASE_CLASSPATH = $thirdparty
  }
}
$oldShellOpts = $env:HBASE_SHELL_OPTS
$addOpens = '--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED'
if ($oldShellOpts) {
  $env:HBASE_SHELL_OPTS = "$oldShellOpts $addOpens"
} else {
  $env:HBASE_SHELL_OPTS = $addOpens
}

try {
  $output = Get-Content -Raw -Encoding UTF8 $hqlFile | & $hbase shell 2>&1 | Out-String
  Write-Host $output
}
finally {
  if ($null -ne $oldClasspath) {
    $env:HBASE_CLASSPATH = $oldClasspath
  } else {
    Remove-Item Env:HBASE_CLASSPATH -ErrorAction SilentlyContinue
  }
  if ($null -ne $oldShellOpts) {
    $env:HBASE_SHELL_OPTS = $oldShellOpts
  } else {
    Remove-Item Env:HBASE_SHELL_OPTS -ErrorAction SilentlyContinue
  }
}

if ($LASTEXITCODE -ne 0 -or $output -match 'ERROR:' -or $output -match 'KeeperErrorCode') {
  throw 'HBase initialization failed.'
}

Write-Host 'HBase tables initialized under namespace movie_ns.'
