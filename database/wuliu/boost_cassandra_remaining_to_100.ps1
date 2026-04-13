param(
  [string]$CqlHost = '127.0.0.1',
  [int]$Port = 9042
)

$ErrorActionPreference = 'Stop'

$cql = (Get-Command cqlsh -ErrorAction SilentlyContinue).Source
if (-not $cql) {
  $cql = 'D:\Program Files\Apache\apache-cassandra-5.0.6\bin\cqlsh.cmd'
}
if (-not (Test-Path $cql)) {
  throw 'cqlsh not found'
}

function Get-CqlCount([string]$TableName) {
  $out = & $cql $CqlHost $Port -e "SELECT count(*) FROM wuliu_t1_cassandra_ks.$TableName;"
  if ($LASTEXITCODE -ne 0) {
    throw "count failed: $TableName"
  }
  $m = [regex]::Matches(($out -join "`n"), '(?m)^\s*(\d+)\s*$')
  if ($m.Count -eq 0) {
    throw "cannot parse count: $TableName"
  }
  return [int]$m[$m.Count - 1].Groups[1].Value
}

$tmp = [System.IO.Path]::GetTempFileName()
$sb = New-Object System.Text.StringBuilder
[void]$sb.AppendLine('USE wuliu_t1_cassandra_ks;')

$scheduleIds = @('SI_T1_20260326_01', 'SI_T1_20260326_03', 'SI_T1_20260326_04', 'SI_T1_20260327_01')
$eventNodes = @('NODE_SZ01', 'NODE_SHHUB', 'NODE_HZ02', 'NODE_TJHOSP', 'NODE_G5_K887')
$eventTypes = @('LOAD_SCAN', 'ARRIVAL_SCAN', 'SIGN_SYNC', 'EXCEPTION_ALERT')
$eventRemarks = @(
  'manifest checked, waiting departure',
  'arrived at hub and queued for sorting',
  'electronic proof of delivery synced',
  'traffic control raised delay warning'
)
$sensorDevices = @('DEV_CL_0001', 'DEV_CL_0005', 'DEV_CL_0008')
$sensorTypes = @('COLD_CHAIN_TEMP_HUM', 'CABIN_ENV')
$vehicleIds = @('VI_T1_0001', 'VI_T1_0003', 'VI_T1_0004', 'VI_T1_0006')

$missing = 100 - (Get-CqlCount 'arrival_departure_event')
if ($missing -gt 0) {
  $base = [datetime]::Parse('2026-03-30 08:00:00')
  for ($i = 1; $i -le $missing; $i++) {
    $t = $base.AddMinutes($i).ToString('yyyy-MM-ddTHH:mm:ss+0800')
    $scheduleId = $scheduleIds[($i - 1) % $scheduleIds.Count]
    $nodeId = $eventNodes[($i - 1) % $eventNodes.Count]
    $eventType = $eventTypes[($i - 1) % $eventTypes.Count]
    $remark = $eventRemarks[($i - 1) % $eventRemarks.Count]
    [void]$sb.AppendLine("INSERT INTO arrival_departure_event (schedule_id, event_time, node_id, event_type, remark) VALUES ('$scheduleId', '$t', '$nodeId', '$eventType', '$remark');")
  }
}

$missing = 100 - (Get-CqlCount 'route_eta_series')
if ($missing -gt 0) {
  $base = [datetime]::Parse('2026-03-30 09:00:00')
  for ($i = 1; $i -le $missing; $i++) {
    $t = $base.AddMinutes($i).ToString('yyyy-MM-ddTHH:mm:ss+0800')
    $scheduleId = $scheduleIds[($i - 1) % $scheduleIds.Count]
    $delay = ($i * 3) % 120
    $p = $base.AddMinutes(150 + ($i % 90) + $delay).ToString('yyyy-MM-ddTHH:mm:ss+0800')
    [void]$sb.AppendLine("INSERT INTO route_eta_series (schedule_id, record_time, predicted_arrival_time, delay_minutes) VALUES ('$scheduleId', '$t', '$p', $delay);")
  }
}

$missing = 100 - (Get-CqlCount 'sensor_record_by_trip')
if ($missing -gt 0) {
  $base = [datetime]::Parse('2026-03-30 10:00:00')
  $culture = [System.Globalization.CultureInfo]::InvariantCulture
  for ($i = 1; $i -le $missing; $i++) {
    $t = $base.AddMinutes($i).ToString('yyyy-MM-ddTHH:mm:ss+0800')
    $scheduleId = $scheduleIds[($i - 1) % $scheduleIds.Count]
    $deviceId = $sensorDevices[($i - 1) % $sensorDevices.Count]
    $sensorType = $sensorTypes[($i - 1) % $sensorTypes.Count]
    $tempValue = 3.5 + ($i % 18) * 0.35
    $humValue = 48.0 + ($i % 28) * 0.7
    $temp = $tempValue.ToString('0.0', $culture)
    $hum = $humValue.ToString('0.0', $culture)
    $alarm = if (($tempValue -gt 8.5) -or ($humValue -gt 64.0)) { 'Y' } else { 'N' }
    [void]$sb.AppendLine("INSERT INTO sensor_record_by_trip (schedule_id, record_time, temperature_value, humidity_value, device_id, sensor_type, alarm_flag) VALUES ('$scheduleId', '$t', $temp, $hum, '$deviceId', '$sensorType', '$alarm');")
  }
}

$missing = 100 - (Get-CqlCount 'vehicle_heartbeat')
if ($missing -gt 0) {
  $base = [datetime]::Parse('2026-03-30 11:00:00')
  for ($i = 1; $i -le $missing; $i++) {
    $t = $base.AddMinutes($i).ToString('yyyy-MM-ddTHH:mm:ss+0800')
    $vehicleId = $vehicleIds[($i - 1) % $vehicleIds.Count]
    $battery = 45 + ($i % 55)
    $engine = @('RUNNING', 'IDLE', 'STOPPED')[$i % 3]
    $device = if ($i % 18 -eq 0) { 'ALERT' } elseif ($i % 25 -eq 0) { 'MAINTENANCE' } else { 'NORMAL' }
    $network = if ($i % 9 -eq 0) { 'WEAK_SIGNAL' } else { 'ONLINE' }
    $gps = if ($i % 30 -eq 0) { 'OFFLINE' } else { 'ONLINE' }
    [void]$sb.AppendLine("INSERT INTO vehicle_heartbeat (vehicle_id, heartbeat_time, engine_status, device_status, battery_level, network_status, gps_status) VALUES ('$vehicleId', '$t', '$engine', '$device', $battery, '$network', '$gps');")
  }
}

Set-Content -Path $tmp -Value $sb.ToString() -Encoding UTF8
& $cql $CqlHost $Port -f $tmp
$code = $LASTEXITCODE
Remove-Item $tmp -Force -ErrorAction SilentlyContinue
if ($code -ne 0) {
  throw 'cqlsh apply failed'
}

Write-Host 'Cassandra remaining tables boosted to target.'
