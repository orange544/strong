param(
  [int]$TargetCount = 100,

  [string]$MySqlHost = '127.0.0.1',
  [int]$MySqlPort = 3306,
  [string]$MySqlUser = 'root',
  [string]$MySqlPassword = '123456',

  [string]$PgHost = '127.0.0.1',
  [int]$PgPort = 5432,
  [string]$PgUser = 'postgres',
  [string]$PgPassword = '123456',

  [string]$ClickHouseHost = '127.0.0.1',
  [int]$ClickHousePort = 9000,
  [string]$ClickHouseUser = 'admin',
  [string]$ClickHousePassword = 'Click123!',

  [string]$MongoHost = '127.0.0.1',
  [int]$MongoPort = 27017,

  [string]$CassandraHost = '127.0.0.1',
  [int]$CassandraPort = 9042,

  [string]$RedisHost = '127.0.0.1',
  [int]$RedisPort = 6379,
  [string]$RedisPassword = '123456'
)

Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false
$ErrorActionPreference = 'Stop'

function Resolve-CommandPath {
  param(
    [string[]]$Names,
    [string[]]$Fallbacks = @()
  )

  foreach ($name in $Names) {
    $cmd = Get-Command $name -ErrorAction SilentlyContinue
    if ($cmd) {
      return $cmd.Source
    }
  }

  foreach ($path in $Fallbacks) {
    if (Test-Path $path) {
      return $path
    }
  }

  throw "Command not found. Names: $($Names -join ', ')"
}

function Parse-LastInteger {
  param([string[]]$Lines)
  $text = $Lines -join "`n"
  $matches = [regex]::Matches($text, '(?m)^\s*(\d+)\s*$')
  if ($matches.Count -eq 0) {
    throw "Unable to parse integer from output: $text"
  }
  return [int]$matches[$matches.Count - 1].Groups[1].Value
}

$mysql = Resolve-CommandPath -Names @('mysql')
$psql = Resolve-CommandPath -Names @('psql') -Fallbacks @(
  'D:\Program Files\PostgreSQL\18\bin\psql.exe',
  'C:\Program Files\PostgreSQL\18\bin\psql.exe'
)
$clickhouse = Resolve-CommandPath -Names @('clickhouse-client') -Fallbacks @(
  'D:\Program Files\ClickHouse\Client\clickhouse-client.cmd',
  'C:\Program Files\ClickHouse\Client\clickhouse-client.exe'
)
$mongosh = Resolve-CommandPath -Names @('mongosh') -Fallbacks @(
  'D:\Programs\mongosh\mongosh.exe',
  'C:\Program Files\MongoDB\mongosh\bin\mongosh.exe'
)
$cqlsh = Resolve-CommandPath -Names @('cqlsh') -Fallbacks @(
  'D:\Program Files\Apache\apache-cassandra-5.0.6\bin\cqlsh.cmd',
  'D:\Program Files\Apache\apache-cassandra-5.0.6\bin\cqlsh.bat'
)
$redis = Resolve-CommandPath -Names @('redis-cli') -Fallbacks @(
  'D:\Program Files\Redis\redis-cli.exe'
)
$python = Resolve-CommandPath -Names @('python')

function Invoke-MySqlQuery {
  param([string]$Query)
  $out = & $script:mysql `
    --host=$script:MySqlHost `
    --port=$script:MySqlPort `
    --user=$script:MySqlUser `
    --default-character-set=utf8mb4 `
    --batch `
    --skip-column-names `
    -e $Query
  if ($LASTEXITCODE -ne 0) {
    throw 'MySQL command failed.'
  }
  return $out
}

function Get-MySqlCount {
  param(
    [string]$DatabaseName,
    [string]$TableName
  )
  $q = "SELECT COUNT(*) FROM $DatabaseName.$TableName;"
  return Parse-LastInteger -Lines (Invoke-MySqlQuery -Query $q)
}

function Invoke-PgQuery {
  param(
    [string]$DatabaseName,
    [string]$Query
  )
  $null = $Query | & $script:psql `
    -h $script:PgHost `
    -p $script:PgPort `
    -U $script:PgUser `
    -d $DatabaseName `
    -v ON_ERROR_STOP=1 `
    -f -
  if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL command failed for database: $DatabaseName"
  }
}

function Get-PgCount {
  param(
    [string]$DatabaseName,
    [string]$Query
  )
  $out = & $script:psql `
    -h $script:PgHost `
    -p $script:PgPort `
    -U $script:PgUser `
    -d $DatabaseName `
    -tAc $Query
  if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL count query failed for database: $DatabaseName"
  }
  return Parse-LastInteger -Lines $out
}

function Invoke-ClickHouseQuery {
  param([string]$Query)
  $tmpSql = [System.IO.Path]::GetTempFileName()
  Set-Content -Path $tmpSql -Value $Query -Encoding UTF8

  $attempts = @(
    @{ User = $script:ClickHouseUser; Password = $script:ClickHousePassword },
    @{ User = 'admin'; Password = 'Click123!' },
    @{ User = 'default'; Password = '' },
    @{ User = $script:ClickHouseUser; Password = '' }
  )

  $seen = @{}
  $lastError = ''
  try {
    foreach ($attempt in $attempts) {
      $key = "$($attempt.User)|$($attempt.Password)"
      if ($seen.ContainsKey($key)) {
        continue
      }
      $seen[$key] = $true

      $args = @(
        '--host', $script:ClickHouseHost,
        '--port', "$script:ClickHousePort",
        '--user', $attempt.User,
        '--multiquery',
        '--queries-file', $tmpSql
      )
      if ($attempt.Password) {
        $args += @('--password', $attempt.Password)
      }

      $out = $null
      $code = 1
      try {
        $out = & $script:clickhouse @args 2>$null
        $code = $LASTEXITCODE
      }
      catch {
        $code = 1
      }

      if ($code -eq 0) {
        return $out
      }
      $lastError = "exit_code=$code user=$($attempt.User)"
    }
  }
  finally {
    Remove-Item -Path $tmpSql -Force -ErrorAction SilentlyContinue
  }

  throw "ClickHouse command failed. Last error: $lastError"
}

function Get-ClickHouseCount {
  param([string]$TableName)
  $q = "SELECT count() FROM $TableName FORMAT TabSeparated"
  return Parse-LastInteger -Lines (Invoke-ClickHouseQuery -Query $q)
}

function Invoke-MongoEval {
  param([string]$ScriptText)
  $out = & $script:mongosh --host $script:MongoHost --port $script:MongoPort --quiet --eval $ScriptText
  if ($LASTEXITCODE -ne 0) {
    throw 'MongoDB command failed.'
  }
  return $out
}

function Get-MongoCount {
  param(
    [string]$DatabaseName,
    [string]$CollectionName
  )
  $js = "const d=db.getSiblingDB('$DatabaseName'); print(d.getCollection('$CollectionName').countDocuments({}));"
  return Parse-LastInteger -Lines (Invoke-MongoEval -ScriptText $js)
}

function Get-CassandraCount {
  param([string]$TableName)
  $out = & $script:cqlsh $script:CassandraHost $script:CassandraPort -e "SELECT count(*) FROM $TableName;"
  if ($LASTEXITCODE -ne 0) {
    throw 'Cassandra count query failed.'
  }
  return Parse-LastInteger -Lines $out
}

function Invoke-CassandraFile {
  param([string]$FilePath)
  & $script:cqlsh $script:CassandraHost $script:CassandraPort -f $FilePath
  if ($LASTEXITCODE -ne 0) {
    throw "Cassandra command failed for file: $FilePath"
  }
}

function Get-RedisLen {
  param([string]$KeyName)
  $out = & $script:redis -h $script:RedisHost -p $script:RedisPort LLEN $KeyName
  if ($LASTEXITCODE -ne 0) {
    throw "Redis LLEN failed for key: $KeyName"
  }
  return Parse-LastInteger -Lines $out
}

Write-Host "Target count per selected table/collection: $TargetCount"

$results = [ordered]@{}

$env:MYSQL_PWD = $MySqlPassword
$env:PGPASSWORD = $PgPassword
$env:REDISCLI_AUTH = $RedisPassword
try {
  # MySQL: m1.sales_order
  $current = Get-MySqlCount -DatabaseName 'wuliu_m1_mysql_db' -TableName 'sales_order'
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $sql = @"
USE wuliu_m1_mysql_db;
INSERT IGNORE INTO sales_order (
  order_id, order_no, customer_id, consignee_id, order_status, order_source,
  order_time, requested_ship_time, requested_delivery_time, priority_level,
  total_amount, total_qty, total_weight_kg, total_volume_m3, payment_status,
  remark
)
SELECT
  CONCAT('ORD_M1_20260330_', LPAD(n, 4, '0')),
  CONCAT('SO-M1-20260330-', LPAD(n, 4, '0')),
  CONCAT('CUS_M1_', LPAD(MOD(n, 6) + 1, 4, '0')),
  ELT(
    MOD(n, 8) + 1,
    'CNEE_SZH_001', 'CNEE_HGH_002', 'CNEE_NJG_003', 'CNEE_HFZ_004',
    'CNEE_NBO_005', 'CNEE_WHU_006', 'CNEE_SZH_007', 'CNEE_SHA_008'
  ),
  ELT(MOD(n, 4) + 1, 'CREATED', 'PROCESSING', 'DONE', 'ABNORMAL'),
  ELT(MOD(n, 3) + 1, 'ERP', 'API', 'OMS'),
  DATE_ADD('2026-03-30 08:00:00', INTERVAL (n * 7) MINUTE),
  DATE_ADD('2026-03-30 13:00:00', INTERVAL (n * 7) MINUTE),
  DATE_ADD('2026-03-31 09:00:00', INTERVAL (n * 9) MINUTE),
  ELT(MOD(n, 3) + 1, 'HIGH', 'MEDIUM', 'LOW'),
  ROUND(42000 + n * 356.70 + MOD(n, 5) * 980, 2),
  12 + MOD(n, 180),
  ROUND(88 + n * 2.35, 3),
  ROUND(0.8600 + n * 0.0320, 4),
  ELT(MOD(n, 4) + 1, 'UNPAID', 'PARTIAL', 'PAID', 'REFUNDED'),
  ELT(
    MOD(n, 5) + 1,
    '医院项目补货，优先安排早班发运',
    '冷链设备混载，需装车复核温控记录',
    '分拨中心晚班到货，需预约卸货窗口',
    '院配签收要求纸质回单和电子签名并行',
    '干线拥堵预警，建议分批出库'
  )
FROM (
  SELECT ones.n + tens.n * 10 + 1 AS n
  FROM
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS ones
  CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS tens
  WHERE ones.n + tens.n * 10 < $missing
) AS seq;
"@
    Invoke-MySqlQuery -Query $sql | Out-Null
  }
  $results['m1.sales_order'] = Get-MySqlCount -DatabaseName 'wuliu_m1_mysql_db' -TableName 'sales_order'

  # MySQL: m2.biz_order
  $current = Get-MySqlCount -DatabaseName 'wuliu_m2_mysql_db' -TableName 'biz_order'
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $sql = @"
USE wuliu_m2_mysql_db;
INSERT IGNORE INTO biz_order (
  biz_order_id, sales_no, client_code, receiver_code, status_cd, source_type,
  order_tm, expect_ship_tm, expect_arrive_tm, priority_cd, total_amt, remark_txt
)
SELECT
  CONCAT('BIZ_M2_20260330_', LPAD(n, 4, '0')),
  CONCAT('SAL-M2-20260330-', LPAD(n, 4, '0')),
  CONCAT('CLT_M2_', LPAD(MOD(n, 5) + 1, 4, '0')),
  ELT(
    MOD(n, 8) + 1,
    'RCV_BJ_001', 'RCV_TJ_002', 'RCV_SJZ_003', 'RCV_TY_004',
    'RCV_BJ_005', 'RCV_TJ_006', 'RCV_TY_007', 'RCV_HHH_008'
  ),
  ELT(MOD(n, 4) + 1, '1', '9', '0', '-1'),
  ELT(MOD(n, 3) + 1, 'DISTRIBUTOR_API', 'ERP', 'OMS'),
  DATE_ADD('2026-03-30 08:30:00', INTERVAL (n * 9) MINUTE),
  DATE_ADD('2026-03-30 12:00:00', INTERVAL (n * 9) MINUTE),
  DATE_ADD('2026-03-31 09:00:00', INTERVAL (n * 11) MINUTE),
  ELT(MOD(n, 3) + 1, 'P1', 'P2', 'P3'),
  ROUND(36000 + n * 512.30 + MOD(n, 6) * 880, 2),
  ELT(
    MOD(n, 5) + 1,
    '京津冀联采补货',
    '冷链院配优先派车',
    '常规耗材周转补库',
    '危化资质待复核，先建单后审',
    '影像设备巡检备件补发'
  )
FROM (
  SELECT ones.n + tens.n * 10 + 1 AS n
  FROM
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS ones
  CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS tens
  WHERE ones.n + tens.n * 10 < $missing
) AS seq;
"@
    Invoke-MySqlQuery -Query $sql | Out-Null
  }
  $results['m2.biz_order'] = Get-MySqlCount -DatabaseName 'wuliu_m2_mysql_db' -TableName 'biz_order'

  # MySQL: w1.inbound_order
  $current = Get-MySqlCount -DatabaseName 'wuliu_w1_mysql_db' -TableName 'inbound_order'
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $sql = @"
USE wuliu_w1_mysql_db;
INSERT IGNORE INTO inbound_order (
  inbound_order_id, inbound_order_no, supplier_name, warehouse_id, inbound_type,
  planned_arrival_time, actual_arrival_time, order_status
)
SELECT
  CONCAT('IBO_W1_20260330_', LPAD(n, 4, '0')),
  CONCAT('IBN-W1-', ELT(MOD(n, 3) + 1, 'SZ', 'SH', 'NJ'), '-', LPAD(n, 4, '0')),
  ELT(
    MOD(n, 6) + 1,
    '华东电子成品中心',
    '华北医械上海中转仓',
    '华东电子冷链供应商',
    '东部仓储返修中心',
    '南京医用耗材集采平台',
    '苏州智联自动化科技有限公司'
  ),
  ELT(MOD(n, 3) + 1, 'W1_WH_SZ01', 'W1_WH_SH01', 'W1_WH_NJ01'),
  ELT(MOD(n, 3) + 1, '采购入库', '调拨入库', '退货入库'),
  DATE_ADD('2026-03-30 09:00:00', INTERVAL (n * 11) MINUTE),
  CASE WHEN MOD(n, 3) = 2 THEN NULL ELSE DATE_ADD('2026-03-30 09:00:00', INTERVAL (n * 11 + 25) MINUTE) END,
  ELT(MOD(n, 3) + 1, 'DONE', 'PROCESSING', 'CREATED')
FROM (
  SELECT ones.n + tens.n * 10 + 1 AS n
  FROM
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS ones
  CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS tens
  WHERE ones.n + tens.n * 10 < $missing
) AS seq;
"@
    Invoke-MySqlQuery -Query $sql | Out-Null
  }
  $results['w1.inbound_order'] = Get-MySqlCount -DatabaseName 'wuliu_w1_mysql_db' -TableName 'inbound_order'

  # MySQL: w2.asn_order
  $current = Get-MySqlCount -DatabaseName 'wuliu_w2_mysql_db' -TableName 'asn_order'
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $sql = @"
USE wuliu_w2_mysql_db;
INSERT IGNORE INTO asn_order (
  asn_no, source_no, wh_id, arrival_plan_tm, arrival_real_tm, doc_status
)
SELECT
  CONCAT('ASN_W2_20260330_', LPAD(n, 4, '0')),
  CONCAT('PO-HZ-202603-', LPAD(MOD(n, 999) + 1, 3, '0')),
  ELT(MOD(n, 3) + 1, 'W2_WH_WUH01', 'W2_WH_ZZ01', 'W2_WH_CQ01'),
  DATE_ADD('2026-03-30 06:00:00', INTERVAL (n * 13) MINUTE),
  CASE WHEN MOD(n, 3) = 2 THEN NULL ELSE DATE_ADD('2026-03-30 06:00:00', INTERVAL (n * 13 + 35) MINUTE) END,
  ELT(MOD(n, 3) + 1, 'FINISHED', 'WORKING', 'INIT_DOC')
FROM (
  SELECT ones.n + tens.n * 10 + 1 AS n
  FROM
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS ones
  CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS tens
  WHERE ones.n + tens.n * 10 < $missing
) AS seq;
"@
    Invoke-MySqlQuery -Query $sql | Out-Null
  }
  $results['w2.asn_order'] = Get-MySqlCount -DatabaseName 'wuliu_w2_mysql_db' -TableName 'asn_order'

  # MySQL: t2.trip_plan
  $current = Get-MySqlCount -DatabaseName 'wuliu_t2_mysql_db' -TableName 'trip_plan'
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $sql = @"
USE wuliu_t2_mysql_db;
INSERT IGNORE INTO trip_plan (
  trip_no, route_code, truck_id, driver_id,
  plan_depart_tm, real_depart_tm, plan_arrive_tm, real_arrive_tm, execute_status
)
SELECT
  CONCAT('TRINO-20260330-', LPAD(n, 3, '0')),
  ELT(
    MOD(n, 6) + 1,
    'ROUCOD-T2-SZ-SH', 'ROUCOD-T2-NJ-HZ', 'ROUCOD-T2-BJ-HOSP',
    'ROUCOD-T2-TJ-HOSP', 'ROUCOD-T2-TJ-TY', 'ROUCOD-T2-SH-NB'
  ),
  CONCAT('TI_T2_', LPAD(MOD(n, 6) + 1, 4, '0')),
  CONCAT('DRC_T2_', LPAD(MOD(n, 6) + 1, 4, '0')),
  DATE_ADD('2026-03-30 06:00:00', INTERVAL (n * 20) MINUTE),
  CASE WHEN MOD(n, 3) = 2 THEN NULL ELSE DATE_ADD('2026-03-30 06:00:00', INTERVAL (n * 20 + 12) MINUTE) END,
  DATE_ADD('2026-03-30 10:30:00', INTERVAL (n * 22) MINUTE),
  CASE WHEN MOD(n, 3) = 1 THEN DATE_ADD('2026-03-30 10:30:00', INTERVAL (n * 22 - 15) MINUTE) ELSE NULL END,
  ELT(MOD(n, 3) + 1, '运输中', '已签收', '待发车')
FROM (
  SELECT ones.n + tens.n * 10 + 1 AS n
  FROM
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS ones
  CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS tens
  WHERE ones.n + tens.n * 10 < $missing
) AS seq;
"@
    Invoke-MySqlQuery -Query $sql | Out-Null
  }
  $results['t2.trip_plan'] = Get-MySqlCount -DatabaseName 'wuliu_t2_mysql_db' -TableName 'trip_plan'

  # PostgreSQL: p1.master_waybill
  $current = Get-PgCount -DatabaseName 'wuliu_p1_postgresql_db' -Query "SELECT COUNT(*) FROM p1.master_waybill;"
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $sql = @"
SET search_path TO p1;
WITH seq AS (
  SELECT generate_series(1, $missing) AS n
)
INSERT INTO master_waybill (
  master_waybill_id, master_waybill_no, order_id, shipper_id,
  origin_node_id, destination_node_id, waybill_status,
  planned_departure_time, planned_arrival_time, actual_departure_time, actual_arrival_time,
  total_package_qty, total_weight_kg, total_volume_m3
)
SELECT
  format('MWID_P1_20260330_%s', lpad(seq.n::text, 4, '0')),
  format('MWB-P1-20260330-%s', lpad(seq.n::text, 4, '0')),
  CASE
    WHEN seq.n % 2 = 0 THEN format('ORD_M1_20260330_%s', lpad((seq.n % 900 + 1)::text, 4, '0'))
    ELSE format('BIZ_M2_20260330_%s', lpad((seq.n % 900 + 1)::text, 4, '0'))
  END,
  CASE WHEN seq.n % 2 = 0 THEN 'SHP_P1_M1_01' ELSE 'SHP_P1_M2_01' END,
  CASE WHEN seq.n % 4 = 0 THEN 'NODE_SZ01' WHEN seq.n % 4 = 1 THEN 'NODE_NJ01' WHEN seq.n % 4 = 2 THEN 'NODE_BJ01' ELSE 'NODE_TJ01' END,
  CASE WHEN seq.n % 4 = 0 THEN 'NODE_SH09' WHEN seq.n % 4 = 1 THEN 'NODE_HZ02' WHEN seq.n % 4 = 2 THEN 'NODE_BJHOSP' ELSE 'NODE_SXTY' END,
  CASE WHEN seq.n % 4 = 0 THEN 'IN_TRANSIT' WHEN seq.n % 4 = 1 THEN 'SIGNED' WHEN seq.n % 4 = 2 THEN 'DISPATCHING' ELSE 'EXCEPTION' END,
  timestamp '2026-03-30 08:00:00' + (seq.n * interval '11 minutes'),
  timestamp '2026-03-31 08:00:00' + (seq.n * interval '13 minutes'),
  timestamp '2026-03-30 08:15:00' + (seq.n * interval '11 minutes'),
  CASE WHEN seq.n % 4 = 1 THEN timestamp '2026-03-31 06:50:00' + (seq.n * interval '13 minutes') ELSE NULL END,
  16 + (seq.n % 72),
  round((120 + seq.n * 3.25)::numeric, 3),
  round((1.20 + seq.n * 0.045)::numeric, 4)
FROM seq
ON CONFLICT DO NOTHING;
"@
    Invoke-PgQuery -DatabaseName 'wuliu_p1_postgresql_db' -Query $sql
  }
  $results['p1.master_waybill'] = Get-PgCount -DatabaseName 'wuliu_p1_postgresql_db' -Query "SELECT COUNT(*) FROM p1.master_waybill;"

  # PostgreSQL: t1.transport_task
  $current = Get-PgCount -DatabaseName 'wuliu_t1_postgresql_db' -Query "SELECT COUNT(*) FROM transport_task;"
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $sql = @"
WITH seq AS (
  SELECT generate_series(1, $missing) AS n
),
seed AS (
  SELECT schedule_id, vehicle_id, driver_id
  FROM departure_schedule
  ORDER BY schedule_id
  LIMIT 1
)
INSERT INTO transport_task (
  task_id, sub_waybill_id, schedule_id, vehicle_id, driver_id,
  task_status, pickup_node_id, delivery_node_id
)
SELECT
  format('TI_T1_%s', lpad((100000 + seq.n)::text, 6, '0')),
  format('SWID_P1_%s', lpad((100000 + seq.n)::text, 6, '0')),
  seed.schedule_id,
  seed.vehicle_id,
  seed.driver_id,
  CASE
    WHEN seq.n % 4 = 0 THEN '执行中'
    WHEN seq.n % 4 = 1 THEN '已完成'
    WHEN seq.n % 4 = 2 THEN '待执行'
    ELSE '异常'
  END,
  CASE
    WHEN seq.n % 4 = 0 THEN 'NODE_SZ01'
    WHEN seq.n % 4 = 1 THEN 'NODE_NJ01'
    WHEN seq.n % 4 = 2 THEN 'NODE_BJ01'
    ELSE 'NODE_TJ01'
  END,
  CASE
    WHEN seq.n % 4 = 0 THEN 'NODE_SHHUB'
    WHEN seq.n % 4 = 1 THEN 'NODE_HZ02'
    WHEN seq.n % 4 = 2 THEN 'NODE_BJHOSP'
    ELSE 'NODE_SXTY'
  END
FROM seq
CROSS JOIN seed
ON CONFLICT DO NOTHING;
"@
    Invoke-PgQuery -DatabaseName 'wuliu_t1_postgresql_db' -Query $sql
  }
  $results['t1.transport_task'] = Get-PgCount -DatabaseName 'wuliu_t1_postgresql_db' -Query "SELECT COUNT(*) FROM transport_task;"

  # PostgreSQL: h1.arrival_scan
  $current = Get-PgCount -DatabaseName 'wuliu_h1_postgresql_db' -Query "SELECT COUNT(*) FROM arrival_scan;"
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $sql = @"
WITH seq AS (
  SELECT generate_series(1, $missing) AS n
)
INSERT INTO arrival_scan (
  scan_id, package_no, waybill_no, station_id, scan_time, scan_type, operator_id, device_id
)
SELECT
  format('AS_H1_20260330_%s', lpad(seq.n::text, 4, '0')),
  format('PACNO-20260330-%s', lpad(seq.n::text, 4, '0')),
  format('MWB-P1-20260330-%s', lpad((seq.n % 400 + 1)::text, 4, '0')),
  CASE WHEN seq.n % 4 = 0 THEN 'H1_ST_SH01' WHEN seq.n % 4 = 1 THEN 'H1_ST_NJ01' WHEN seq.n % 4 = 2 THEN 'H1_ST_BJ01' ELSE 'H1_ST_TJ01' END,
  timestamp '2026-03-30 08:00:00' + (seq.n * interval '45 seconds'),
  '到站扫描',
  CASE
    WHEN seq.n % 4 = 0 THEN format('OP_H1_SH_%s', lpad((seq.n % 6 + 1)::text, 2, '0'))
    WHEN seq.n % 4 = 1 THEN format('OP_H1_NJ_%s', lpad((seq.n % 6 + 1)::text, 2, '0'))
    WHEN seq.n % 4 = 2 THEN format('OP_H1_BJ_%s', lpad((seq.n % 6 + 1)::text, 2, '0'))
    ELSE format('OP_H1_TJ_%s', lpad((seq.n % 6 + 1)::text, 2, '0'))
  END,
  CASE
    WHEN seq.n % 4 = 0 THEN format('DEV_H1_SH_GATE_%s', lpad((seq.n % 3 + 1)::text, 2, '0'))
    WHEN seq.n % 4 = 1 THEN format('DEV_H1_NJ_GATE_%s', lpad((seq.n % 3 + 1)::text, 2, '0'))
    WHEN seq.n % 4 = 2 THEN format('DEV_H1_BJ_GATE_%s', lpad((seq.n % 3 + 1)::text, 2, '0'))
    ELSE format('DEV_H1_TJ_GATE_%s', lpad((seq.n % 3 + 1)::text, 2, '0'))
  END
FROM seq
ON CONFLICT DO NOTHING;
"@
    Invoke-PgQuery -DatabaseName 'wuliu_h1_postgresql_db' -Query $sql
  }
  $results['h1.arrival_scan'] = Get-PgCount -DatabaseName 'wuliu_h1_postgresql_db' -Query "SELECT COUNT(*) FROM arrival_scan;"

  # ClickHouse: w1.scan_event_fact
  $current = Get-ClickHouseCount -TableName 'wuliu_w1_clickhouse_db.scan_event_fact'
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $sql = @"
INSERT INTO wuliu_w1_clickhouse_db.scan_event_fact
SELECT
  concat('SEF_W1_20260330_', toString(number + 1)) AS event_id,
  multiIf(number % 3 = 0, 'W1_WH_SZ01', number % 3 = 1, 'W1_WH_SH01', 'W1_WH_NJ01') AS warehouse_id,
  multiIf(
    number % 5 = 2,
    concat(
      'DOCK_',
      multiIf(number % 3 = 0, 'SZ', number % 3 = 1, 'SH', 'NJ'),
      '_',
      if(number % 4 + 1 < 10, concat('0', toString(number % 4 + 1)), toString(number % 4 + 1))
    ),
    concat(
      'PDA_',
      multiIf(number % 3 = 0, 'SZ', number % 3 = 1, 'SH', 'NJ'),
      '_',
      if(number % 12 + 1 < 10, concat('0', toString(number % 12 + 1)), toString(number % 12 + 1))
    )
  ) AS device_id,
  multiIf(
    number % 8 = 0, 'SKU_M1_0001',
    number % 8 = 1, 'SKU_M1_0003',
    number % 8 = 2, 'GDS_M2_0002',
    number % 8 = 3, 'GDS_M2_0004',
    number % 8 = 4, 'SKU_M1_0005',
    number % 8 = 5, 'SKU_M1_0006',
    number % 8 = 6, 'SKU_M1_0002',
    'SKU_M1_0004'
  ) AS sku_id,
  multiIf(number % 5 = 0, 'INBOUND_SCAN', number % 5 = 1, 'PICK_SCAN', number % 5 = 2, 'OUTBOUND_CONFIRM', number % 5 = 3, 'COUNT_SCAN', 'QUALITY_HOLD_SCAN') AS event_type,
  toDateTime('2026-03-30 08:00:00') + number * 60 AS event_time
FROM numbers($missing);
"@
    Invoke-ClickHouseQuery -Query $sql | Out-Null
  }
  $results['w1.scan_event_fact'] = Get-ClickHouseCount -TableName 'wuliu_w1_clickhouse_db.scan_event_fact'

  # MongoDB: p1.event_log
  $current = Get-MongoCount -DatabaseName 'wuliu_p1_mongodb_db' -CollectionName 'event_log'
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $jsTemplate = @'
const missing = __MISSING__;
const d = db.getSiblingDB('wuliu_p1_mongodb_db');
const base = new Date('2026-03-30T00:00:00Z').getTime();
const docs = [];
for (let i = 1; i <= missing; i += 1) {
  docs.push({
    event_id: `EVT_P1_20260330_${String(i).padStart(4, '0')}`,
    waybill_no: `SWB-P1-${String((i % 600) + 1).padStart(6, '0')}`,
    event_type: ['PICKUP_CONFIRMED', 'DEPARTED_HUB', 'ARRIVED_HUB', 'OUT_FOR_DELIVERY', 'SIGNED'][i % 5],
    event_time: new Date(base + i * 60000),
    operator_id: ['op_sz_wh_01', 'op_sz_hub_03', 'op_sh_hub_07', 'op_nj_cold_02', 'op_hz_station_01'][i % 5],
    node_id: ['NODE_SZ01', 'NODE_SHHUB', 'NODE_NJ01', 'NODE_HZ02', 'NODE_BJ01'][i % 5],
    status_before: ['CREATED', 'PICKED', 'IN_TRANSIT', 'ARRIVED', 'DELIVERING'][i % 5],
    status_after: ['PICKED', 'IN_TRANSIT', 'ARRIVED', 'DELIVERING', 'SIGNED'][i % 5],
    payload: {
      source: ['m1_wms', 'tms_gateway', 'hub_wcs', 'cold_chain_iot', 'd1_pod'][i % 5],
      biz_tag: ['干线起运', '中转出港', '枢纽到站', '末端派送', '签收回传'][i % 5],
      seq: i
    }
  });
}
if (docs.length > 0) {
  d.event_log.bulkWrite(
    docs.map((doc) => ({
      updateOne: {
        filter: { event_id: doc.event_id },
        update: { $setOnInsert: doc },
        upsert: true
      }
    }))
  );
}
print(d.event_log.countDocuments({}));
'@
    $js = $jsTemplate -replace '__MISSING__', [string]$missing
    Invoke-MongoEval -ScriptText $js | Out-Null
  }
  $results['p1.event_log'] = Get-MongoCount -DatabaseName 'wuliu_p1_mongodb_db' -CollectionName 'event_log'

  # MongoDB: d1.delivery_task
  $current = Get-MongoCount -DatabaseName 'wuliu_d1_mongodb_db' -CollectionName 'delivery_task'
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $jsTemplate = @'
const missing = __MISSING__;
const d = db.getSiblingDB('wuliu_d1_mongodb_db');
const base = new Date('2026-03-30T00:00:00Z').getTime();
const docs = [];
for (let i = 1; i <= missing; i += 1) {
  docs.push({
    task_id: `DTK_D1_20260330_${String(i).padStart(4, '0')}`,
    task_no: `D1-TASK-20260330-${String(i).padStart(4, '0')}`,
    waybill_no: `MWB-P1-20260330-${String((i % 500) + 1).padStart(4, '0')}`,
    package_no: `PACNO-20260330-${String(i).padStart(4, '0')}`,
    courier_id: ['CR_D1_SH_01', 'CR_D1_SH_02', 'CR_D1_HZ_01', 'CR_D1_BJ_03', 'CR_D1_TJ_01'][i % 5],
    station_id: ['D1_ST_SH09', 'D1_ST_HZ02', 'D1_ST_BJHOSP', 'D1_ST_TJHOSP'][i % 4],
    delivery_status: ['待派送', '派送中', '已签收', '派送异常'][i % 4],
    dispatch_time: new Date(base + i * 90000),
    planned_sign_time: new Date(base + i * 90000 + 3 * 3600 * 1000),
    actual_sign_time: i % 4 === 2 ? new Date(base + i * 90000 + 2 * 3600 * 1000) : null,
    recipient_name: ['上海九院器械库', '杭州滨江门诊中心', '北京朝阳医院设备科', '天津中心医院器械库', '宁波港区自动化中心'][i % 5],
    recipient_phone: `13918${String(100000 + i).slice(-6)}`,
    delivery_address: [
      '上海市黄浦区制造局路639号器械收货口',
      '浙江省杭州市滨江区江南大道3888号一层收货区',
      '北京市朝阳区工体南路8号器械收货口',
      '天津市和平区南京路78号后勤收货区',
      '浙江省宁波市北仑区港通路66号自动化仓'
    ][i % 5],
    remark: ['手术器械优先签收', '到院后需电话确认开门', '需扫码签收并回传附件', '签收后需上传温度照片', '夜间到港件需提前报备'][i % 5]
  });
}
if (docs.length > 0) {
  d.delivery_task.bulkWrite(
    docs.map((doc) => ({
      updateOne: {
        filter: { task_id: doc.task_id },
        update: { $setOnInsert: doc },
        upsert: true
      }
    }))
  );
}
print(d.delivery_task.countDocuments({}));
'@
    $js = $jsTemplate -replace '__MISSING__', [string]$missing
    Invoke-MongoEval -ScriptText $js | Out-Null
  }
  $results['d1.delivery_task'] = Get-MongoCount -DatabaseName 'wuliu_d1_mongodb_db' -CollectionName 'delivery_task'

  # Cassandra: t1.gps_track_by_vehicle
  $current = Get-CassandraCount -TableName 'wuliu_t1_cassandra_ks.gps_track_by_vehicle'
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $tmpCql = [System.IO.Path]::GetTempFileName()
    try {
      $sb = New-Object System.Text.StringBuilder
      [void]$sb.AppendLine('USE wuliu_t1_cassandra_ks;')
      $baseTime = [datetime]::ParseExact('2026-03-28 08:00:00', 'yyyy-MM-dd HH:mm:ss', $null)
      $culture = [System.Globalization.CultureInfo]::InvariantCulture
      $scheduleIds = @('SI_T1_20260326_01', 'SI_T1_20260326_03', 'SI_T1_20260326_04', 'SI_T1_20260327_01')
      $vehicleIds = @('VI_T1_0001', 'VI_T1_0003', 'VI_T1_0004', 'VI_T1_0001')
      $directionTags = @('离开深圳枢纽', '沪向北段', '接近上海枢纽', '宁杭高速', '进入杭州区域', '北京三环内配送段', '受管制减速')
      for ($i = 1; $i -le $missing; $i++) {
        $ts = $baseTime.AddMinutes($i).ToString('yyyy-MM-ddTHH:mm:ss+0800')
        $lng = (113.9000 + ($i * 0.021)).ToString('0.0000', $culture)
        $lat = (22.6000 + ($i * 0.085)).ToString('0.0000', $culture)
        $speed = 28 + ($i % 55)
        $idx = ($i - 1) % $scheduleIds.Count
        $scheduleId = $scheduleIds[$idx]
        $vehicleId = $vehicleIds[$idx]
        $direction = $directionTags[($i - 1) % $directionTags.Count]
        $line = "INSERT INTO gps_track_by_vehicle (vehicle_id, track_date, track_time, lng, lat, speed_kmh, direction, schedule_id) VALUES ('$vehicleId', '2026-03-28', '$ts', $lng, $lat, $speed, '$direction', '$scheduleId');"
        [void]$sb.AppendLine($line)
      }
      Set-Content -Path $tmpCql -Value $sb.ToString() -Encoding UTF8
      Invoke-CassandraFile -FilePath $tmpCql
    }
    finally {
      Remove-Item -Path $tmpCql -Force -ErrorAction SilentlyContinue
    }
  }
  $results['t1.gps_track_by_vehicle'] = Get-CassandraCount -TableName 'wuliu_t1_cassandra_ks.gps_track_by_vehicle'

  # SQLite: d1.local_task_cache
  $dbFile = Join-Path $PSScriptRoot 'd1_sqlite\d1_sqlite.db'
  $py = @'
import sqlite3
import sys

db_path = sys.argv[1]
target = int(sys.argv[2])
conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM local_task_cache")
current = int(cur.fetchone()[0])
missing = target - current
if missing > 0:
    docs = []
    for i in range(1, missing + 1):
        docs.append((
            f"DTK_D1_20260330_{i:04d}",
            f"D1-TASK-20260330-{i:04d}",
            f"MWB-P1-20260330-{(i % 500) + 1:04d}",
            ["待派送", "派送中", "已签收", "派送异常"][i % 4],
            ["PENDING", "SYNCED", "FAILED"][i % 3]
        ))
    cur.executemany(
        "INSERT OR IGNORE INTO local_task_cache(task_id, task_no, waybill_no, delivery_status, sync_status) VALUES (?, ?, ?, ?, ?)",
        docs
    )
conn.commit()
cur.execute("SELECT COUNT(*) FROM local_task_cache")
print(cur.fetchone()[0])
conn.close()
'@
  $sqliteCountOut = $py | & $python - $dbFile "$TargetCount"
  if ($LASTEXITCODE -ne 0) {
    throw 'SQLite boost failed.'
  }
  $results['d1.local_task_cache'] = Parse-LastInteger -Lines $sqliteCountOut

  # Redis: h1 pending_sort_queue
  $redisKey = 'pending_sort_queue:{H1_ST_SH01}'
  $current = Get-RedisLen -KeyName $redisKey
  $missing = $TargetCount - $current
  if ($missing -gt 0) {
    $redisBase = [datetimeoffset]::Parse('2026-03-30T08:00:00+08:00')
    for ($i = 1; $i -le $missing; $i++) {
      $pkg = ('PACNO-20260330-{0}' -f $i.ToString('0000'))
      $priority = if ($i % 3 -eq 0) { 'L1' } elseif ($i % 3 -eq 1) { 'L2' } else { 'L3' }
      $enqueue = $redisBase.AddSeconds($i * 45).ToString('yyyy-MM-ddTHH:mm:sszzz')
      $payload = "{`"package_no`":`"$pkg`",`"station_id`":`"H1_ST_SH01`",`"priority_level`":`"$priority`",`"enqueue_time`":`"$enqueue`"}"
      & $redis -h $RedisHost -p $RedisPort RPUSH $redisKey $payload | Out-Null
      if ($LASTEXITCODE -ne 0) {
        throw 'Redis RPUSH failed.'
      }
    }
    & $redis -h $RedisHost -p $RedisPort EXPIRE $redisKey 7200 | Out-Null
    if ($LASTEXITCODE -ne 0) {
      throw 'Redis EXPIRE failed.'
    }
  }
  $results['h1.pending_sort_queue'] = Get-RedisLen -KeyName $redisKey
}
finally {
  Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue
  Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
  Remove-Item Env:REDISCLI_AUTH -ErrorAction SilentlyContinue
}

Write-Host ''
Write-Host '=== Boost Result ==='
foreach ($entry in $results.GetEnumerator()) {
  Write-Host ('{0}={1}' -f $entry.Key, $entry.Value)
}

