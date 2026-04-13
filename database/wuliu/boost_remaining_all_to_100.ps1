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

  [string]$MongoHost = '127.0.0.1',
  [int]$MongoPort = 27017,

  [string]$ClickHouseHost = '127.0.0.1',
  [int]$ClickHousePort = 9000,
  [string]$ClickHouseUser = 'admin',
  [string]$ClickHousePassword = 'Click123!',

  [string]$RedisHost = '127.0.0.1',
  [int]$RedisPort = 6379,
  [string]$RedisPassword = '123456'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

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
$mongosh = Resolve-CommandPath -Names @('mongosh') -Fallbacks @(
  'D:\Programs\mongosh\mongosh.exe',
  'C:\Program Files\MongoDB\mongosh\bin\mongosh.exe'
)
$clickhouse = Resolve-CommandPath -Names @('clickhouse-client') -Fallbacks @(
  'D:\Program Files\ClickHouse\Client\clickhouse-client.cmd',
  'C:\Program Files\ClickHouse\Client\clickhouse-client.exe'
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

function Get-MySqlScalar {
  param([string]$Query)
  return Parse-LastInteger -Lines (Invoke-MySqlQuery -Query $Query)
}

function Get-MySqlExpression {
  param(
    [string]$Column,
    [string]$DataType,
    [int]$CharLen,
    [bool]$IsUnique
  )
  $dtype = $DataType.ToLowerInvariant()
  $base = "b.$Column"
  $safeLen = if ($CharLen -gt 6) { $CharLen - 5 } else { 8 }
  if ($safeLen -lt 1) { $safeLen = 1 }

  $strTypes = @('char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext')
  $numTypes = @('tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint', 'decimal', 'numeric', 'float', 'double')

  if ($IsUnique) {
    if ($strTypes -contains $dtype) {
      return "CONCAT(LEFT(COALESCE($base,'X'), $safeLen), '_', LPAD(seq.n,4,'0'))"
    }
    if ($numTypes -contains $dtype) {
      return "(COALESCE($base,0) + seq.n * 1000)"
    }
    if ($dtype -eq 'date') {
      return "DATE_ADD(COALESCE($base,'2026-01-01'), INTERVAL seq.n DAY)"
    }
    if (($dtype -eq 'datetime') -or ($dtype -eq 'timestamp')) {
      return "DATE_ADD(COALESCE($base,'2026-01-01 00:00:00'), INTERVAL seq.n MINUTE)"
    }
    return $base
  }

  if ($dtype -eq 'date') {
    return "DATE_ADD(COALESCE($base,'2026-01-01'), INTERVAL seq.n DAY)"
  }
  if (($dtype -eq 'datetime') -or ($dtype -eq 'timestamp')) {
    return "DATE_ADD(COALESCE($base,'2026-01-01 00:00:00'), INTERVAL seq.n MINUTE)"
  }
  return $base
}

function Boost-MySqlDatabase {
  param([string]$DbName)
  $tables = Invoke-MySqlQuery -Query "SELECT table_name FROM information_schema.tables WHERE table_schema='$DbName' ORDER BY table_name;"
  foreach ($table in $tables) {
    if ([string]::IsNullOrWhiteSpace($table)) { continue }
    $table = $table.Trim()
    for ($attempt = 1; $attempt -le 6; $attempt++) {
      $count = Get-MySqlScalar -Query "SELECT COUNT(*) FROM $DbName.$table;"
      if ($count -ge $TargetCount) {
        break
      }
      if ($count -le 0) {
        break
      }
      $missing = $TargetCount - $count
      $colRows = Invoke-MySqlQuery -Query @"
SELECT column_name, data_type, COALESCE(character_maximum_length,0)
FROM information_schema.columns
WHERE table_schema='$DbName' AND table_name='$table'
ORDER BY ordinal_position;
"@
      $uniqRows = @(Invoke-MySqlQuery -Query @"
SELECT DISTINCT column_name
FROM information_schema.statistics
WHERE table_schema='$DbName' AND table_name='$table' AND non_unique=0
ORDER BY column_name;
"@)
      $uniqSet = @{}
      foreach ($u in $uniqRows) {
        if (-not [string]::IsNullOrWhiteSpace($u)) {
          $uniqSet[$u.Trim()] = $true
        }
      }
      $columns = New-Object System.Collections.Generic.List[string]
      $exprs = New-Object System.Collections.Generic.List[string]
      foreach ($row in $colRows) {
        if ([string]::IsNullOrWhiteSpace($row)) { continue }
        $parts = $row -split "`t"
        if ($parts.Count -lt 3) { continue }
        $col = $parts[0].Trim()
        $dtype = $parts[1].Trim()
        $len = 0
        [void][int]::TryParse($parts[2].Trim(), [ref]$len)
        $isUnique = $uniqSet.ContainsKey($col)
        $columns.Add($col)
        $exprs.Add((Get-MySqlExpression -Column $col -DataType $dtype -CharLen $len -IsUnique:$isUnique))
      }
      if ($columns.Count -eq 0) { break }

      $colList = $columns -join ', '
      $exprList = $exprs -join ",`n  "
      $orderCol = if (($uniqRows | Measure-Object).Count -gt 0) { $uniqRows[0].Trim() } else { $columns[0] }
      $sql = @"
INSERT IGNORE INTO $DbName.$table ($colList)
SELECT
  $exprList
FROM (
  SELECT ones.n + tens.n * 10 + hundreds.n * 100 + 1 AS n
  FROM
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS ones
  CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS tens
  CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS hundreds
  WHERE ones.n + tens.n * 10 + hundreds.n * 100 < $missing
) AS seq
JOIN $DbName.$table b
  ON 1=0;
"@
      $sql = $sql -replace "JOIN $DbName\.$table b\s+ON 1=0;", @"
JOIN (
  SELECT (@rn := @rn + 1) AS rn, src.*
  FROM (SELECT * FROM $DbName.$table ORDER BY $orderCol) AS src
  JOIN (SELECT @rn := 0) AS init
) AS b
  ON b.rn = ((seq.n - 1) MOD $count) + 1;
"@
      Invoke-MySqlQuery -Query $sql | Out-Null
    }
  }
}

function Invoke-PgScalar {
  param(
    [string]$Database,
    [string]$Query
  )
  $out = & $script:psql -h $script:PgHost -p $script:PgPort -U $script:PgUser -d $Database -tAc $Query
  if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL scalar query failed: $Database"
  }
  return Parse-LastInteger -Lines $out
}

function Invoke-PgQuery {
  param(
    [string]$Database,
    [string]$Query
  )
  $null = $Query | & $script:psql -h $script:PgHost -p $script:PgPort -U $script:PgUser -d $Database -v ON_ERROR_STOP=1 -f -
  if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL command failed: $Database"
  }
}

function Get-PgExpression {
  param(
    [string]$Column,
    [string]$DataType,
    [int]$CharLen,
    [bool]$IsUnique
  )
  $dtype = $DataType.ToLowerInvariant()
  $base = "b.$Column"
  $safeLen = if ($CharLen -gt 6) { $CharLen - 5 } else { 12 }
  if ($safeLen -lt 1) { $safeLen = 1 }

  $strTypes = @('character varying', 'character', 'text')
  $numTypes = @('smallint', 'integer', 'bigint', 'numeric', 'decimal', 'real', 'double precision')

  if ($IsUnique) {
    if ($strTypes -contains $dtype) {
      return "left(coalesce($base::text,'X'), $safeLen) || '_' || lpad(seq.n::text,4,'0')"
    }
    if ($numTypes -contains $dtype) {
      return "(coalesce($base,0) + seq.n * 1000)"
    }
    if ($dtype -eq 'date') {
      return "(coalesce($base::date, date '2026-01-01') + seq.n)"
    }
    if (($dtype -eq 'timestamp without time zone') -or ($dtype -eq 'timestamp with time zone')) {
      return "(coalesce($base, timestamp '2026-01-01 00:00:00') + (seq.n || ' minute')::interval)"
    }
    return $base
  }

  if ($dtype -eq 'date') {
    return "(coalesce($base::date, date '2026-01-01') + seq.n)"
  }
  if (($dtype -eq 'timestamp without time zone') -or ($dtype -eq 'timestamp with time zone')) {
    return "(coalesce($base, timestamp '2026-01-01 00:00:00') + (seq.n || ' minute')::interval)"
  }
  return $base
}

function Boost-PgDatabase {
  param(
    [string]$Database,
    [string]$Schema
  )
  $tabsRaw = & $script:psql -h $script:PgHost -p $script:PgPort -U $script:PgUser -d $Database -tAc "SELECT table_name FROM information_schema.tables WHERE table_schema='$Schema' AND table_type='BASE TABLE' ORDER BY table_name;"
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to query PostgreSQL tables: $Database.$Schema"
  }
  foreach ($t in $tabsRaw) {
    $table = $t.Trim()
    if (-not $table) { continue }

    for ($attempt = 1; $attempt -le 6; $attempt++) {
      $count = Invoke-PgScalar -Database $Database -Query "SELECT COUNT(*) FROM $Schema.$table;"
      if ($count -ge $TargetCount) {
        break
      }
      if ($count -le 0) {
        break
      }
      $missing = $TargetCount - $count

      $colsRaw = & $script:psql -h $script:PgHost -p $script:PgPort -U $script:PgUser -d $Database -tAc @"
SELECT column_name || E'\t' || data_type || E'\t' || COALESCE(character_maximum_length::text,'0')
FROM information_schema.columns
WHERE table_schema='$Schema' AND table_name='$table'
ORDER BY ordinal_position;
"@
      if ($LASTEXITCODE -ne 0) {
        throw "Failed to query PostgreSQL columns: $Database.$Schema.$table"
      }

      $uniqRaw = & $script:psql -h $script:PgHost -p $script:PgPort -U $script:PgUser -d $Database -tAc @"
SELECT DISTINCT a.attname
FROM pg_constraint con
JOIN pg_class c ON c.oid=con.conrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS x(attnum, ord) ON true
JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=x.attnum
WHERE n.nspname='$Schema' AND c.relname='$table' AND con.contype IN ('p','u')
ORDER BY a.attname;
"@
      if ($LASTEXITCODE -ne 0) {
        throw "Failed to query PostgreSQL unique cols: $Database.$Schema.$table"
      }
      $uniqSet = @{}
      foreach ($u in $uniqRaw) {
        $un = $u.Trim()
        if ($un) { $uniqSet[$un] = $true }
      }

      $columns = New-Object System.Collections.Generic.List[string]
      $exprs = New-Object System.Collections.Generic.List[string]
      foreach ($row in $colsRaw) {
        if ([string]::IsNullOrWhiteSpace($row)) { continue }
        $parts = $row -split "`t"
        if ($parts.Count -lt 3) { continue }
        $col = $parts[0].Trim()
        $dtype = $parts[1].Trim()
        $len = 0
        [void][int]::TryParse($parts[2].Trim(), [ref]$len)
        $isUnique = $uniqSet.ContainsKey($col)
        $columns.Add($col)
        $exprs.Add((Get-PgExpression -Column $col -DataType $dtype -CharLen $len -IsUnique:$isUnique))
      }
      if ($columns.Count -eq 0) { break }
      $colList = $columns -join ', '
      $exprList = $exprs -join ",`n  "

      $sql = @"
WITH RECURSIVE seq(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < $missing
),
base AS (
  SELECT *, row_number() OVER (ORDER BY 1) AS rn
  FROM $Schema.$table
),
bc AS (
  SELECT count(*) AS c FROM base
)
INSERT INTO $Schema.$table ($colList)
SELECT
  $exprList
FROM seq
CROSS JOIN bc
JOIN base b ON b.rn = ((seq.n - 1) % bc.c) + 1
ON CONFLICT DO NOTHING;
"@
      Invoke-PgQuery -Database $Database -Query $sql
    }
  }
}

function Invoke-MongoEval {
  param([string]$ScriptText)
  $out = & $script:mongosh --host $script:MongoHost --port $script:MongoPort --quiet --eval $ScriptText
  if ($LASTEXITCODE -ne 0) {
    throw 'MongoDB command failed.'
  }
  return $out
}

function Boost-Mongo {
  $js = @"
const target = $TargetCount;
function mutate(v, key, i) {
  if (v === null || v === undefined) return v;
  if (v instanceof Date) return new Date(v.getTime() + i * 60000);
  if (Array.isArray(v)) return v.map((x, idx) => mutate(x, key + '_' + idx, i));
  if (typeof v === 'object') {
    const o = {};
    for (const k of Object.keys(v)) {
      if (k === '_id') continue;
      o[k] = mutate(v[k], k, i);
    }
    return o;
  }
  if (typeof v === 'number') return v + (i % 11);
  if (typeof v === 'string') {
    if (/phone/i.test(key)) return '13' + String(100000000 + i).slice(-9);
    if (/(id|no|code|task|ticket|batch|manifest|sheet|flow|scan|event|waybill|package|device|operator|retry|work|record|doc|carrier|driver|vehicle|address|image|payload)/i.test(key)) {
      return v + '-' + String(i).padStart(4, '0');
    }
    return v;
  }
  return v;
}
function boostDb(name) {
  const d = db.getSiblingDB(name);
  const cols = d.getCollectionNames().filter((n) => !n.startsWith('system.'));
  for (const c of cols) {
    let cnt = d.getCollection(c).countDocuments({});
    if (cnt <= 0 || cnt >= target) continue;
    let guard = 0;
    while (cnt < target && guard < 6) {
      guard += 1;
      const missing = target - cnt;
      const baseDocs = d.getCollection(c).find({}).limit(Math.min(30, cnt)).toArray();
      if (!baseDocs.length) break;
      const docs = [];
      for (let i = 1; i <= missing; i += 1) {
        const seed = baseDocs[(i - 1) % baseDocs.length];
        const doc = mutate(seed, '', cnt + i);
        delete doc._id;
        docs.push(doc);
      }
      if (docs.length) {
        try {
          d.getCollection(c).insertMany(docs, { ordered: false });
        } catch (e) {}
      }
      cnt = d.getCollection(c).countDocuments({});
    }
    print(name + '.' + c + '=' + cnt);
  }
}
boostDb('wuliu_p1_mongodb_db');
boostDb('wuliu_d1_mongodb_db');
"@
  Invoke-MongoEval -ScriptText $js | Out-Null
}

function Invoke-ClickHouseQuery {
  param([string]$Query)
  $tmpSql = [System.IO.Path]::GetTempFileName()
  Set-Content -Path $tmpSql -Value $Query -Encoding UTF8
  try {
    & $script:clickhouse `
      --host $script:ClickHouseHost `
      --port "$script:ClickHousePort" `
      --user $script:ClickHouseUser `
      --password $script:ClickHousePassword `
      --multiquery `
      --queries-file $tmpSql | Out-Null
    if ($LASTEXITCODE -ne 0) {
      throw 'ClickHouse command failed.'
    }
  }
  finally {
    Remove-Item -Path $tmpSql -Force -ErrorAction SilentlyContinue
  }
}

function Get-ClickHouseCount {
  param([string]$TableName)
  $out = & $script:clickhouse `
    --host $script:ClickHouseHost `
    --port "$script:ClickHousePort" `
    --user $script:ClickHouseUser `
    --password $script:ClickHousePassword `
    --query "SELECT count() FROM $TableName FORMAT TabSeparated"
  if ($LASTEXITCODE -ne 0) {
    throw 'ClickHouse count query failed.'
  }
  return Parse-LastInteger -Lines $out
}

function Boost-ClickHouse {
  $missing = $TargetCount - (Get-ClickHouseCount -TableName 'wuliu_w1_clickhouse_db.inventory_snapshot_fact')
  if ($missing -gt 0) {
    Invoke-ClickHouseQuery -Query @"
INSERT INTO wuliu_w1_clickhouse_db.inventory_snapshot_fact
SELECT
  toDate('2026-04-01') + number % 10 AS snapshot_date,
  multiIf(number % 3 = 0, 'W1_WH_SZ01', number % 3 = 1, 'W1_WH_SH01', 'W1_WH_NJ01') AS warehouse_id,
  multiIf(number % 6 = 0, 'SKU_M1_0001', number % 6 = 1, 'SKU_M1_0003', number % 6 = 2, 'GDS_M2_0002', number % 6 = 3, 'GDS_M2_0004', number % 6 = 4, 'SKU_M1_0005', 'SKU_M1_0006') AS sku_id,
  320 + number * 5 AS available_qty,
  12 + number % 60 AS locked_qty
FROM numbers($missing);
"@
  }

  $missing = $TargetCount - (Get-ClickHouseCount -TableName 'wuliu_w1_clickhouse_db.task_efficiency_fact')
  if ($missing -gt 0) {
    Invoke-ClickHouseQuery -Query @"
INSERT INTO wuliu_w1_clickhouse_db.task_efficiency_fact
SELECT
  concat('PT_W1_E_', toString(number + 1)) AS task_id,
  'PICK' AS task_type,
  concat('PICKER_', if(number % 20 + 1 < 10, concat('00', toString(number % 20 + 1)), concat('0', toString(number % 20 + 1)))) AS operator_id,
  multiIf(number % 3 = 0, 'W1_WH_SZ01', number % 3 = 1, 'W1_WH_SH01', 'W1_WH_NJ01') AS warehouse_id,
  toDateTime('2026-04-01 08:00:00') + number * 120 AS start_time,
  toDateTime('2026-04-01 08:12:00') + number * 120 AS end_time,
  720 + number % 1800 AS duration_sec,
  if(number % 8 = 0, 0, 1) AS success_flag,
  toDate('2026-04-01') + number % 10 AS stat_date
FROM numbers($missing);
"@
  }

  $missing = $TargetCount - (Get-ClickHouseCount -TableName 'wuliu_w1_clickhouse_db.device_log_fact')
  if ($missing -gt 0) {
    Invoke-ClickHouseQuery -Query @"
INSERT INTO wuliu_w1_clickhouse_db.device_log_fact
SELECT
  concat('LOG_W1_E_', toString(number + 1)) AS log_id,
  multiIf(number % 3 = 0, concat('PDA_SZ_', if(number % 12 + 1 < 10, concat('0', toString(number % 12 + 1)), toString(number % 12 + 1))),
          number % 3 = 1, concat('PDA_SH_', if(number % 12 + 1 < 10, concat('0', toString(number % 12 + 1)), toString(number % 12 + 1))),
          concat('PDA_NJ_', if(number % 12 + 1 < 10, concat('0', toString(number % 12 + 1)), toString(number % 12 + 1)))) AS device_id,
  multiIf(number % 3 = 0, 'W1_WH_SZ01', number % 3 = 1, 'W1_WH_SH01', 'W1_WH_NJ01') AS warehouse_id,
  multiIf(number % 2 = 0, 'PDA', 'DOCK') AS device_type,
  multiIf(number % 5 = 0, 'WARN', number % 5 = 1, 'INFO', number % 5 = 2, 'ERROR', number % 5 = 3, 'INFO', 'DEBUG') AS log_level,
  multiIf(number % 4 = 0, 'SORTING_THROUGHPUT_FLUCT', number % 4 = 1, 'DEVICE_HEARTBEAT_OK', number % 4 = 2, 'SCAN_RETRY_SUCCESS', 'NETWORK_RECOVERED') AS log_message,
  toDateTime('2026-04-01 09:00:00') + number * 45 AS log_time,
  concat('10.10.', toString(number % 30 + 1), '.', toString(number % 200 + 1)) AS source_ip,
  multiIf(number % 4 = 0, 'DEGRADED', number % 4 = 1, 'NORMAL', number % 4 = 2, 'RECOVERED', 'NORMAL') AS status
FROM numbers($missing);
"@
  }

  $missing = $TargetCount - (Get-ClickHouseCount -TableName 'wuliu_w1_clickhouse_db.warehouse_kpi_daily')
  if ($missing -gt 0) {
    Invoke-ClickHouseQuery -Query @"
INSERT INTO wuliu_w1_clickhouse_db.warehouse_kpi_daily
SELECT
  toDate('2026-04-01') + number % 35 AS stat_date,
  multiIf(number % 3 = 0, 'W1_WH_SZ01', number % 3 = 1, 'W1_WH_SH01', 'W1_WH_NJ01') AS warehouse_id,
  800 + number * 3 AS inbound_qty,
  760 + number * 3 AS outbound_qty,
  0.82 + (number % 12) * 0.01 AS pick_efficiency,
  0.90 + (number % 8) * 0.01 AS inventory_accuracy_rate,
  0.002 + (number % 5) * 0.0005 AS damage_rate,
  0.88 + (number % 10) * 0.01 AS on_time_rate
FROM numbers($missing);
"@
  }
}

function Boost-Sqlite {
  $dbFile = Join-Path $PSScriptRoot 'd1_sqlite\d1_sqlite.db'
  $py = @'
import sqlite3
import re
import sys
from datetime import datetime, timedelta

db_path = sys.argv[1]
target = int(sys.argv[2])

def mutate_str(key: str, val: str, idx: int) -> str:
    if val is None:
        return val
    if "phone" in key.lower():
        return "13" + str(100000000 + idx)[-9:]
    if re.search(r"(id|no|code|task|scan|retry|address|biz|device|sign)", key, re.I):
        return f"{val}-{idx:04d}"
    return val

def mutate_val(key: str, val, idx: int):
    if isinstance(val, str):
        if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", val):
            dt = datetime.strptime(val, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=idx)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return mutate_str(key, val, idx)
    if isinstance(val, int):
        return val + (idx % 9)
    if isinstance(val, float):
        return val + (idx % 5) * 0.01
    return val

conn = sqlite3.connect(db_path)
cur = conn.cursor()
tables = ["local_sign_cache", "offline_address_book", "retry_upload_queue", "scan_cache"]
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    cnt = int(cur.fetchone()[0])
    if cnt <= 0:
        continue
    tries = 0
    while cnt < target and tries < 6:
        tries += 1
        missing = target - cnt
        cur.execute(f"PRAGMA table_info({t})")
        cols_meta = cur.fetchall()
        cols = [x[1] for x in cols_meta]
        cur.execute(f"SELECT * FROM {t}")
        base_rows = cur.fetchall()
        if not base_rows:
            break
        inserts = []
        for i in range(1, missing + 1):
            seed = base_rows[(i - 1) % len(base_rows)]
            row = [mutate_val(cols[j], seed[j], cnt + i) for j in range(len(cols))]
            inserts.append(tuple(row))
        placeholders = ",".join(["?"] * len(cols))
        col_list = ",".join(cols)
        cur.executemany(f"INSERT OR IGNORE INTO {t} ({col_list}) VALUES ({placeholders})", inserts)
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = int(cur.fetchone()[0])
    print(f"{t}={cnt}")
conn.close()
'@
  $out = $py | & $script:python - $dbFile "$TargetCount"
  if ($LASTEXITCODE -ne 0) {
    throw 'SQLite boost failed.'
  }
  return $out
}

function Boost-Redis {
  $keys = @(
    'pending_sort_queue:{H1_ST_SH01}',
    'pending_sort_queue:{H1_ST_NJ01}',
    'pending_sort_queue:{H1_ST_TJ01}'
  )
  foreach ($key in $keys) {
    $station = ($key -replace '^pending_sort_queue:\{', '') -replace '\}$', ''
    $lenOut = & $script:redis -h $script:RedisHost -p $script:RedisPort -a $script:RedisPassword LLEN $key
    if ($LASTEXITCODE -ne 0) { throw "Redis LLEN failed: $key" }
    $len = Parse-LastInteger -Lines $lenOut
    if ($len -ge $TargetCount) { continue }
    $base = [datetimeoffset]::Parse('2026-04-01T08:00:00+08:00')
    for ($i = $len + 1; $i -le $TargetCount; $i++) {
      $pkg = ('PACNO-20260401-{0}' -f $i.ToString('0000'))
      $priority = if ($i % 3 -eq 0) { 'L1' } elseif ($i % 3 -eq 1) { 'L2' } else { 'L3' }
      $enqueue = $base.AddSeconds($i * 35).ToString('yyyy-MM-ddTHH:mm:sszzz')
      $payload = "{`"package_no`":`"$pkg`",`"station_id`":`"$station`",`"priority_level`":`"$priority`",`"enqueue_time`":`"$enqueue`"}"
      & $script:redis -h $script:RedisHost -p $script:RedisPort -a $script:RedisPassword RPUSH $key $payload | Out-Null
      if ($LASTEXITCODE -ne 0) { throw "Redis RPUSH failed: $key" }
    }
    & $script:redis -h $script:RedisHost -p $script:RedisPort -a $script:RedisPassword EXPIRE $key 7200 | Out-Null
  }
}

function Show-Summary {
  Write-Host ''
  Write-Host '=== Summary Counts (key objects) ==='
  $mysqlChecks = @(
    'SELECT ''wuliu_m1_mysql_db.customer'', COUNT(*) FROM wuliu_m1_mysql_db.customer',
    'SELECT ''wuliu_m2_mysql_db.client_master'', COUNT(*) FROM wuliu_m2_mysql_db.client_master',
    'SELECT ''wuliu_w1_mysql_db.warehouse'', COUNT(*) FROM wuliu_w1_mysql_db.warehouse',
    'SELECT ''wuliu_w2_mysql_db.warehouse_ref'', COUNT(*) FROM wuliu_w2_mysql_db.warehouse_ref',
    'SELECT ''wuliu_t2_mysql_db.truck_master'', COUNT(*) FROM wuliu_t2_mysql_db.truck_master'
  )
  foreach ($q in $mysqlChecks) {
    $o = Invoke-MySqlQuery -Query $q
    Write-Host ($o -join ' ')
  }
}

$env:MYSQL_PWD = $MySqlPassword
$env:PGPASSWORD = $PgPassword
try {
  Write-Host "Boost remaining tables/collections to target=$TargetCount"

  # MySQL all tables
  foreach ($db in @('wuliu_m1_mysql_db', 'wuliu_m2_mysql_db', 'wuliu_w1_mysql_db', 'wuliu_w2_mysql_db', 'wuliu_t2_mysql_db')) {
    Boost-MySqlDatabase -DbName $db
  }

  # PostgreSQL all tables
  Boost-PgDatabase -Database 'wuliu_p1_postgresql_db' -Schema 'p1'
  Boost-PgDatabase -Database 'wuliu_t1_postgresql_db' -Schema 'public'
  Boost-PgDatabase -Database 'wuliu_h1_postgresql_db' -Schema 'public'

  # MongoDB all collections
  Boost-Mongo

  # ClickHouse remaining analytical tables
  Boost-ClickHouse

  # SQLite remaining local cache tables
  $null = Boost-Sqlite

  # Redis missing queue keys
  Boost-Redis

  Show-Summary
}
finally {
  Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue
  Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}
