param(
  [string]$Endpoint = 'http://127.0.0.1:9200'
)

$baseUri = $Endpoint.TrimEnd('/')

function Invoke-EsRequest {
  param(
    [string]$Method,
    [string]$Path,
    [object]$Body = $null
  )

  $uri = "$baseUri/$($Path.TrimStart('/'))"
  $params = @{
    Method      = $Method
    Uri         = $uri
    TimeoutSec  = 30
    ErrorAction = 'Stop'
  }

  if ($null -ne $Body) {
    if ($Body -is [string]) {
      $params.Body = $Body
    }
    else {
      $params.Body = $Body | ConvertTo-Json -Depth 12 -Compress
    }
    $params.ContentType = 'application/json'
  }

  Invoke-RestMethod @params
}

try {
  Invoke-EsRequest -Method Get -Path '' | Out-Null
}
catch {
  throw "School B Elasticsearch connection failed: $baseUri"
}

$indexName = 'university_search_db'
$mapping = @{
  mappings = @{
    properties = @{
      resource_id   = @{ type = 'keyword' }
      resource_name = @{ type = 'text' }
      source_domain = @{ type = 'keyword' }
      semantic_tags = @{ type = 'keyword' }
      share_level   = @{ type = 'keyword' }
      status        = @{ type = 'keyword' }
    }
  }
}

$docs = @(
  @{
    resource_id   = 'RES_B_0001'
    resource_name = '数据库系统原理教学大纲'
    source_domain = 'teaching_affairs_db'
    semantic_tags = @('教学文档', '数据库课程', '大纲')
    share_level   = 'intra-university'
    status        = 'indexed'
  }
)

try {
  Invoke-EsRequest -Method Delete -Path $indexName | Out-Null
}
catch {
  $statusCode = $null
  if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
    $statusCode = [int]$_.Exception.Response.StatusCode.value__
  }
  if ($statusCode -ne 404) {
    throw
  }
}

$created = $false
for ($i = 0; $i -lt 2 -and -not $created; $i++) {
  try {
    Invoke-EsRequest -Method Put -Path $indexName -Body $mapping | Out-Null
    $created = $true
  }
  catch {
    $message = $_.ErrorDetails.Message
    if ($message -match 'resource_already_exists_exception' -and $i -eq 0) {
      Invoke-EsRequest -Method Delete -Path $indexName | Out-Null
      Start-Sleep -Milliseconds 200
    }
    else {
      throw
    }
  }
}

if (-not $created) {
  throw "Failed to create index: $indexName"
}

foreach ($doc in $docs) {
  $docId = [string]$doc.resource_id
  Invoke-EsRequest -Method Put -Path "$indexName/_doc/$docId" -Body $doc | Out-Null
}

Invoke-EsRequest -Method Post -Path "$indexName/_refresh" | Out-Null
$countResp = Invoke-EsRequest -Method Get -Path "$indexName/_count"
if ([int]$countResp.count -ne 1) {
  throw 'School B Elasticsearch sample count verification failed.'
}

Write-Host "School B Elasticsearch index initialized: $indexName"