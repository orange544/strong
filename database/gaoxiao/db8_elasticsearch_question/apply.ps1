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
  throw "DB8 Elasticsearch connection failed: $baseUri"
}

$indexDefinitions = @(
  @{
    Name = 'question_bank_index'
    IdField = 'question_id'
    Mapping = @{
      mappings = @{
        properties = @{
          question_id      = @{ type = 'keyword' }
          course_code      = @{ type = 'keyword' }
          question_type    = @{ type = 'keyword' }
          knowledge_point  = @{ type = 'keyword' }
          difficulty_level = @{ type = 'keyword' }
          question_text    = @{ type = 'text' }
          status           = @{ type = 'keyword' }
        }
      }
    }
    Docs = @(
      @{
        question_id      = 'Q_0001'
        course_code      = 'CS101'
        question_type    = 'MCQ'
        knowledge_point  = 'loop_structure'
        difficulty_level = 'MEDIUM'
        question_text    = 'Which statement about while loops is correct?'
        status           = 'ACTIVE'
      },
      @{
        question_id      = 'Q_0002'
        course_code      = 'MATH204'
        question_type    = 'FIB'
        knowledge_point  = 'determinant'
        difficulty_level = 'EASY'
        question_text    = 'For a 3x3 matrix A, det(A^T)=___'
        status           = 'ACTIVE'
      },
      @{
        question_id      = 'Q_0003'
        course_code      = 'EE310'
        question_type    = 'SHORT_ANSWER'
        knowledge_point  = 'combinational_logic'
        difficulty_level = 'HARD'
        question_text    = 'Briefly explain how a decoder works.'
        status           = 'INACTIVE'
      }
    )
  },
  @{
    Name = 'subjective_answer_index'
    IdField = 'answer_id'
    Mapping = @{
      mappings = @{
        properties = @{
          answer_id    = @{ type = 'keyword' }
          question_id  = @{ type = 'keyword' }
          student_id   = @{ type = 'keyword' }
          answer_text  = @{ type = 'text' }
          score        = @{ type = 'float' }
          evaluator_id = @{ type = 'keyword' }
          submit_time  = @{ type = 'date' }
        }
      }
    }
    Docs = @(
      @{
        answer_id    = 'ANS_0001'
        question_id  = 'Q_0003'
        student_id   = 'STU_2025_0001'
        answer_text  = 'A decoder maps encoded input bits to one-hot outputs for address selection.'
        score        = 8.5
        evaluator_id = 'T00021'
        submit_time  = '2025-12-10T09:20:00'
      },
      @{
        answer_id    = 'ANS_0002'
        question_id  = 'Q_0003'
        student_id   = 'STU_2025_0088'
        answer_text  = 'A decoder activates exactly one output line from binary inputs.'
        score        = 9.0
        evaluator_id = 'T00108'
        submit_time  = '2025-12-10T09:23:00'
      },
      @{
        answer_id    = 'ANS_0003'
        question_id  = 'Q_0003'
        student_id   = 'STU_2025_1566'
        answer_text  = 'An n-to-2^n decoder expands n-bit input to 2^n selectable outputs.'
        score        = 7.5
        evaluator_id = 'T00356'
        submit_time  = '2025-12-10T09:28:00'
      }
    )
  },
  @{
    Name = 'evaluation_report_index'
    IdField = 'report_id'
    Mapping = @{
      mappings = @{
        properties = @{
          report_id       = @{ type = 'keyword' }
          term_id         = @{ type = 'keyword' }
          course_code     = @{ type = 'keyword' }
          report_title    = @{ type = 'text' }
          report_text     = @{ type = 'text' }
          completion_rate = @{ type = 'float' }
          pass_rate       = @{ type = 'float' }
          generated_time  = @{ type = 'date' }
        }
      }
    }
    Docs = @(
      @{
        report_id       = 'REP_0001'
        term_id         = '2025-2026-1'
        course_code     = 'CS101'
        report_title    = 'Programming Fundamentals Attainment Report'
        report_text     = 'Loop and branching objectives are mostly achieved; add exception handling drills.'
        completion_rate = 0.96
        pass_rate       = 0.92
        generated_time  = '2026-01-20T10:00:00'
      },
      @{
        report_id       = 'REP_0002'
        term_id         = '2025-2026-1'
        course_code     = 'MATH204'
        report_title    = 'Linear Algebra Attainment Report'
        report_text     = 'Determinant and eigenvalue topics are stable; proof skills need reinforcement.'
        completion_rate = 0.91
        pass_rate       = 0.88
        generated_time  = '2026-01-20T10:05:00'
      },
      @{
        report_id       = 'REP_0003'
        term_id         = '2025-2026-1'
        course_code     = 'EE310'
        report_title    = 'Digital Circuits Attainment Report'
        report_text     = 'Combinational logic pass rate is lower than target; schedule focused tutoring.'
        completion_rate = 0.87
        pass_rate       = 0.81
        generated_time  = '2026-01-20T10:10:00'
      }
    )
  },
  @{
    Name = 'achievement_analysis_index'
    IdField = 'analysis_id'
    Mapping = @{
      mappings = @{
        properties = @{
          analysis_id      = @{ type = 'keyword' }
          course_code      = @{ type = 'keyword' }
          class_id         = @{ type = 'keyword' }
          knowledge_point  = @{ type = 'keyword' }
          attainment_score = @{ type = 'float' }
          risk_level       = @{ type = 'keyword' }
          recommendation   = @{ type = 'text' }
          calc_time        = @{ type = 'date' }
        }
      }
    }
    Docs = @(
      @{
        analysis_id      = 'ANA_0001'
        course_code      = 'CS101'
        class_id         = 'CLS_CS01'
        knowledge_point  = 'loop_structure'
        attainment_score = 0.89
        risk_level       = 'LOW'
        recommendation   = 'Keep current pace and add integrated coding tasks.'
        calc_time        = '2026-01-21T09:00:00'
      },
      @{
        analysis_id      = 'ANA_0002'
        course_code      = 'MATH204'
        class_id         = 'CLS_MA01'
        knowledge_point  = 'determinant'
        attainment_score = 0.84
        risk_level       = 'MEDIUM'
        recommendation   = 'Add more proof-style examples and derivation walkthroughs.'
        calc_time        = '2026-01-21T09:05:00'
      },
      @{
        analysis_id      = 'ANA_0003'
        course_code      = 'EE310'
        class_id         = 'CLS_EE01'
        knowledge_point  = 'combinational_logic'
        attainment_score = 0.78
        risk_level       = 'HIGH'
        recommendation   = 'Arrange topic-focused tutoring and simulator-based practice.'
        calc_time        = '2026-01-21T09:10:00'
      }
    )
  }
)

foreach ($index in $indexDefinitions) {
  Invoke-EsRequest -Method Delete -Path "$($index.Name)?ignore_unavailable=true" | Out-Null
  Invoke-EsRequest -Method Put -Path $index.Name -Body $index.Mapping | Out-Null

  foreach ($doc in $index.Docs) {
    $docId = [string]$doc[$index.IdField]
    Invoke-EsRequest -Method Put -Path "$($index.Name)/_doc/$docId" -Body $doc | Out-Null
  }

  Invoke-EsRequest -Method Post -Path "$($index.Name)/_refresh" | Out-Null
}

$verify = New-Object System.Collections.Generic.List[object]
foreach ($index in $indexDefinitions) {
  $countResp = Invoke-EsRequest -Method Get -Path "$($index.Name)/_count"
  $count = [int]$countResp.count
  $verify.Add([pscustomobject]@{ Index = $index.Name; Count = $count })
  if ($count -lt 1) {
    throw "DB8 Elasticsearch index has no sample data: $($index.Name)"
  }
}

$verify | Format-Table -AutoSize
Write-Host 'DB8 Elasticsearch indexes and sample data initialized.'