param(
  [string]$MySqlHost = '127.0.0.1',
  [int]$MySqlPort = 3306,
  [string]$MySqlUser = 'root',
  [string]$MySqlPassword = '123456',

  [string]$PostgresHost = '127.0.0.1',
  [int]$PostgresPort = 5432,
  [string]$PostgresUser = 'postgres',
  [string]$PostgresPassword = '123456'
)

$ErrorActionPreference = 'Stop'
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$mysql = (Get-Command mysql -ErrorAction Stop).Source
$env:MYSQL_PWD = $MySqlPassword

function Invoke-MySql {
  param([string]$Sql)
  $result = $Sql | & $mysql --host=$MySqlHost --port=$MySqlPort --user=$MySqlUser --default-character-set=utf8mb4 --batch --raw --skip-column-names 2>$null
  if ($LASTEXITCODE -ne 0) {
    throw "MySQL execution failed."
  }
  return $result
}

function Get-MySqlScalar {
  param([string]$Sql)
  $lines = Invoke-MySql -Sql ($Sql + "`n") | ForEach-Object { ($_ | Out-String).Trim() } | Where-Object { $_ -ne '' }
  if ($lines.Count -lt 1) { return '' }
  return [string]$lines[$lines.Count - 1]
}

function Get-MySqlCount {
  param([string]$Db, [string]$Table)
  $v = Get-MySqlScalar -Sql "SELECT COUNT(*) FROM $Db.$Table;"
  $n = 0
  [int]::TryParse($v, [ref]$n) | Out-Null
  return $n
}

function Get-MySqlLines {
  param([string]$Sql)
  $lines = Invoke-MySql -Sql ($Sql + "`n") | ForEach-Object { ($_ | Out-String).Trim() } | Where-Object { $_ -ne '' }
  return @($lines)
}

$candidates = @()
$psqlCmd = Get-Command psql -ErrorAction SilentlyContinue
if ($psqlCmd) { $candidates += $psqlCmd.Source }
$candidates += 'D:\Program Files\PostgreSQL\18\bin\psql.exe'
$candidates += 'C:\Program Files\PostgreSQL\18\bin\psql.exe'
$candidates += 'D:\Program Files\PostgreSQL\17\bin\psql.exe'
$candidates += 'C:\Program Files\PostgreSQL\17\bin\psql.exe'
$psql = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $psql) { throw 'psql not found.' }

$env:PGPASSWORD = $PostgresPassword
$env:PGCLIENTENCODING = 'UTF8'

function Invoke-Psql {
  param([string]$Db, [string]$Sql)
  $out = & $psql -h $PostgresHost -p $PostgresPort -U $PostgresUser -d $Db -v ON_ERROR_STOP=1 -Atc $Sql
  if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL execution failed on database: $Db"
  }
  return $out
}

function Get-PsqlCount {
  param([string]$Db, [string]$Table)
  $raw = (Invoke-Psql -Db $Db -Sql "SELECT COUNT(*) FROM $Table;" | Out-String).Trim()
  $n = 0
  [int]::TryParse($raw, [ref]$n) | Out-Null
  return $n
}

$target = 88

# ---------- MySQL A ----------
$sb = [System.Text.StringBuilder]::new()
$current = Get-MySqlCount 'university_base_db' 'college_department'
for ($i = $current + 1; $i -le $target; $i++) {
  $deptId = ('DA{0:d3}' -f $i)
  $cat = @('工学','理学','管理学')[$i % 3]
  [void]$sb.AppendLine("INSERT IGNORE INTO university_base_db.college_department (dept_id, university_id, dept_name, dept_name_en, discipline_category, director_name, teacher_total, student_total, status) VALUES ('$deptId', 'U001', '学院A$i', 'School A $i', '$cat', '主任A$i', $(80 + ($i % 90)), $(1500 + (($i * 17) % 2000)), 'normal');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'university_base_db' 'discipline_info'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('DIA{0:d4}' -f $i)
  $code = ('A{0:d4}' -f $i)
  $dept = if ($i % 2 -eq 0) { 'D101' } else { 'D102' }
  $lvl = if ($i % 2 -eq 0) { '一级学科' } else { '二级学科' }
  $eval = @('A-','B+','B','C+')[$i % 4]
  $st = @('ongoing','planned','stable')[$i % 3]
  [void]$sb.AppendLine("INSERT IGNORE INTO university_base_db.discipline_info (discipline_id, university_id, dept_id, discipline_name, discipline_code, discipline_level, evaluation_result, status) VALUES ('$id', 'U001', '$dept', '计算机科学与技术', '$code', '$lvl', '$eval', '$st');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'faculty_hr_db' 'teacher'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('TA{0:d5}' -f $i)
  $emp = ('8{0:d7}' -f $i)
  $name = ('教师A{0:d3}' -f $i)
  $gender = if ($i % 2 -eq 0) { '男' } else { '女' }
  $birth = 1975 + ($i % 20)
  $dept = if ($i % 2 -eq 0) { 'D101' } else { 'D102' }
  $title = @('教授','副教授','讲师')[$i % 3]
  $pos = @('专任教师','教学科研岗','科研岗')[$i % 3]
  $degree = if ($i % 3 -eq 0) { '硕士' } else { '博士' }
  $alma = @('南京大学','东南大学','武汉大学','浙江大学')[$i % 4]
  $area = @('数据挖掘与知识工程','知识图谱与语义检索','数据库系统与优化','分布式系统')[$i % 4]
  [void]$sb.AppendLine("INSERT IGNORE INTO faculty_hr_db.teacher (teacher_id, emp_no, teacher_name, gender, birth_year, dept_id, title, position_type, degree, alma_mater, research_area, employment_status, status) VALUES ('$id', '$emp', '$name', '$gender', $birth, '$dept', '$title', '$pos', '$degree', '$alma', '$area', '在岗', 'active');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$teacherIds = Get-MySqlLines "SELECT teacher_id FROM faculty_hr_db.teacher ORDER BY teacher_id LIMIT $target;"

$sb.Clear() | Out-Null
$idx = 0
foreach ($tid in $teacherIds) {
  $idx++
  $paper = 10 + ($idx % 40)
  $pat = 1 + ($idx % 10)
  $proj = 1 + ($idx % 8)
  $fund = 300000 + ($idx * 15000)
  $sci = 3 + ($idx % 15)
  [void]$sb.AppendLine("INSERT IGNORE INTO faculty_hr_db.teacher_research_summary (teacher_id, paper_count, patent_count, project_count, received_funding_3y, sci_paper_count, status) VALUES ('$tid', $paper, $pat, $proj, $fund, $sci, 'valid');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$idx = 0
foreach ($tid in $teacherIds) {
  $idx++
  $rid = ('THA{0:d5}' -f $idx)
  $year = 2010 + ($idx % 15)
  $month = 1 + ($idx % 12)
  $day = 1 + ($idx % 27)
  $title = @('讲师','副教授','教授')[$idx % 3]
  [void]$sb.AppendLine("INSERT IGNORE INTO faculty_hr_db.teacher_title_history (record_id, teacher_id, title_name, effective_date, appraisal_result, status) VALUES ('$rid', '$tid', '$title', '$year-$month-$day', '通过', 'archived');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'teaching_affairs_db' 'major_catalog'
for ($i = $current + 1; $i -le $target; $i++) {
  $mid = ('MA{0:d4}' -f $i)
  $mcode = ('18{0:d4}' -f $i)
  [void]$sb.AppendLine("INSERT IGNORE INTO teaching_affairs_db.major_catalog (major_id, major_name, major_code, degree_type, admission_category, status) VALUES ('$mid', '专业A$i', '$mcode', '工学学士', '普通本科', 'running');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'teaching_affairs_db' 'course_info'
for ($i = $current + 1; $i -le $target; $i++) {
  $cid = ('CA{0:d4}' -f $i)
  $ctype = if ($i % 2 -eq 0) { '专业核心课' } else { '专业选修课' }
  $core = if ($i % 2 -eq 0) { 1 } else { 0 }
  $credit = if ($i % 3 -eq 0) { '2.0' } elseif ($i % 3 -eq 1) { '3.0' } else { '3.5' }
  $hours = 32 + (($i % 4) * 8)
  [void]$sb.AppendLine("INSERT IGNORE INTO teaching_affairs_db.course_info (course_id, course_name, course_type, credit, total_hours, is_core_course, status) VALUES ('$cid', '课程A$i', '$ctype', $credit, $hours, $core, 'open');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$courseIds = Get-MySqlLines "SELECT course_id FROM teaching_affairs_db.course_info ORDER BY course_id LIMIT $target;"

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'teaching_affairs_db' 'course_offering'
for ($i = $current + 1; $i -le $target; $i++) {
  $oid = ('OFA{0:d5}' -f $i)
  $cid = $courseIds[($i - 1) % $courseIds.Count]
  $tid = $teacherIds[($i - 1) % $teacherIds.Count]
  $enroll = 30 + (($i * 7) % 120)
  $avg = 70 + (($i * 3) % 25)
  [void]$sb.AppendLine("INSERT IGNORE INTO teaching_affairs_db.course_offering (offering_id, course_id, academic_term, teacher_id, enrolled_count, avg_score, status) VALUES ('$oid', '$cid', '2025-秋', '$tid', $enroll, $avg, 'finished');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'student_training_db' 'student_record'
for ($i = $current + 1; $i -le $target; $i++) {
  $sid = ('SA{0:d6}' -f $i)
  $sno = ('31{0:d6}' -f $i)
  $name = ('学生A{0:d3}' -f $i)
  $major = if ($i % 2 -eq 0) { 'M080901' } else { 'M080202' }
  $status = if ($i % 8 -eq 0) { '毕业' } else { '在读' }
  [void]$sb.AppendLine("INSERT IGNORE INTO student_training_db.student_record (student_id, student_no, student_name, education_level, major_id, current_status, status) VALUES ('$sid', '$sno', '$name', '本科', '$major', '$status', 'active');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$studentIds = Get-MySqlLines "SELECT student_id FROM student_training_db.student_record ORDER BY student_id LIMIT $target;"

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'student_training_db' 'course_grade'
for ($i = $current + 1; $i -le $target; $i++) {
  $gid = ('GRA{0:d5}' -f $i)
  $sid = $studentIds[($i - 1) % $studentIds.Count]
  $usual = 70 + (($i * 2) % 28)
  $final = 68 + (($i * 3) % 30)
  $total = [math]::Round(($usual * 0.4 + $final * 0.6), 2)
  $gpa = [math]::Round((2.0 + (($i % 20) / 10.0)), 2)
  [void]$sb.AppendLine("INSERT IGNORE INTO student_training_db.course_grade (grade_id, student_id, usual_score, final_score, total_score, gpa_score, status) VALUES ('$gid', '$sid', $usual, $final, $total, $gpa, 'published');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$idx = 0
foreach ($sid in $studentIds) {
  $idx++
  [void]$sb.AppendLine("INSERT IGNORE INTO student_training_db.degree_graduation (student_id, thesis_title, defense_result, degree_type, graduation_status, status) VALUES ('$sid', '论文A$idx', '通过', '工学学士', '正常毕业', 'completed');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'admission_employment_db' 'admission_plan'
for ($i = $current + 1; $i -le $target; $i++) {
  $aid = ('ADA{0:d5}' -f $i)
  $plan = 150 + ($i % 120)
  $adm = $plan - ($i % 5)
  $avg = 560 + ($i % 70)
  [void]$sb.AppendLine("INSERT IGNORE INTO admission_employment_db.admission_plan (admission_id, admission_type, planned_count, admitted_count, avg_score, status) VALUES ('$aid', '普通本科', $plan, $adm, $avg, 'completed');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'admission_employment_db' 'graduate_employment'
for ($i = $current + 1; $i -le $target; $i++) {
  $rid = ('EA{0:d5}' -f $i)
  $sid = $studentIds[($i - 1) % $studentIds.Count]
  $dest = @('协议就业','升学','灵活就业')[$i % 3]
  $employerSql = if ($dest -eq '协议就业') { "'企业A$i'" } else { 'NULL' }
  $industrySql = if ($dest -eq '协议就业') { "'信息技术服务业'" } else { 'NULL' }
  $citySql = if ($dest -eq '协议就业') { "'南京'" } else { 'NULL' }
  $salarySql = if ($dest -eq '协议就业') { "'10k-15k'" } else { 'NULL' }
  $studySql = if ($dest -eq '升学') { "'硕士'" } else { 'NULL' }
  [void]$sb.AppendLine("INSERT IGNORE INTO admission_employment_db.graduate_employment (record_id, student_id, destination_type, employer_name, industry_sector, work_city, salary_range, study_level, status) VALUES ('$rid', '$sid', '$dest', $employerSql, $industrySql, $citySql, $salarySql, $studySql, 'confirmed');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

# ---------- MySQL B ----------
$sb.Clear() | Out-Null
$current = Get-MySqlCount 'base_info_db' 'org_unit'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('BU{0:d3}' -f $i)
  $cat = @('工学','理学','管理学')[$i % 3]
  [void]$sb.AppendLine("INSERT IGNORE INTO base_info_db.org_unit (unit_id, institution_id, unit_name, subject_category, status) VALUES ('$id', 'U002', '学院B$i', '$cat', 'normal');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'base_info_db' 'subject_catalog'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('SUBB{0:d4}' -f $i)
  $lvl = if ($i % 2 -eq 0) { '一级学科' } else { '二级学科' }
  $grade = @('A-','B+','B','C+')[$i % 4]
  [void]$sb.AppendLine("INSERT IGNORE INTO base_info_db.subject_catalog (subject_id, institution_id, subject_name, subject_level, assessment_grade, status) VALUES ('$id', 'U002', '学科B$i', '$lvl', '$grade', 'ongoing');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'teaching_affairs_db' 'program_info'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('PB{0:d4}' -f $i)
  $code = ('28{0:d4}' -f $i)
  [void]$sb.AppendLine("INSERT IGNORE INTO teaching_affairs_db.program_info (program_id, program_name, program_code, degree_name, status) VALUES ('$id', '专业B$i', '$code', '工学学士', 'running');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'teaching_affairs_db' 'curriculum_course'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('CB{0:d4}' -f $i)
  $type = if ($i % 2 -eq 0) { '专业核心课' } else { '专业选修课' }
  $core = if ($i % 2 -eq 0) { 1 } else { 0 }
  $credit = if ($i % 3 -eq 0) { '2.0' } elseif ($i % 3 -eq 1) { '2.5' } else { '3.0' }
  $hours = 32 + (($i % 4) * 8)
  [void]$sb.AppendLine("INSERT IGNORE INTO teaching_affairs_db.curriculum_course (curriculum_id, subject_name, curriculum_type, credit_value, hours_total, core_flag, status) VALUES ('$id', '课程B$i', '$type', $credit, $hours, $core, 'open');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$currIds = Get-MySqlLines "SELECT curriculum_id FROM teaching_affairs_db.curriculum_course ORDER BY curriculum_id LIMIT $target;"
$facIds = @('F000221','F000222','F000223','F000224')

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'teaching_affairs_db' 'class_schedule_record'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('CLB{0:d5}' -f $i)
  $cid = $currIds[($i - 1) % $currIds.Count]
  $fid = $facIds[($i - 1) % $facIds.Count]
  $cnt = 40 + (($i * 5) % 120)
  [void]$sb.AppendLine("INSERT IGNORE INTO teaching_affairs_db.class_schedule_record (class_id, curriculum_id, faculty_no, student_count, status) VALUES ('$id', '$cid', '$fid', $cnt, 'finished');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'student_training_db' 'learner_profile'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('LB{0:d6}' -f $i)
  $no = ('41{0:d6}' -f $i)
  $name = ('学生B{0:d3}' -f $i)
  $st = if ($i % 8 -eq 0) { '毕业' } else { '在读' }
  [void]$sb.AppendLine("INSERT IGNORE INTO student_training_db.learner_profile (learner_id, stu_no, learner_name, level_name, student_status, status) VALUES ('$id', '$no', '$name', '本科', '$st', 'active');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$learnerIds = Get-MySqlLines "SELECT learner_id FROM student_training_db.learner_profile ORDER BY learner_id LIMIT $target;"

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'student_training_db' 'student_score'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('SCB{0:d5}' -f $i)
  $lid = $learnerIds[($i - 1) % $learnerIds.Count]
  $usual = 68 + (($i * 2) % 30)
  $final = 66 + (($i * 3) % 32)
  $gp = [math]::Round((2.0 + (($i % 20) / 10.0)), 2)
  [void]$sb.AppendLine("INSERT IGNORE INTO student_training_db.student_score (score_id, learner_id, usual_mark, final_mark, grade_point, status) VALUES ('$id', '$lid', $usual, $final, $gp, 'published');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$idx = 0
foreach ($lid in $learnerIds) {
  $idx++
  [void]$sb.AppendLine("INSERT IGNORE INTO student_training_db.graduate_degree_info (learner_id, dissertation_title, degree_name, graduate_status, status) VALUES ('$lid', '论文B$idx', '工学学士', '正常毕业', 'completed');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'admission_employment_db' 'enrollment_quota'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('EQB{0:d5}' -f $i)
  $quota = 130 + ($i % 120)
  $actual = $quota - ($i % 6)
  $mean = 540 + ($i % 70)
  [void]$sb.AppendLine("INSERT IGNORE INTO admission_employment_db.enrollment_quota (quota_id, enroll_type, quota_num, actual_num, mean_score, status) VALUES ('$id', '普通本科', $quota, $actual, $mean, 'completed');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

$sb.Clear() | Out-Null
$current = Get-MySqlCount 'admission_employment_db' 'career_outcome'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('COB{0:d5}' -f $i)
  $lid = $learnerIds[($i - 1) % $learnerIds.Count]
  $career = @('升学','协议就业','灵活就业')[$i % 3]
  $company = if ($career -eq '协议就业') { "'企业B$i'" } else { 'NULL' }
  $income = if ($career -eq '协议就业') { "'10k-15k'" } else { 'NULL' }
  $school = if ($career -eq '升学') { "'高校B$i'" } else { 'NULL' }
  $stage = if ($career -eq '升学') { "'硕士'" } else { 'NULL' }
  [void]$sb.AppendLine("INSERT IGNORE INTO admission_employment_db.career_outcome (outcome_id, learner_id, career_type, company_name, income_band, study_school, study_stage, status) VALUES ('$id', '$lid', '$career', $company, $income, $school, $stage, 'confirmed');")
}
if ($sb.Length -gt 0) { Invoke-MySql -Sql $sb.ToString() | Out-Null }

# ---------- PostgreSQL research_output_db ----------
$current = Get-PsqlCount 'research_output_db' 'research_project'
for ($i = $current + 1; $i -le $target; $i++) {
  $projId = ('RPA{0:d5}' -f $i)
  $pcode = ('NSFCA{0:d5}' -f $i)
  $principal = if ($i % 2 -eq 0) { 'T000123' } else { 'T000124' }
  Invoke-Psql -Db 'research_output_db' -Sql "INSERT INTO research_project (project_id, project_name, project_code, project_level, project_source, principal_id, received_amount, status) VALUES ('$projId','ProjectA$i','$pcode','provincial','science_plan','$principal',$(150000 + $i * 2000),'active') ON CONFLICT (project_id) DO NOTHING;" | Out-Null
}

$current = Get-PsqlCount 'research_output_db' 'paper_output'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('PA{0:d5}' -f $i)
  $doi = ('10.9999/eswa.a.{0:d5}' -f $i)
  Invoke-Psql -Db 'research_output_db' -Sql "INSERT INTO paper_output (paper_id, title, journal_name, index_type, doi, citation_count, paper_status, status) VALUES ('$id','Paper A $i','Journal A','SCI','$doi',$($i % 120),'published','valid') ON CONFLICT (paper_id) DO NOTHING;" | Out-Null
}

$current = Get-PsqlCount 'research_output_db' 'patent_record'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('PTA{0:d5}' -f $i)
  $app = ('CN20A{0:d7}.1' -f $i)
  Invoke-Psql -Db 'research_output_db' -Sql "INSERT INTO patent_record (patent_id, patent_name, patent_type, application_no, transfer_amount, current_status, status) VALUES ('$id','PatentA$i','Invention','$app',$([decimal](50000 + $i * 1000)),'granted','valid') ON CONFLICT (patent_id) DO NOTHING;" | Out-Null
}

$current = Get-PsqlCount 'research_output_db' 'grant_award'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('GAB{0:d5}' -f $i)
  $leader = if ($i % 2 -eq 0) { 'F000221' } else { 'F000222' }
  Invoke-Psql -Db 'research_output_db' -Sql "INSERT INTO grant_award (grant_id, grant_name, grant_level, funding_source, leader_id, arrival_fee, status) VALUES ('$id','GrantB$i','provincial','science_dept','$leader',$([decimal](90000 + $i * 1200)),'active') ON CONFLICT (grant_id) DO NOTHING;" | Out-Null
}

$current = Get-PsqlCount 'research_output_db' 'publication_record'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('PUBB{0:d5}' -f $i)
  Invoke-Psql -Db 'research_output_db' -Sql "INSERT INTO publication_record (publication_id, paper_title, journal_title, include_type, cited_times, publish_status, status) VALUES ('$id','Publication B $i','Journal B','SCI',$($i % 80),'published','valid') ON CONFLICT (publication_id) DO NOTHING;" | Out-Null
}

$current = Get-PsqlCount 'research_output_db' 'ip_asset'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('IPB{0:d5}' -f $i)
  Invoke-Psql -Db 'research_output_db' -Sql "INSERT INTO ip_asset (ip_id, ip_name, ip_type, conversion_income, legal_status, status) VALUES ('$id','IPB$i','Invention',$([decimal](30000 + $i * 800)),'granted','valid') ON CONFLICT (ip_id) DO NOTHING;" | Out-Null
}

# ---------- PostgreSQL faculty_research_db ----------
$current = Get-PsqlCount 'faculty_research_db' 'faculty_member'
for ($i = $current + 1; $i -le $target; $i++) {
  $id = ('FB{0:d5}' -f $i)
  $emp = ('7{0:d7}' -f $i)
  $rank = @('Lecturer','AssociateProfessor','Professor')[$i % 3]
  $job = @('FullTime','TeachingResearch','Research')[$i % 3]
  $school = @('Zhengzhou University','Wuhan University','HUST','XJTU')[$i % 4]
  $dir = @('KnowledgeGraph','EduData','SemanticComputing','SmartManufacturing')[$i % 4]
  Invoke-Psql -Db 'faculty_research_db' -Sql "INSERT INTO faculty_member (faculty_no, employee_code, faculty_name, `"rank`", job_type, graduated_school, research_direction, job_status, status) VALUES ('$id','$emp','FacultyB$i','$rank','$job','$school','$dir','active','active') ON CONFLICT (faculty_no) DO NOTHING;" | Out-Null
}

$facultyIds = Invoke-Psql -Db 'faculty_research_db' -Sql "SELECT faculty_no FROM faculty_member ORDER BY faculty_no LIMIT $target;"
$facultyIds = @($facultyIds | ForEach-Object { ($_ | Out-String).Trim() } | Where-Object { $_ -ne '' })

$idx = 0
foreach ($fid in $facultyIds) {
  $idx++
  Invoke-Psql -Db 'faculty_research_db' -Sql "INSERT INTO faculty_output_metric (faculty_no, publication_total, patent_total, fund_income_3y, status) VALUES ('$fid',$(10 + ($idx % 40)),$(1 + ($idx % 12)),$([decimal](500000 + $idx * 10000)),'valid') ON CONFLICT (faculty_no) DO NOTHING;" | Out-Null
}

$idx = 0
foreach ($fid in $facultyIds) {
  $idx++
  $rid = ('FRB{0:d5}' -f $idx)
  $rank = @('Lecturer','AssociateProfessor','Professor')[$idx % 3]
  Invoke-Psql -Db 'faculty_research_db' -Sql "INSERT INTO faculty_rank_record (record_id, faculty_no, rank_name, review_result, status) VALUES ('$rid','$fid','$rank','pass','archived') ON CONFLICT (record_id) DO NOTHING;" | Out-Null
}

# ---------- MongoDB: keep A + B docs together ----------
& (Join-Path $PSScriptRoot 'school_a\mongodb\apply.ps1') | Out-Null
& (Join-Path $PSScriptRoot 'school_b\mongodb\apply.ps1') | Out-Null

Write-Host 'fill_samples_to_88 completed.'

Remove-Item Env:MYSQL_PWD -ErrorAction SilentlyContinue
Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
Remove-Item Env:PGCLIENTENCODING -ErrorAction SilentlyContinue
