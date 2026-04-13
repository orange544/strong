USE gaoxiao_db1_admission;

INSERT INTO candidate (
  candidate_id,
  exam_no,
  candidate_name,
  gender,
  age,
  id_card_no,
  mobile_no,
  province_code,
  target_major_code,
  admission_status
) VALUES
  ('CAND_2025_0001', 'GK3205010001', '张晨', '男', 18, '320583200701015612', '13812345678', 'JS', '080901', '已录取'),
  ('CAND_2025_0088', 'GK3701020088', '李媛', '女', 19, '370102200609086527', '13988886666', 'SD', '070101', '待投档'),
  ('CAND_2025_1566', 'GK1101081566', '王磊', '未知', 17, '110108200812123216', '13766669999', 'BJ', '081001', '退档');

INSERT INTO application_form (
  app_id, candidate_id, batch_code, first_major_code, second_major_code,
  submit_time, form_status
) VALUES
  ('AF_0001', 'CAND_2025_0001', 'JS_BK_1', '080901', '080902', '2025-06-28 08:30:00', '已提交'),
  ('AF_0088', 'CAND_2025_0088', 'SD_BK_1', '070101', '070102', '2025-06-28 09:10:00', '已提交'),
  ('AF_1566', 'CAND_2025_1566', 'BJ_BK_1', '081001', '081002', '2025-06-28 09:45:00', '已锁定');

INSERT INTO admission_result (
  result_id, candidate_id, admit_major_code, admit_type, score,
  admit_status, publish_time
) VALUES
  ('AR_0001', 'CAND_2025_0001', '080901', '普通批', 642.5, '已录取', '2025-07-25 11:00:00'),
  ('AR_0088', 'CAND_2025_0088', '070101', '普通批', 598.0, '待投档', '2025-07-25 11:10:00'),
  ('AR_1566', 'CAND_2025_1566', '081001', '提前批', 575.0, '退档', '2025-07-25 11:20:00');

INSERT INTO major_plan (
  plan_id, major_code, major_name, province_code, plan_year, planned_count
) VALUES
  ('MP_JS_080901', '080901', '计算机科学与技术', 'JS', 2025, 120),
  ('MP_SD_070101', '070101', '数学与应用数学', 'SD', 2025, 90),
  ('MP_BJ_081001', '081001', '电气工程及其自动化', 'BJ', 2025, 80);

INSERT INTO province_batch (
  batch_id, province_code, batch_name, min_score, exam_year
) VALUES
  ('PB_JS_2025_1', 'JS', '本科一批', 610, 2025),
  ('PB_SD_2025_1', 'SD', '本科一批', 590, 2025),
  ('PB_BJ_2025_1', 'BJ', '本科普通批', 580, 2025);

INSERT INTO freshman_report_task (
  task_id, candidate_id, report_date, campus_code, dorm_assign_status, task_status
) VALUES
  ('FRT_0001', 'CAND_2025_0001', '2025-09-01', 'CAMPUS_MAIN', '已分配', '待报到'),
  ('FRT_0088', 'CAND_2025_0088', '2025-09-01', 'CAMPUS_EAST', '待分配', '待报到'),
  ('FRT_1566', 'CAND_2025_1566', '2025-09-02', 'CAMPUS_MAIN', '不适用', '取消报到');
