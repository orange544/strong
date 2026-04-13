USE gaoxiao_db7_exam;

INSERT INTO exam_plan (
  exam_id, course_code, exam_name, term_id, exam_type,
  exam_time, candidate_scope, status
) VALUES
  ('EX_2025_0001', 'CS101', '程序设计基础期末考试', '2025-2026-1', '期末', '2026-01-12 09:00:00', '本科生', '已发布'),
  ('EX_2025_0045', 'MATH204', '高等代数补考', '2025-2026-1', '补考', '2026-02-20 14:00:00', '本科补考学生', '编排中'),
  ('EX_2025_0128', 'EE310', '数字电路重修考试', '2025-2026-2', '重修', '2026-06-28 10:00:00', '重修学生', '已结束');

INSERT INTO exam_room_arrangement (
  arrangement_id, exam_id, room_id, seat_count, arrange_status
) VALUES
  ('ERA_0001', 'EX_2025_0001', 'RM_A101', 60, '已编排'),
  ('ERA_0045', 'EX_2025_0045', 'RM_B203', 80, '待确认'),
  ('ERA_0128', 'EX_2025_0128', 'LAB_C305', 120, '已完成');

INSERT INTO invigilator_assignment (
  assignment_id, exam_id, teacher_no, teacher_name, duty_role, assign_status
) VALUES
  ('IVA_0001', 'EX_2025_0001', 'T00021', '刘强', '主监考', '已确认'),
  ('IVA_0045', 'EX_2025_0045', 'T00108', '周敏', '副监考', '待确认'),
  ('IVA_0128', 'EX_2025_0128', 'T00356', '陈浩', '主监考', '已完成');

INSERT INTO exam_signup (
  signup_id, exam_id, candidate_no, candidate_name, class_id,
  signup_status, signup_time
) VALUES
  ('ES_0001', 'EX_2025_0001', 'STU_2025_0001', '张晨', 'SEC_2025_0001', '已报名', '2025-12-20 08:00:00'),
  ('ES_0045', 'EX_2025_0045', 'STU_2025_0088', '李媛', 'SEC_2025_0023', '待审核', '2025-12-20 09:00:00'),
  ('ES_0128', 'EX_2025_0128', 'STU_2025_1566', '王磊', 'SEC_2025_0108', '已结束', '2026-06-10 10:00:00');

INSERT INTO admission_ticket (
  ticket_id, signup_id, ticket_no, print_time, ticket_status
) VALUES
  ('TICKET_0001', 'ES_0001', 'TK20250001', '2026-01-10 12:00:00', '已打印'),
  ('TICKET_0045', 'ES_0045', 'TK20250045', '2026-02-18 09:00:00', '待打印'),
  ('TICKET_0128', 'ES_0128', 'TK20250128', '2026-06-26 13:00:00', '已作废');

INSERT INTO exam_attendance (
  attendance_id, signup_id, checkin_time, attendance_status, room_id
) VALUES
  ('ATT_0001', 'ES_0001', '2026-01-12 08:40:00', '正常', 'RM_A101'),
  ('ATT_0045', 'ES_0045', '2026-02-20 13:50:00', '缺考', 'RM_B203'),
  ('ATT_0128', 'ES_0128', '2026-06-28 09:45:00', '迟到', 'LAB_C305');

INSERT INTO exam_violation (
  violation_id, signup_id, violation_type, violation_detail, record_time, process_status
) VALUES
  ('EV_0001', 'ES_0001', '无', '无违纪记录', '2026-01-12 11:00:00', '已归档'),
  ('EV_0045', 'ES_0045', '无', '缺考不构成违纪', '2026-02-20 17:00:00', '已归档'),
  ('EV_0128', 'ES_0128', '携带通讯设备', '考试中发现未关机手机', '2026-06-28 10:30:00', '处理中');

INSERT INTO exam_score_result (
  result_id, signup_id, raw_score, final_score, result_status, publish_time
) VALUES
  ('ESR_0001', 'ES_0001', 90.00, 90.00, '已发布', '2026-01-20 10:00:00'),
  ('ESR_0045', 'ES_0045', 0.00, 0.00, '缺考', '2026-02-25 11:00:00'),
  ('ESR_0128', 'ES_0128', 58.00, 52.00, '违纪降分', '2026-07-05 09:00:00');
