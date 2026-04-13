USE gaoxiao_db5_selection_score;

INSERT INTO course_selection (
  select_id, sid, full_name, sex_type, age_year, class_id,
  select_type, selection_status, select_time
) VALUES
  ('SEL_0001', 'STU_2025_0001', '张晨', '男', 18, 'SEC_2025_0001', '主修', '已选', '2025-09-02 09:00:00'),
  ('SEL_0235', 'STU_2025_0088', '李媛', '女', 19, 'SEC_2025_0023', '重修', '已退', '2025-09-02 10:15:00'),
  ('SEL_1890', 'STU_2025_1566', '王磊', '未知', 17, 'SEC_2025_0108', '补选', '待确认', '2025-09-03 08:40:00');

INSERT INTO selection_change_log (
  change_id, select_id, old_status, new_status, change_reason, change_time
) VALUES
  ('SCL_0001', 'SEL_0001', '待确认', '已选', '审核通过', '2025-09-02 09:05:00'),
  ('SCL_0235', 'SEL_0235', '已选', '已退', '学生退课', '2025-09-04 11:20:00'),
  ('SCL_1890', 'SEL_1890', '待确认', '待确认', '等待学院审批', '2025-09-03 09:00:00');

INSERT INTO course_score (
  score_id, select_id, sid, class_id, usual_score, exam_score, final_score, score_status
) VALUES
  ('SCORE_0001', 'SEL_0001', 'STU_2025_0001', 'SEC_2025_0001', 88.00, 90.00, 89.20, '已发布'),
  ('SCORE_0235', 'SEL_0235', 'STU_2025_0088', 'SEC_2025_0023', 70.00, 68.00, 68.80, '复核中'),
  ('SCORE_1890', 'SEL_1890', 'STU_2025_1566', 'SEC_2025_0108', 0.00, 0.00, 0.00, '未录入');

INSERT INTO score_component (
  component_id, score_id, component_name, component_weight, component_score
) VALUES
  ('SCOMP_0001', 'SCORE_0001', '平时成绩', 40.00, 88.00),
  ('SCOMP_0235', 'SCORE_0235', '期末考试', 60.00, 68.00),
  ('SCOMP_1890', 'SCORE_1890', '实验成绩', 20.00, 0.00);

INSERT INTO gpa_stat (
  stat_id, sid, term_id, total_credit, gpa, rank_in_major
) VALUES
  ('GPA_0001', 'STU_2025_0001', '2025-2026-1', 20.00, 3.72, 12),
  ('GPA_0088', 'STU_2025_0088', '2025-2026-1', 18.00, 2.65, 85),
  ('GPA_1566', 'STU_2025_1566', '2025-2026-1', 15.00, 1.20, 132);

INSERT INTO retake_record (
  retake_id, sid, class_id, reason, apply_time, retake_status
) VALUES
  ('RET_0001', 'STU_2025_0001', 'SEC_2025_0001', '成绩不满意申请重修提升', '2025-09-15 10:10:00', '已批准'),
  ('RET_0088', 'STU_2025_0088', 'SEC_2025_0023', '先修课程未通过', '2025-09-16 11:30:00', '待审核'),
  ('RET_1566', 'STU_2025_1566', 'SEC_2025_0108', '补选课程要求重修', '2025-09-17 09:45:00', '已驳回');

INSERT INTO makeup_exam_record (
  makeup_id, sid, class_id, signup_time, exam_time, exam_room, makeup_status
) VALUES
  ('MUP_0001', 'STU_2025_0001', 'SEC_2025_0001', '2025-12-20 08:00:00', '2025-12-30 14:00:00', 'RM_A101', '已报名'),
  ('MUP_0088', 'STU_2025_0088', 'SEC_2025_0023', '2025-12-20 09:10:00', '2025-12-30 15:00:00', 'RM_B203', '待确认'),
  ('MUP_1566', 'STU_2025_1566', 'SEC_2025_0108', '2025-12-20 09:45:00', '2025-12-30 16:00:00', 'LAB_C305', '取消');
