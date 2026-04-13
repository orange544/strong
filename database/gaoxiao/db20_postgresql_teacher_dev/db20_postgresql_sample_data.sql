INSERT INTO training_project (
  training_project_id, project_name, organizer, start_date, end_date, total_hour, project_status
) VALUES
  ('TR_0001', '新工科课程建设培训', '教师发展中心', '2025-10-01', '2025-10-12', 16.0, '已完成'),
  ('TR_0003', '课堂教学创新工作坊', '教师发展中心', '2025-10-25', '2025-11-03', 8.0, '已完成'),
  ('TR_0005', '师德师风专题培训', '教师发展中心', '2025-11-15', '2025-11-20', 4.0, '进行中');

INSERT INTO training_participation (
  participation_id, teacher_id, teacher_name, training_project_id,
  project_name, credit_hour, result_status, finish_time
) VALUES
  ('TPR_0001', 'T00021', '刘强', 'TR_0001', '新工科课程建设培训', 16.0, '已完成', '2025-10-12'),
  ('TPR_0002', 'T00108', '周敏', 'TR_0003', '课堂教学创新工作坊', 8.0, '已完成', '2025-11-03'),
  ('TPR_0003', 'T00356', '陈浩', 'TR_0005', '师德师风专题培训', 4.0, '进行中', '2025-11-20');

INSERT INTO credit_hour_record (
  record_id, participation_id, teacher_id, credit_hour, recognized_flag, record_time
) VALUES
  ('CHR_0001', 'TPR_0001', 'T00021', 16.0, TRUE, '2025-10-12'),
  ('CHR_0002', 'TPR_0002', 'T00108', 8.0, TRUE, '2025-11-03'),
  ('CHR_0003', 'TPR_0003', 'T00356', 4.0, FALSE, '2025-11-20');

INSERT INTO teaching_development_activity (
  activity_id, teacher_id, activity_name, activity_date, contribution_score, activity_status
) VALUES
  ('TDA_0001', 'T00021', '教学竞赛经验分享', '2025-10-18', 9.00, '已完成'),
  ('TDA_0002', 'T00108', '课堂互动设计研讨', '2025-10-28', 8.50, '已完成'),
  ('TDA_0003', 'T00356', '课程案例库建设', '2025-11-12', 7.80, '进行中');

INSERT INTO ethics_training_record (
  ethics_id, teacher_id, course_name, completion_date, exam_score, status
) VALUES
  ('ETR_0001', 'T00021', '师德规范与职业行为', '2025-11-02', 95.00, '已通过'),
  ('ETR_0002', 'T00108', '高校教师职业道德专题', '2025-11-05', 92.00, '已通过'),
  ('ETR_0003', 'T00356', '教学伦理与学术诚信', '2025-11-18', 88.00, '待补考');
