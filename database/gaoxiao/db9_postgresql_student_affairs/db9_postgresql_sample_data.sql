INSERT INTO student_profile (
  profile_id, stu_id, stu_name, xb, student_age, contact_phone,
  department_id, major_code, economic_level, oncampus_status
) VALUES
  ('PF_0001', 'STU_2025_0001', '张晨', '男', 18, '13812345678', 'D010', '080901', '特别困难', '在校'),
  ('PF_0088', 'STU_2025_0088', '李媛', '女', 19, '13988886666', 'D023', '070101', '一般困难', '请假'),
  ('PF_1566', 'STU_2025_1566', '王磊', '未知', 17, '13766669999', 'D018', '080701', '非困难', '离校实习');

INSERT INTO student_affair (
  affair_id, profile_id, affair_type, affair_desc, affair_time, affair_status
) VALUES
  ('SAF_0001', 'PF_0001', '日常管理', '宿舍夜归登记并完成教育提醒', '2025-10-08 21:30:00', '已处理'),
  ('SAF_0088', 'PF_0088', '请假管理', '因病请假材料提交审核', '2025-10-09 10:00:00', '审核中'),
  ('SAF_1566', 'PF_1566', '实习备案', '校外实习去向登记', '2025-10-10 14:00:00', '已备案');

INSERT INTO disciplinary_record (
  discipline_id, profile_id, discipline_level, reason, decision_date, expire_date, record_status
) VALUES
  ('DR_0001', 'PF_0001', '警告', '晚归两次', '2025-11-01', '2026-11-01', '生效'),
  ('DR_0088', 'PF_0088', '通报批评', '考试请假材料逾期', '2025-11-03', '2026-05-03', '生效'),
  ('DR_1566', 'PF_1566', '无', '无处分记录', '2025-11-05', NULL, '归档');

INSERT INTO leave_application (
  leave_id, profile_id, leave_type, start_date, end_date, leave_reason, leave_status
) VALUES
  ('LA_0001', 'PF_0001', '病假', '2025-10-15', '2025-10-17', '上呼吸道感染', '已批准'),
  ('LA_0088', 'PF_0088', '事假', '2025-10-20', '2025-10-22', '家庭事务', '待审批'),
  ('LA_1566', 'PF_1566', '实习假', '2025-10-25', '2025-12-25', '企业实习', '已批准');

INSERT INTO psychological_warning (
  warning_id, profile_id, warning_level, warning_tag, assess_date, warning_status
) VALUES
  ('PW_0001', 'PF_0001', '低', '学习压力', '2025-10-12', '跟踪中'),
  ('PW_0088', 'PF_0088', '中', '焦虑倾向', '2025-10-13', '干预中'),
  ('PW_1566', 'PF_1566', '高', '情绪波动', '2025-10-14', '重点关注');

INSERT INTO difficulty_identification (
  difficulty_id, profile_id, economic_level, identified_by, identified_date, ident_status
) VALUES
  ('DI_0001', 'PF_0001', '特别困难', '辅导员A', '2025-09-20', '已认定'),
  ('DI_0088', 'PF_0088', '一般困难', '辅导员B', '2025-09-21', '待复核'),
  ('DI_1566', 'PF_1566', '非困难', '辅导员C', '2025-09-22', '不通过');

INSERT INTO counselor_binding (
  binding_id, profile_id, counselor_id, counselor_name, bind_date, binding_status
) VALUES
  ('CB_0001', 'PF_0001', 'CNS001', '吴老师', '2025-09-01', '有效'),
  ('CB_0088', 'PF_0088', 'CNS008', '郑老师', '2025-09-01', '有效'),
  ('CB_1566', 'PF_1566', 'CNS116', '唐老师', '2025-09-01', '暂停');
