INSERT INTO course (
  course_code, course_name, credit, course_type, dept_code, status
) VALUES
  ('CS101', '程序设计基础', 3.0, '必修', 'CS', '启用'),
  ('MATH204', '高等代数', 4.0, '必修', 'MATH', '启用'),
  ('EE310', '数字电路', 3.5, '专业选修', 'EE', '启用');

INSERT INTO classroom (
  room_id, building_name, room_capacity, room_type, room_status
) VALUES
  ('RM_A101', '教学楼A', 80, '普通教室', '可用'),
  ('RM_B203', '教学楼B', 100, '普通教室', '可用'),
  ('LAB_C305', '实验楼C', 120, '实验室', '可用');

INSERT INTO course_section (
  section_id, course_code, course_name, teacher_no, teacher_name,
  term_id, class_capacity, room_id, class_status
) VALUES
  ('SEC_2025_0001', 'CS101', '程序设计基础', 'T00021', '刘强', '2025-2026-1', 60, 'RM_A101', '已开课'),
  ('SEC_2025_0023', 'MATH204', '高等代数', 'T00108', '周敏', '2025-2026-1', 80, 'RM_B203', '待开课'),
  ('SEC_2025_0108', 'EE310', '数字电路', 'T00356', '陈浩', '2025-2026-2', 120, 'LAB_C305', '停开');

INSERT INTO teacher_assignment (
  assignment_id, section_id, teacher_no, teacher_name, role_type, assigned_at
) VALUES
  ('TA_0001', 'SEC_2025_0001', 'T00021', '刘强', '主讲', '2025-08-25 10:00:00'),
  ('TA_0023', 'SEC_2025_0023', 'T00108', '周敏', '主讲', '2025-08-25 10:05:00'),
  ('TA_0108', 'SEC_2025_0108', 'T00356', '陈浩', '主讲', '2025-08-25 10:10:00');

INSERT INTO term_calendar (
  calendar_id, term_id, week_no, week_start, week_end, teaching_flag
) VALUES
  ('CAL_2025_01', '2025-2026-1', 1, '2025-09-01', '2025-09-07', TRUE),
  ('CAL_2025_02', '2025-2026-1', 2, '2025-09-08', '2025-09-14', TRUE),
  ('CAL_2025_03', '2025-2026-2', 1, '2026-02-23', '2026-03-01', TRUE);

INSERT INTO teaching_schedule (
  schedule_id, section_id, week_day, start_period, end_period,
  room_id, class_mode, schedule_status
) VALUES
  ('SCH_0001', 'SEC_2025_0001', 1, 1, 2, 'RM_A101', '线下', '生效'),
  ('SCH_0023', 'SEC_2025_0023', 3, 3, 4, 'RM_B203', '线下', '草稿'),
  ('SCH_0108', 'SEC_2025_0108', 5, 5, 6, 'LAB_C305', '线下', '停用');

INSERT INTO schedule_adjustment (
  adjustment_id, section_id, original_schedule_id, adjusted_schedule_desc,
  reason, adjusted_by, adjusted_time, adjustment_status
) VALUES
  ('ADJ_0001', 'SEC_2025_0001', 'SCH_0001', '周一1-2节调整为周二3-4节', '节假日冲突', '排课员A', '2025-09-10 09:30:00', '已生效'),
  ('ADJ_0023', 'SEC_2025_0023', 'SCH_0023', '周三3-4节改为周四1-2节', '教室维护', '排课员B', '2025-09-10 10:00:00', '待执行'),
  ('ADJ_0108', 'SEC_2025_0108', 'SCH_0108', '课程暂停，待学院确认', '培养方案调整', '排课员C', '2025-09-10 10:20:00', '已驳回');
