USE gaoxiao_db18_staff;

INSERT INTO staff_basic (
  staff_id, employee_no, name, sex, age_year, department_id, post_name, contract_status
) VALUES
  ('S00011', 'EMP00011', '赵琳', '女', 35, 'D_AFFAIR', '辅导员', '在职'),
  ('S00012', 'EMP00012', '王涛', '男', 41, 'D_LOGI', '宿舍管理员', '在职'),
  ('S00013', 'EMP00013', '陈倩', '女', 29, 'D_LIB', '图书管理员', '试用');

INSERT INTO position_category (
  category_id, category_name, level_name, dept_scope, category_status
) VALUES
  ('PC_0001', '学生事务岗', '中级', '学生工作处', '启用'),
  ('PC_0002', '后勤保障岗', '高级', '后勤中心', '启用'),
  ('PC_0003', '图书服务岗', '初级', '图书馆', '停用');

INSERT INTO labor_contract (
  contract_id, staff_id, contract_type, start_date, end_date, contract_status
) VALUES
  ('LC_0001', 'S00011', '固定期限', '2024-09-01', '2027-08-31', '生效'),
  ('LC_0002', 'S00012', '无固定期限', '2020-03-01', '2035-02-28', '生效'),
  ('LC_0003', 'S00013', '试用合同', '2026-01-01', '2026-12-31', '试用');

INSERT INTO staff_assignment (
  assignment_id, staff_id, department_id, post_name, start_date, end_date, assignment_status
) VALUES
  ('SA_0011', 'S00011', 'D_AFFAIR', '辅导员', '2024-09-01', NULL, '在岗'),
  ('SA_0012', 'S00012', 'D_LOGI', '宿舍管理员', '2020-03-01', NULL, '在岗'),
  ('SA_0013', 'S00013', 'D_LIB', '图书管理员', '2026-01-01', NULL, '试用');

INSERT INTO staff_change_record (
  change_id, staff_id, change_type, old_post, new_post, change_date, change_reason
) VALUES
  ('SCR_0001', 'S00011', '调岗', '学生干事', '辅导员', '2025-09-01', '岗位晋升'),
  ('SCR_0002', 'S00012', '续聘', '宿舍管理员', '宿舍管理员', '2024-03-01', '合同续签'),
  ('SCR_0003', 'S00013', '入职', '无', '图书管理员', '2026-01-01', '新聘人员');
