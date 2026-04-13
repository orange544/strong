INSERT INTO research_project (
  project_id, project_code, project_name, leader_id, leader_name,
  leader_gender, leader_age, dept_id, project_level, fund_amount, project_status
) VALUES
  ('PRJ_0001', 'NSFC2025A01', '面向多数据域的语义统一机制研究', 'T00021', '刘强', '男', 42, 'D010', '国家级', 580000.00, '在研'),
  ('PRJ_0002', 'PROV2025B15', '面向高校数据共享的联邦知识图谱构建', 'T00108', '周敏', '女', 38, 'D023', '省部级', 120000.00, '结题验收'),
  ('PRJ_0003', 'SCH2025C08', '异构数据库字段描述自动生成方法', 'T00356', '陈浩', '男', 47, 'D018', '校级', 30000.00, '申报中');

INSERT INTO project_application (
  application_id, project_id, apply_date, applicant_org, application_status, review_comment
) VALUES
  ('PAPP_0001', 'PRJ_0001', '2025-03-01', '计算机学院', '已立项', '材料完整，建议重点支持'),
  ('PAPP_0002', 'PRJ_0002', '2025-03-08', '数学学院', '已立项', '目标明确，实施可行'),
  ('PAPP_0003', 'PRJ_0003', '2025-03-15', '电气学院', '评审中', '需补充预实验数据');

INSERT INTO project_member (
  member_id, project_id, teacher_id, teacher_name, member_role, contribution_ratio
) VALUES
  ('PM_0001', 'PRJ_0001', 'T00021', '刘强', '负责人', 50.00),
  ('PM_0002', 'PRJ_0002', 'T00108', '周敏', '负责人', 45.00),
  ('PM_0003', 'PRJ_0003', 'T00356', '陈浩', '负责人', 60.00);

INSERT INTO project_budget (
  budget_id, project_id, budget_year, equipment_budget, labor_budget, travel_budget, budget_status
) VALUES
  ('PBUD_0001', 'PRJ_0001', 2025, 200000.00, 250000.00, 50000.00, '已审批'),
  ('PBUD_0002', 'PRJ_0002', 2025, 30000.00, 60000.00, 15000.00, '已执行'),
  ('PBUD_0003', 'PRJ_0003', 2025, 10000.00, 12000.00, 5000.00, '待审批');

INSERT INTO project_midterm_review (
  review_id, project_id, review_date, review_result, reviewer, review_comment
) VALUES
  ('PMR_0001', 'PRJ_0001', '2026-06-10', '通过', '专家组A', '阶段成果显著'),
  ('PMR_0002', 'PRJ_0002', '2026-06-15', '通过', '专家组B', '按计划推进'),
  ('PMR_0003', 'PRJ_0003', '2026-06-20', '延期复审', '专家组C', '样本规模不足');

INSERT INTO project_fund_execution (
  execution_id, project_id, expense_type, expense_amount, expense_date, execution_status
) VALUES
  ('PFE_0001', 'PRJ_0001', '设备采购', 120000.00, '2025-11-18', '已报销'),
  ('PFE_0002', 'PRJ_0002', '劳务支出', 35000.00, '2025-12-05', '已支付'),
  ('PFE_0003', 'PRJ_0003', '差旅支出', 2800.00, '2025-12-20', '待审核');

INSERT INTO project_closure (
  closure_id, project_id, closure_date, closure_result, archive_flag, closure_status
) VALUES
  ('PCLOSE_0001', 'PRJ_0001', '2027-12-31', '优秀', TRUE, '归档'),
  ('PCLOSE_0002', 'PRJ_0002', '2027-10-31', '合格', TRUE, '归档'),
  ('PCLOSE_0003', 'PRJ_0003', '2028-06-30', '待定', FALSE, '未结题');
