USE gaoxiao_db10_aid;

INSERT INTO scholarship_apply (
  apply_id, sid, person_name, sex, age, college_code,
  scholarship_type, apply_term, score_rank, award_status, grant_amount
) VALUES
  ('SA_0001', 'STU_2025_0001', '张晨', '男', 18, 'GC010', '国家奖学金', '2025-2026-1', 3, '初审通过', 8000.00),
  ('SA_0002', 'STU_2025_0088', '李媛', '女', 19, 'GC023', '学业一等奖学金', '2025-2026-1', 1, '已公示', 3000.00),
  ('SA_0003', 'STU_2025_1566', '王磊', '未知', 17, 'GC018', '励志奖学金', '2025-2026-2', 12, '待审核', 5000.00);

INSERT INTO grant_review (
  review_id, apply_id, reviewer, review_result, review_time, review_comment
) VALUES
  ('GR_0001', 'SA_0001', '资助老师A', '通过', '2025-11-12 10:00:00', '材料齐全，符合条件'),
  ('GR_0002', 'SA_0002', '资助老师B', '通过', '2025-11-12 10:20:00', '成绩排名优异'),
  ('GR_0003', 'SA_0003', '资助老师C', '补充材料', '2025-11-12 10:40:00', '需补交困难认定证明');

INSERT INTO workstudy_post (
  post_id, sid, post_name, dept_name, hourly_pay, post_status, start_date
) VALUES
  ('WS_0001', 'STU_2025_0001', '图书馆助理', '图书馆', 18.00, '在岗', '2025-09-15'),
  ('WS_0002', 'STU_2025_0088', '实验室助管', '理学院', 20.00, '待上岗', '2025-10-01'),
  ('WS_0003', 'STU_2025_1566', '信息录入员', '学生处', 16.00, '离岗', '2025-07-01');

INSERT INTO student_loan (
  loan_id, sid, loan_type, loan_amount, bank_name, loan_status, disburse_date
) VALUES
  ('LOAN_0001', 'STU_2025_0001', '国家助学贷款', 12000.00, '中国银行', '发放中', '2025-10-20'),
  ('LOAN_0002', 'STU_2025_0088', '国家助学贷款', 8000.00, '农业银行', '已发放', '2025-10-18'),
  ('LOAN_0003', 'STU_2025_1566', '生源地信用助学贷款', 10000.00, '建设银行', '审核中', NULL);

INSERT INTO aid_distribution (
  distribution_id, apply_id, sid, aid_type, amount, grant_status, pay_time, voucher_no
) VALUES
  ('AD_0001', 'SA_0001', 'STU_2025_0001', '奖学金', 8000.00, '已发放', '2025-12-01 14:00:00', 'VCH20250001'),
  ('AD_0002', 'SA_0002', 'STU_2025_0088', '奖学金', 3000.00, '处理中', '2025-12-02 10:30:00', 'VCH20250002'),
  ('AD_0003', 'SA_0003', 'STU_2025_1566', '奖学金', 5000.00, '待发放', NULL, NULL);
