INSERT INTO graduate_student (
  graduate_student_id, stu_no, name, sex, student_age, cert_no,
  phone_no, college_code, discipline_code, reg_status
) VALUES
  ('PG_2025_0001', 'YJS250001', '赵婷', '女', 24, '320104200102015628', '13612349876', 'GC010', '0812', '在籍'),
  ('PG_2025_0088', 'YJS250088', '韩松', '男', 26, '421127199911236519', '13899887766', 'GC015', '0835', '休学'),
  ('PG_2025_0116', 'YJS250116', '陈雪', '女', 23, '110108200203145627', '13577889966', 'GC022', '0702', '结业');

INSERT INTO supervisor_relation (
  relation_id, graduate_student_id, supervisor_id, supervisor_name,
  relation_type, start_date, end_date, relation_status
) VALUES
  ('SR_0001', 'PG_2025_0001', 'SUP001', '王建', '主导师', '2025-09-01', NULL, '有效'),
  ('SR_0088', 'PG_2025_0088', 'SUP008', '刘楠', '主导师', '2025-09-01', NULL, '有效'),
  ('SR_0116', 'PG_2025_0116', 'SUP116', '周琴', '联合导师', '2025-09-01', '2026-12-31', '终止');

INSERT INTO cultivation_plan (
  plan_id,
  graduate_student_id,
  term_id,
  required_credit,
  completed_credit,
  plan_status
) VALUES
  ('CP_0001', 'PG_2025_0001', '2025-2026-1', 32.0, 10.0, '执行中'),
  ('CP_0088', 'PG_2025_0088', '2025-2026-1', 30.0, 6.0, '暂停'),
  ('CP_0116', 'PG_2025_0116', '2025-2026-1', 34.0, 34.0, '已完成');

INSERT INTO proposal_review (
  proposal_id, graduate_student_id, topic_title, reviewer,
  review_result, review_time, review_comment
) VALUES
  ('PR_0001', 'PG_2025_0001', '联邦学习在教育数据共享中的应用研究', '评审专家A', '通过', '2026-01-05 14:00:00', '选题明确，方法可行'),
  ('PR_0088', 'PG_2025_0088', '高维图像检索算法优化', '评审专家B', '修改后通过', '2026-01-05 14:20:00', '需补充实验对比'),
  ('PR_0116', 'PG_2025_0116', '新能源系统稳定性分析', '评审专家C', '未通过', '2026-01-05 14:40:00', '研究范围过宽');

INSERT INTO midterm_assessment (
  midterm_id, graduate_student_id, assessment_score, assessment_result,
  assessor, assess_time, assess_comment
) VALUES
  ('MA_0001', 'PG_2025_0001', 91.5, '通过', '导师王建', '2026-06-10 10:00:00', '进展顺利'),
  ('MA_0088', 'PG_2025_0088', 75.0, '待改进', '导师刘楠', '2026-06-10 10:15:00', '阶段成果较弱'),
  ('MA_0116', 'PG_2025_0116', 58.0, '不通过', '导师周琴', '2026-06-10 10:30:00', '关键任务未完成');

INSERT INTO thesis_basic (
  thesis_id, graduate_student_id, thesis_title, research_field,
  submit_time, thesis_status
) VALUES
  ('TH_0001', 'PG_2025_0001', '去中心化语义映射框架在高校数据治理中的实践', '数据治理', '2027-03-01 09:00:00', '送审中'),
  ('TH_0088', 'PG_2025_0088', '多模态知识图谱检索性能提升研究', '人工智能', '2027-03-01 09:20:00', '修改中'),
  ('TH_0116', 'PG_2025_0116', '电力电子系统中的鲁棒控制方法', '电气工程', '2027-03-01 09:40:00', '未提交');

INSERT INTO blind_review_task (
  task_id, thesis_id, reviewer_code, dispatch_time, review_status, score
) VALUES
  ('BRT_0001', 'TH_0001', 'RV1001', '2027-03-05 08:00:00', '已回收', 88.5),
  ('BRT_0088', 'TH_0088', 'RV1002', '2027-03-05 08:10:00', '评审中', 0.0),
  ('BRT_0116', 'TH_0116', 'RV1003', '2027-03-05 08:20:00', '已退回', 0.0);

INSERT INTO thesis_defense (
  defense_id, thesis_id, defense_time, defense_room, defense_result, committee_chair
) VALUES
  ('TD_0001', 'TH_0001', '2027-05-20 14:00:00', 'YJ_A301', '通过', '钱明教授'),
  ('TD_0088', 'TH_0088', '2027-05-20 15:00:00', 'YJ_A302', '暂缓', '宋华教授'),
  ('TD_0116', 'TH_0116', '2027-05-20 16:00:00', 'YJ_A303', '不通过', '冯涛教授');

INSERT INTO degree_award (
  award_id, graduate_student_id, degree_name, award_date, award_status, degree_cert_no
) VALUES
  ('DA_0001', 'PG_2025_0001', '工学硕士', '2027-06-30', '已授予', 'DEG20270001'),
  ('DA_0088', 'PG_2025_0088', '理学硕士', '2027-06-30', '待审议', 'DEG20270088'),
  ('DA_0116', 'PG_2025_0116', '工学硕士', '2027-06-30', '未授予', 'DEG20270116');
