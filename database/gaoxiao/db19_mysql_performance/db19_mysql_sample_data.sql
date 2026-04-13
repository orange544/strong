USE gaoxiao_db19_performance;

INSERT INTO annual_assessment (
  assessment_id, teacher_id, teacher_name, assessment_year, moral_score, discipline_score, base_status
) VALUES
  ('AA_2025_0001', 'T00021', '刘强', 2025, 94.00, 90.00, '已完成'),
  ('AA_2025_0002', 'T00108', '周敏', 2025, 92.00, 89.00, '已完成'),
  ('AA_2025_0003', 'T00356', '陈浩', 2025, 85.00, 83.00, '进行中');

INSERT INTO teaching_workload (
  workload_id, teacher_id, assessment_year, course_count, class_hour, teaching_score
) VALUES
  ('TW_2025_0001', 'T00021', 2025, 3, 196.0, 92.00),
  ('TW_2025_0002', 'T00108', 2025, 3, 182.0, 90.00),
  ('TW_2025_0003', 'T00356', 2025, 2, 148.0, 84.00);

INSERT INTO research_score (
  research_id, teacher_id, assessment_year, paper_score, project_score, total_research_score
) VALUES
  ('RS_2025_0001', 'T00021', 2025, 48.00, 47.00, 95.00),
  ('RS_2025_0002', 'T00108', 2025, 44.00, 42.00, 86.00),
  ('RS_2025_0003', 'T00356', 2025, 38.00, 40.00, 78.00);

INSERT INTO service_score (
  service_id, teacher_id, assessment_year, student_service_score, committee_service_score, total_service_score
) VALUES
  ('SS_2025_0001', 'T00021', 2025, 45.00, 43.00, 88.00),
  ('SS_2025_0002', 'T00108', 2025, 46.00, 45.00, 91.00),
  ('SS_2025_0003', 'T00356', 2025, 40.00, 40.00, 80.00);

INSERT INTO final_performance_result (
  result_id, teacher_id, teacher_name, assessment_year, teaching_score,
  research_score, service_score, final_grade, status
) VALUES
  ('PF_2025_0001', 'T00021', '刘强', 2025, 92.00, 95.00, 88.00, 'A', '已发布'),
  ('PF_2025_0002', 'T00108', '周敏', 2025, 90.00, 86.00, 91.00, 'A', '已发布'),
  ('PF_2025_0003', 'T00356', '陈浩', 2025, 84.00, 78.00, 80.00, 'B', '待审核');
