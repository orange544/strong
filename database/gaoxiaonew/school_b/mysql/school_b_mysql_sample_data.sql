USE base_info_db;

INSERT INTO institution_master (
  institution_id, institution_name, institution_code, level_type, supervising_org, status
) VALUES
('U002', '中州综合大学', '4141019876', '本科', '河南省教育厅', 'active');

INSERT INTO org_unit (
  unit_id, institution_id, unit_name, subject_category, status
) VALUES
('B201', 'U002', '信息工程学院', '工学', 'normal'),
('B202', 'U002', '智能制造学院', '工学', 'normal');

INSERT INTO subject_catalog (
  subject_id, institution_id, subject_name, subject_level, assessment_grade, status
) VALUES
('SUB0812', 'U002', '计算机科学与技术', '一级学科', 'B', 'ongoing');

USE teaching_affairs_db;

INSERT INTO program_info (
  program_id, program_name, program_code, degree_name, status
) VALUES
('P080901', '计算机科学与技术', '080901', '工学学士', 'running'),
('P080213', '智能制造工程', '080213', '工学学士', 'running');

INSERT INTO curriculum_course (
  curriculum_id, subject_name, curriculum_type, credit_value, hours_total, core_flag, status
) VALUES
('CUR3001', '数据库系统原理', '专业核心课', 3.5, 56, 1, 'open'),
('CUR3002', '信息检索', '专业选修课', 2.5, 40, 0, 'open'),
('CUR2101', '智能制造概论', '专业核心课', 3.0, 48, 1, 'open');

INSERT INTO class_schedule_record (
  class_id, curriculum_id, faculty_no, student_count, status
) VALUES
('CL2025F001', 'CUR3001', 'F000221', 79, 'finished'),
('CL2025F002', 'CUR3002', 'F000222', 58, 'finished');

USE student_training_db;

INSERT INTO learner_profile (
  learner_id, stu_no, learner_name, level_name, student_status, status
) VALUES
('L20210088', '20210088', '王悦', '本科', '在读', 'active'),
('L20210089', '20210089', '宋凯', '本科', '在读', 'active'),
('L20210090', '20210090', '冯雪', '本科', '毕业', 'active'),
('L20220031', '20220031', '马辰', '本科', '在读', 'active');

INSERT INTO student_score (
  score_id, learner_id, usual_mark, final_mark, grade_point, status
) VALUES
('SC2023012', 'L20210088', 90.00, 87.00, 3.80, 'published'),
('SC2023013', 'L20210089', 84.00, 80.00, 3.30, 'published'),
('SC2023014', 'L20210090', 92.00, 90.00, 4.00, 'published');

INSERT INTO graduate_degree_info (
  learner_id, dissertation_title, degree_name, graduate_status, status
) VALUES
('L20210090', '跨校异构数据语义统一研究', '工学学士', '正常毕业', 'completed');

USE admission_employment_db;

INSERT INTO enrollment_quota (
  quota_id, enroll_type, quota_num, actual_num, mean_score, status
) VALUES
('EQ2025-01', '普通本科', 180, 177, 596.40, 'completed');

INSERT INTO career_outcome (
  outcome_id, learner_id, career_type, company_name, income_band,
  study_school, study_stage, status
) VALUES
('CO2025-301', 'L20210090', '升学', NULL, NULL, '华中科技大学', '硕士', 'confirmed');