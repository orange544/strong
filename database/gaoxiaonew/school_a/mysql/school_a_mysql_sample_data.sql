USE university_base_db;

INSERT INTO university_profile (
  university_id, university_name, university_code, university_type, education_level,
  province, city, governing_body, founded_year, is_double_first_class,
  undergrad_population, postgrad_population, faculty_count, status
) VALUES
('U001', '华东先进工业大学', '4132014587', '理工类', '本科', '江苏省', '南京市', '江苏省教育厅', 1958, 1, 24860, 8360, 1825, 'active');

INSERT INTO college_department (
  dept_id, university_id, dept_name, dept_name_en, discipline_category,
  director_name, teacher_total, student_total, status
) VALUES
('D101', 'U001', '计算机科学与工程学院', 'School of Computer Science and Engineering', '工学', '王建国', 126, 2350, 'normal'),
('D102', 'U001', '机械工程学院', 'School of Mechanical Engineering', '工学', '赵海峰', 118, 2140, 'normal');

INSERT INTO discipline_info (
  discipline_id, university_id, dept_id, discipline_name, discipline_code,
  discipline_level, evaluation_result, status
) VALUES
('DIS0812', 'U001', 'D101', '计算机科学与技术', '0812', '一级学科', 'B+', 'ongoing');

USE faculty_hr_db;

INSERT INTO teacher (
  teacher_id, emp_no, teacher_name, gender, birth_year, dept_id, title,
  position_type, degree, alma_mater, research_area, employment_status, status
) VALUES
('T000123', '20140327', '李明', '男', 1981, 'D101', '教授', '专任教师', '博士', '东南大学', '数据挖掘与知识工程', '在岗', 'active'),
('T000124', '20150718', '陈雪', '女', 1986, 'D101', '副教授', '专任教师', '博士', '南京大学', '知识图谱与语义检索', '在岗', 'active'),
('T000125', '20121109', '周强', '男', 1979, 'D102', '教授', '专任教师', '博士', '哈尔滨工业大学', '智能制造与工业控制', '在岗', 'active'),
('T000126', '20190915', '孙蕾', '女', 1990, 'D101', '讲师', '教学科研岗', '博士', '华中科技大学', '教育数据分析', '在岗', 'active');

INSERT INTO teacher_research_summary (
  teacher_id, paper_count, patent_count, project_count, received_funding_3y, sci_paper_count, status
) VALUES
('T000123', 36, 8, 6, 1860000.00, 14, 'valid'),
('T000124', 22, 3, 4, 980000.00, 9, 'valid');

INSERT INTO teacher_title_history (
  record_id, teacher_id, title_name, effective_date, appraisal_result, status
) VALUES
('TH2021001', 'T000123', '教授', '2021-12-01', '通过', 'archived');

USE teaching_affairs_db;

INSERT INTO major_catalog (
  major_id, major_name, major_code, degree_type, admission_category, status
) VALUES
('M080901', '计算机科学与技术', '080901', '工学学士', '普通本科', 'running'),
('M080202', '机械设计制造及其自动化', '080202', '工学学士', '普通本科', 'running');

INSERT INTO course_info (
  course_id, course_name, course_type, credit, total_hours, is_core_course, status
) VALUES
('CSE3001', '数据库系统原理', '专业核心课', 3.5, 56, 1, 'open'),
('CSE3002', '知识图谱导论', '专业选修课', 2.0, 32, 0, 'open'),
('MEC2001', '机械原理', '专业核心课', 3.0, 48, 1, 'open');

INSERT INTO course_offering (
  offering_id, course_id, academic_term, teacher_id, enrolled_count, avg_score, status
) VALUES
('OF2025F001', 'CSE3001', '2025-秋', 'T000123', 86, 79.50, 'finished'),
('OF2025F002', 'CSE3002', '2025-秋', 'T000124', 42, 83.10, 'finished');

USE student_training_db;

INSERT INTO student_record (
  student_id, student_no, student_name, education_level, major_id, current_status, status
) VALUES
('S202100234', '202100234', '张婷', '本科', 'M080901', '在读', 'active'),
('S202100235', '202100235', '刘洋', '本科', 'M080901', '在读', 'active'),
('S202100236', '202100236', '黄晨', '本科', 'M080202', '毕业', 'active'),
('S202200101', '202200101', '顾欣', '本科', 'M080901', '在读', 'active');

INSERT INTO course_grade (
  grade_id, student_id, usual_score, final_score, total_score, gpa_score, status
) VALUES
('G890123', 'S202100234', 88.00, 84.00, 85.00, 3.70, 'published'),
('G890124', 'S202100235', 91.00, 89.00, 89.80, 3.90, 'published'),
('G890125', 'S202100236', 82.00, 79.00, 80.20, 3.20, 'published');

INSERT INTO degree_graduation (
  student_id, thesis_title, defense_result, degree_type, graduation_status, status
) VALUES
('S202100236', '面向多源异构数据的语义统一方法研究', '通过', '工学学士', '正常毕业', 'completed');

USE admission_employment_db;

INSERT INTO admission_plan (
  admission_id, admission_type, planned_count, admitted_count, avg_score, status
) VALUES
('AD2025-080901-01', '普通本科', 210, 208, 608.30, 'completed');

INSERT INTO graduate_employment (
  record_id, student_id, destination_type, employer_name, industry_sector,
  work_city, salary_range, study_level, status
) VALUES
('E202500123', 'S202100236', '协议就业', '华为技术有限公司', '信息传输、软件和信息技术服务业', '深圳', '15k-20k', NULL, 'confirmed');