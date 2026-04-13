CREATE DATABASE IF NOT EXISTS university_base_db
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS faculty_hr_db
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS teaching_affairs_db
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS student_training_db
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS admission_employment_db
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

SET NAMES utf8mb4;

USE university_base_db;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS discipline_info;
DROP TABLE IF EXISTS college_department;
DROP TABLE IF EXISTS university_profile;

CREATE TABLE university_profile (
  university_id VARCHAR(16) NOT NULL,
  university_name VARCHAR(128) NOT NULL,
  university_code VARCHAR(32) NOT NULL,
  university_type VARCHAR(32) NOT NULL,
  education_level VARCHAR(32) NOT NULL,
  province VARCHAR(32) NOT NULL,
  city VARCHAR(32) NOT NULL,
  governing_body VARCHAR(64) NOT NULL,
  founded_year INT NOT NULL,
  is_double_first_class TINYINT(1) NOT NULL,
  undergrad_population INT NOT NULL,
  postgrad_population INT NOT NULL,
  faculty_count INT NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (university_id),
  UNIQUE KEY uk_university_profile_code (university_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE college_department (
  dept_id VARCHAR(16) NOT NULL,
  university_id VARCHAR(16) NOT NULL,
  dept_name VARCHAR(128) NOT NULL,
  dept_name_en VARCHAR(256) NOT NULL,
  discipline_category VARCHAR(64) NOT NULL,
  director_name VARCHAR(64) NOT NULL,
  teacher_total INT NOT NULL,
  student_total INT NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (dept_id),
  KEY idx_college_department_university (university_id),
  CONSTRAINT fk_college_department_university
    FOREIGN KEY (university_id) REFERENCES university_profile(university_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE discipline_info (
  discipline_id VARCHAR(16) NOT NULL,
  university_id VARCHAR(16) NOT NULL,
  dept_id VARCHAR(16) NOT NULL,
  discipline_name VARCHAR(128) NOT NULL,
  discipline_code VARCHAR(32) NOT NULL,
  discipline_level VARCHAR(32) NOT NULL,
  evaluation_result VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (discipline_id),
  UNIQUE KEY uk_discipline_info_code (discipline_code),
  KEY idx_discipline_info_university (university_id),
  KEY idx_discipline_info_dept (dept_id),
  CONSTRAINT fk_discipline_info_university
    FOREIGN KEY (university_id) REFERENCES university_profile(university_id),
  CONSTRAINT fk_discipline_info_dept
    FOREIGN KEY (dept_id) REFERENCES college_department(dept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;

USE faculty_hr_db;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS teacher_title_history;
DROP TABLE IF EXISTS teacher_research_summary;
DROP TABLE IF EXISTS teacher;

CREATE TABLE teacher (
  teacher_id VARCHAR(16) NOT NULL,
  emp_no VARCHAR(32) NOT NULL,
  teacher_name VARCHAR(64) NOT NULL,
  gender VARCHAR(8) NOT NULL,
  birth_year INT NOT NULL,
  dept_id VARCHAR(16) NOT NULL,
  title VARCHAR(32) NOT NULL,
  position_type VARCHAR(32) NOT NULL,
  degree VARCHAR(32) NOT NULL,
  alma_mater VARCHAR(128) NOT NULL,
  research_area VARCHAR(256) NOT NULL,
  employment_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (teacher_id),
  UNIQUE KEY uk_teacher_emp_no (emp_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE teacher_research_summary (
  teacher_id VARCHAR(16) NOT NULL,
  paper_count INT NOT NULL,
  patent_count INT NOT NULL,
  project_count INT NOT NULL,
  received_funding_3y DECIMAL(14,2) NOT NULL,
  sci_paper_count INT NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (teacher_id),
  CONSTRAINT fk_teacher_research_summary_teacher
    FOREIGN KEY (teacher_id) REFERENCES teacher(teacher_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE teacher_title_history (
  record_id VARCHAR(24) NOT NULL,
  teacher_id VARCHAR(16) NOT NULL,
  title_name VARCHAR(32) NOT NULL,
  effective_date DATE NOT NULL,
  appraisal_result VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (record_id),
  KEY idx_teacher_title_history_teacher (teacher_id),
  CONSTRAINT fk_teacher_title_history_teacher
    FOREIGN KEY (teacher_id) REFERENCES teacher(teacher_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;

USE teaching_affairs_db;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS course_offering;
DROP TABLE IF EXISTS course_info;
DROP TABLE IF EXISTS major_catalog;

CREATE TABLE major_catalog (
  major_id VARCHAR(16) NOT NULL,
  major_name VARCHAR(128) NOT NULL,
  major_code VARCHAR(32) NOT NULL,
  degree_type VARCHAR(64) NOT NULL,
  admission_category VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (major_id),
  UNIQUE KEY uk_major_catalog_code (major_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE course_info (
  course_id VARCHAR(16) NOT NULL,
  course_name VARCHAR(128) NOT NULL,
  course_type VARCHAR(32) NOT NULL,
  credit DECIMAL(4,1) NOT NULL,
  total_hours INT NOT NULL,
  is_core_course TINYINT(1) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (course_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE course_offering (
  offering_id VARCHAR(24) NOT NULL,
  course_id VARCHAR(16) NOT NULL,
  academic_term VARCHAR(32) NOT NULL,
  teacher_id VARCHAR(16) NOT NULL,
  enrolled_count INT NOT NULL,
  avg_score DECIMAL(5,2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (offering_id),
  KEY idx_course_offering_course (course_id),
  CONSTRAINT fk_course_offering_course
    FOREIGN KEY (course_id) REFERENCES course_info(course_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;

USE student_training_db;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS degree_graduation;
DROP TABLE IF EXISTS course_grade;
DROP TABLE IF EXISTS student_record;

CREATE TABLE student_record (
  student_id VARCHAR(16) NOT NULL,
  student_no VARCHAR(32) NOT NULL,
  student_name VARCHAR(64) NOT NULL,
  education_level VARCHAR(32) NOT NULL,
  major_id VARCHAR(16) NOT NULL,
  current_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (student_id),
  UNIQUE KEY uk_student_record_no (student_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE course_grade (
  grade_id VARCHAR(24) NOT NULL,
  student_id VARCHAR(16) NOT NULL,
  usual_score DECIMAL(5,2) NOT NULL,
  final_score DECIMAL(5,2) NOT NULL,
  total_score DECIMAL(5,2) NOT NULL,
  gpa_score DECIMAL(3,2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (grade_id),
  KEY idx_course_grade_student (student_id),
  CONSTRAINT fk_course_grade_student
    FOREIGN KEY (student_id) REFERENCES student_record(student_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE degree_graduation (
  student_id VARCHAR(16) NOT NULL,
  thesis_title VARCHAR(512) NOT NULL,
  defense_result VARCHAR(32) NOT NULL,
  degree_type VARCHAR(64) NOT NULL,
  graduation_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (student_id),
  CONSTRAINT fk_degree_graduation_student
    FOREIGN KEY (student_id) REFERENCES student_record(student_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;

USE admission_employment_db;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS graduate_employment;
DROP TABLE IF EXISTS admission_plan;

CREATE TABLE admission_plan (
  admission_id VARCHAR(24) NOT NULL,
  admission_type VARCHAR(32) NOT NULL,
  planned_count INT NOT NULL,
  admitted_count INT NOT NULL,
  avg_score DECIMAL(6,2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (admission_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE graduate_employment (
  record_id VARCHAR(24) NOT NULL,
  student_id VARCHAR(16) NOT NULL,
  destination_type VARCHAR(32) NOT NULL,
  employer_name VARCHAR(256) NULL,
  industry_sector VARCHAR(128) NULL,
  work_city VARCHAR(64) NULL,
  salary_range VARCHAR(64) NULL,
  study_level VARCHAR(64) NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (record_id),
  KEY idx_graduate_employment_student (student_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;
