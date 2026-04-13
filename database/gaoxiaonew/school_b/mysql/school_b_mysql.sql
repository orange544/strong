CREATE DATABASE IF NOT EXISTS base_info_db
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

USE base_info_db;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS subject_catalog;
DROP TABLE IF EXISTS org_unit;
DROP TABLE IF EXISTS institution_master;

CREATE TABLE institution_master (
  institution_id VARCHAR(16) NOT NULL,
  institution_name VARCHAR(128) NOT NULL,
  institution_code VARCHAR(32) NOT NULL,
  level_type VARCHAR(32) NOT NULL,
  supervising_org VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (institution_id),
  UNIQUE KEY uk_institution_master_code (institution_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE org_unit (
  unit_id VARCHAR(16) NOT NULL,
  institution_id VARCHAR(16) NOT NULL,
  unit_name VARCHAR(128) NOT NULL,
  subject_category VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (unit_id),
  KEY idx_org_unit_institution (institution_id),
  CONSTRAINT fk_org_unit_institution
    FOREIGN KEY (institution_id) REFERENCES institution_master(institution_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE subject_catalog (
  subject_id VARCHAR(16) NOT NULL,
  institution_id VARCHAR(16) NOT NULL,
  subject_name VARCHAR(128) NOT NULL,
  subject_level VARCHAR(32) NOT NULL,
  assessment_grade VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (subject_id),
  KEY idx_subject_catalog_institution (institution_id),
  CONSTRAINT fk_subject_catalog_institution
    FOREIGN KEY (institution_id) REFERENCES institution_master(institution_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;

USE teaching_affairs_db;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS class_schedule_record;
DROP TABLE IF EXISTS curriculum_course;
DROP TABLE IF EXISTS program_info;

CREATE TABLE program_info (
  program_id VARCHAR(16) NOT NULL,
  program_name VARCHAR(128) NOT NULL,
  program_code VARCHAR(32) NOT NULL,
  degree_name VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (program_id),
  UNIQUE KEY uk_program_info_code (program_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE curriculum_course (
  curriculum_id VARCHAR(16) NOT NULL,
  subject_name VARCHAR(128) NOT NULL,
  curriculum_type VARCHAR(32) NOT NULL,
  credit_value DECIMAL(4,1) NOT NULL,
  hours_total INT NOT NULL,
  core_flag TINYINT(1) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (curriculum_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE class_schedule_record (
  class_id VARCHAR(24) NOT NULL,
  curriculum_id VARCHAR(16) NOT NULL,
  faculty_no VARCHAR(16) NOT NULL,
  student_count INT NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (class_id),
  KEY idx_class_schedule_curriculum (curriculum_id),
  CONSTRAINT fk_class_schedule_curriculum
    FOREIGN KEY (curriculum_id) REFERENCES curriculum_course(curriculum_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;

USE student_training_db;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS graduate_degree_info;
DROP TABLE IF EXISTS student_score;
DROP TABLE IF EXISTS learner_profile;

CREATE TABLE learner_profile (
  learner_id VARCHAR(16) NOT NULL,
  stu_no VARCHAR(32) NOT NULL,
  learner_name VARCHAR(64) NOT NULL,
  level_name VARCHAR(32) NOT NULL,
  student_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (learner_id),
  UNIQUE KEY uk_learner_profile_stu_no (stu_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE student_score (
  score_id VARCHAR(24) NOT NULL,
  learner_id VARCHAR(16) NOT NULL,
  usual_mark DECIMAL(5,2) NOT NULL,
  final_mark DECIMAL(5,2) NOT NULL,
  grade_point DECIMAL(3,2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (score_id),
  KEY idx_student_score_learner (learner_id),
  CONSTRAINT fk_student_score_learner
    FOREIGN KEY (learner_id) REFERENCES learner_profile(learner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE graduate_degree_info (
  learner_id VARCHAR(16) NOT NULL,
  dissertation_title VARCHAR(512) NOT NULL,
  degree_name VARCHAR(64) NOT NULL,
  graduate_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (learner_id),
  CONSTRAINT fk_graduate_degree_info_learner
    FOREIGN KEY (learner_id) REFERENCES learner_profile(learner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;

USE admission_employment_db;
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS career_outcome;
DROP TABLE IF EXISTS enrollment_quota;

CREATE TABLE enrollment_quota (
  quota_id VARCHAR(24) NOT NULL,
  enroll_type VARCHAR(32) NOT NULL,
  quota_num INT NOT NULL,
  actual_num INT NOT NULL,
  mean_score DECIMAL(6,2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (quota_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE career_outcome (
  outcome_id VARCHAR(24) NOT NULL,
  learner_id VARCHAR(16) NOT NULL,
  career_type VARCHAR(32) NOT NULL,
  company_name VARCHAR(256) NULL,
  income_band VARCHAR(64) NULL,
  study_school VARCHAR(256) NULL,
  study_stage VARCHAR(64) NULL,
  status VARCHAR(32) NOT NULL,
  PRIMARY KEY (outcome_id),
  KEY idx_career_outcome_learner (learner_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SET FOREIGN_KEY_CHECKS = 1;
