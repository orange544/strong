CREATE DATABASE IF NOT EXISTS gaoxiao_db1_admission
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE gaoxiao_db1_admission;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS freshman_report_task;
DROP TABLE IF EXISTS admission_result;
DROP TABLE IF EXISTS application_form;
DROP TABLE IF EXISTS major_plan;
DROP TABLE IF EXISTS province_batch;
DROP TABLE IF EXISTS candidate;

CREATE TABLE candidate (
  candidate_id VARCHAR(32) NOT NULL,
  exam_no VARCHAR(32) NOT NULL,
  candidate_name VARCHAR(50) NOT NULL,
  gender VARCHAR(10) NOT NULL,
  age TINYINT UNSIGNED NOT NULL,
  id_card_no VARCHAR(18) NOT NULL,
  mobile_no VARCHAR(20) NOT NULL,
  province_code VARCHAR(10) NOT NULL,
  target_major_code VARCHAR(20) NOT NULL,
  admission_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (candidate_id),
  UNIQUE KEY uk_candidate_exam_no (exam_no),
  UNIQUE KEY uk_candidate_id_card_no (id_card_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE application_form (
  app_id VARCHAR(32) NOT NULL,
  candidate_id VARCHAR(32) NOT NULL,
  batch_code VARCHAR(20) NOT NULL,
  first_major_code VARCHAR(20) NOT NULL,
  second_major_code VARCHAR(20) NOT NULL,
  submit_time DATETIME NOT NULL,
  form_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (app_id),
  KEY idx_application_candidate (candidate_id),
  CONSTRAINT fk_application_candidate
    FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE admission_result (
  result_id VARCHAR(32) NOT NULL,
  candidate_id VARCHAR(32) NOT NULL,
  admit_major_code VARCHAR(20) NOT NULL,
  admit_type VARCHAR(20) NOT NULL,
  score DECIMAL(5,1) NOT NULL,
  admit_status VARCHAR(20) NOT NULL,
  publish_time DATETIME NOT NULL,
  PRIMARY KEY (result_id),
  KEY idx_result_candidate (candidate_id),
  CONSTRAINT fk_result_candidate
    FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE major_plan (
  plan_id VARCHAR(32) NOT NULL,
  major_code VARCHAR(20) NOT NULL,
  major_name VARCHAR(100) NOT NULL,
  province_code VARCHAR(10) NOT NULL,
  plan_year INT NOT NULL,
  planned_count INT NOT NULL,
  PRIMARY KEY (plan_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE province_batch (
  batch_id VARCHAR(32) NOT NULL,
  province_code VARCHAR(10) NOT NULL,
  batch_name VARCHAR(40) NOT NULL,
  min_score INT NOT NULL,
  exam_year INT NOT NULL,
  PRIMARY KEY (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE freshman_report_task (
  task_id VARCHAR(32) NOT NULL,
  candidate_id VARCHAR(32) NOT NULL,
  report_date DATE NOT NULL,
  campus_code VARCHAR(20) NOT NULL,
  dorm_assign_status VARCHAR(20) NOT NULL,
  task_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (task_id),
  KEY idx_report_candidate (candidate_id),
  CONSTRAINT fk_report_candidate
    FOREIGN KEY (candidate_id) REFERENCES candidate(candidate_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
