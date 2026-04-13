CREATE DATABASE IF NOT EXISTS gaoxiao_db19_performance
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE gaoxiao_db19_performance;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS final_performance_result;
DROP TABLE IF EXISTS service_score;
DROP TABLE IF EXISTS research_score;
DROP TABLE IF EXISTS teaching_workload;
DROP TABLE IF EXISTS annual_assessment;

CREATE TABLE annual_assessment (
  assessment_id VARCHAR(32) NOT NULL,
  teacher_id VARCHAR(20) NOT NULL,
  teacher_name VARCHAR(50) NOT NULL,
  assessment_year INT NOT NULL,
  moral_score DECIMAL(5,2) NOT NULL,
  discipline_score DECIMAL(5,2) NOT NULL,
  base_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (assessment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE teaching_workload (
  workload_id VARCHAR(32) NOT NULL,
  teacher_id VARCHAR(20) NOT NULL,
  assessment_year INT NOT NULL,
  course_count INT NOT NULL,
  class_hour DECIMAL(6,1) NOT NULL,
  teaching_score DECIMAL(5,2) NOT NULL,
  PRIMARY KEY (workload_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE research_score (
  research_id VARCHAR(32) NOT NULL,
  teacher_id VARCHAR(20) NOT NULL,
  assessment_year INT NOT NULL,
  paper_score DECIMAL(5,2) NOT NULL,
  project_score DECIMAL(5,2) NOT NULL,
  total_research_score DECIMAL(5,2) NOT NULL,
  PRIMARY KEY (research_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE service_score (
  service_id VARCHAR(32) NOT NULL,
  teacher_id VARCHAR(20) NOT NULL,
  assessment_year INT NOT NULL,
  student_service_score DECIMAL(5,2) NOT NULL,
  committee_service_score DECIMAL(5,2) NOT NULL,
  total_service_score DECIMAL(5,2) NOT NULL,
  PRIMARY KEY (service_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE final_performance_result (
  result_id VARCHAR(32) NOT NULL,
  teacher_id VARCHAR(20) NOT NULL,
  teacher_name VARCHAR(50) NOT NULL,
  assessment_year INT NOT NULL,
  teaching_score DECIMAL(5,2) NOT NULL,
  research_score DECIMAL(5,2) NOT NULL,
  service_score DECIMAL(5,2) NOT NULL,
  final_grade VARCHAR(5) NOT NULL,
  status VARCHAR(20) NOT NULL,
  PRIMARY KEY (result_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
