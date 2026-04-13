SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS project_closure;
DROP TABLE IF EXISTS project_fund_execution;
DROP TABLE IF EXISTS project_midterm_review;
DROP TABLE IF EXISTS project_budget;
DROP TABLE IF EXISTS project_member;
DROP TABLE IF EXISTS project_application;
DROP TABLE IF EXISTS research_project;

CREATE TABLE research_project (
  project_id VARCHAR(32) PRIMARY KEY,
  project_code VARCHAR(40) NOT NULL UNIQUE,
  project_name VARCHAR(300) NOT NULL,
  leader_id VARCHAR(20) NOT NULL,
  leader_name VARCHAR(50) NOT NULL,
  leader_gender VARCHAR(10) NOT NULL,
  leader_age SMALLINT NOT NULL,
  dept_id VARCHAR(20) NOT NULL,
  project_level VARCHAR(20) NOT NULL,
  fund_amount NUMERIC(14,2) NOT NULL,
  project_status VARCHAR(20) NOT NULL
);

CREATE TABLE project_application (
  application_id VARCHAR(32) PRIMARY KEY,
  project_id VARCHAR(32) NOT NULL REFERENCES research_project(project_id) ON DELETE CASCADE,
  apply_date DATE NOT NULL,
  applicant_org VARCHAR(100) NOT NULL,
  application_status VARCHAR(20) NOT NULL,
  review_comment VARCHAR(300) NOT NULL
);

CREATE TABLE project_member (
  member_id VARCHAR(32) PRIMARY KEY,
  project_id VARCHAR(32) NOT NULL REFERENCES research_project(project_id) ON DELETE CASCADE,
  teacher_id VARCHAR(20) NOT NULL,
  teacher_name VARCHAR(50) NOT NULL,
  member_role VARCHAR(30) NOT NULL,
  contribution_ratio NUMERIC(5,2) NOT NULL
);

CREATE TABLE project_budget (
  budget_id VARCHAR(32) PRIMARY KEY,
  project_id VARCHAR(32) NOT NULL REFERENCES research_project(project_id) ON DELETE CASCADE,
  budget_year INT NOT NULL,
  equipment_budget NUMERIC(14,2) NOT NULL,
  labor_budget NUMERIC(14,2) NOT NULL,
  travel_budget NUMERIC(14,2) NOT NULL,
  budget_status VARCHAR(20) NOT NULL
);

CREATE TABLE project_midterm_review (
  review_id VARCHAR(32) PRIMARY KEY,
  project_id VARCHAR(32) NOT NULL REFERENCES research_project(project_id) ON DELETE CASCADE,
  review_date DATE NOT NULL,
  review_result VARCHAR(20) NOT NULL,
  reviewer VARCHAR(50) NOT NULL,
  review_comment VARCHAR(300) NOT NULL
);

CREATE TABLE project_fund_execution (
  execution_id VARCHAR(32) PRIMARY KEY,
  project_id VARCHAR(32) NOT NULL REFERENCES research_project(project_id) ON DELETE CASCADE,
  expense_type VARCHAR(30) NOT NULL,
  expense_amount NUMERIC(14,2) NOT NULL,
  expense_date DATE NOT NULL,
  execution_status VARCHAR(20) NOT NULL
);

CREATE TABLE project_closure (
  closure_id VARCHAR(32) PRIMARY KEY,
  project_id VARCHAR(32) NOT NULL REFERENCES research_project(project_id) ON DELETE CASCADE,
  closure_date DATE NOT NULL,
  closure_result VARCHAR(20) NOT NULL,
  archive_flag BOOLEAN NOT NULL,
  closure_status VARCHAR(20) NOT NULL
);
