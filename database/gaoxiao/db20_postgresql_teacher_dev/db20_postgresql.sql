SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS ethics_training_record;
DROP TABLE IF EXISTS teaching_development_activity;
DROP TABLE IF EXISTS credit_hour_record;
DROP TABLE IF EXISTS training_participation;
DROP TABLE IF EXISTS training_project;

CREATE TABLE training_project (
  training_project_id VARCHAR(32) PRIMARY KEY,
  project_name VARCHAR(200) NOT NULL,
  organizer VARCHAR(80) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  total_hour NUMERIC(5,1) NOT NULL,
  project_status VARCHAR(20) NOT NULL
);

CREATE TABLE training_participation (
  participation_id VARCHAR(32) PRIMARY KEY,
  teacher_id VARCHAR(20) NOT NULL,
  teacher_name VARCHAR(50) NOT NULL,
  training_project_id VARCHAR(32) NOT NULL REFERENCES training_project(training_project_id),
  project_name VARCHAR(200) NOT NULL,
  credit_hour NUMERIC(5,1) NOT NULL,
  result_status VARCHAR(20) NOT NULL,
  finish_time DATE NOT NULL
);

CREATE TABLE credit_hour_record (
  record_id VARCHAR(32) PRIMARY KEY,
  participation_id VARCHAR(32) NOT NULL REFERENCES training_participation(participation_id) ON DELETE CASCADE,
  teacher_id VARCHAR(20) NOT NULL,
  credit_hour NUMERIC(5,1) NOT NULL,
  recognized_flag BOOLEAN NOT NULL,
  record_time DATE NOT NULL
);

CREATE TABLE teaching_development_activity (
  activity_id VARCHAR(32) PRIMARY KEY,
  teacher_id VARCHAR(20) NOT NULL,
  activity_name VARCHAR(200) NOT NULL,
  activity_date DATE NOT NULL,
  contribution_score NUMERIC(5,2) NOT NULL,
  activity_status VARCHAR(20) NOT NULL
);

CREATE TABLE ethics_training_record (
  ethics_id VARCHAR(32) PRIMARY KEY,
  teacher_id VARCHAR(20) NOT NULL,
  course_name VARCHAR(200) NOT NULL,
  completion_date DATE NOT NULL,
  exam_score NUMERIC(5,2) NOT NULL,
  status VARCHAR(20) NOT NULL
);
