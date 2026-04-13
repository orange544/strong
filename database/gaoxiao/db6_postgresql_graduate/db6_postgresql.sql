SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS degree_award;
DROP TABLE IF EXISTS thesis_defense;
DROP TABLE IF EXISTS blind_review_task;
DROP TABLE IF EXISTS thesis_basic;
DROP TABLE IF EXISTS midterm_assessment;
DROP TABLE IF EXISTS proposal_review;
DROP TABLE IF EXISTS cultivation_plan;
DROP TABLE IF EXISTS supervisor_relation;
DROP TABLE IF EXISTS graduate_student;

CREATE TABLE graduate_student (
  graduate_student_id VARCHAR(32) PRIMARY KEY,
  stu_no VARCHAR(20) NOT NULL UNIQUE,
  name VARCHAR(50) NOT NULL,
  sex VARCHAR(10) NOT NULL,
  student_age SMALLINT NOT NULL,
  cert_no VARCHAR(18) NOT NULL UNIQUE,
  phone_no VARCHAR(20) NOT NULL,
  college_code VARCHAR(20) NOT NULL,
  discipline_code VARCHAR(20) NOT NULL,
  reg_status VARCHAR(20) NOT NULL
);

CREATE TABLE supervisor_relation (
  relation_id VARCHAR(32) PRIMARY KEY,
  graduate_student_id VARCHAR(32) NOT NULL REFERENCES graduate_student(graduate_student_id) ON DELETE CASCADE,
  supervisor_id VARCHAR(20) NOT NULL,
  supervisor_name VARCHAR(50) NOT NULL,
  relation_type VARCHAR(20) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE,
  relation_status VARCHAR(20) NOT NULL
);

CREATE TABLE cultivation_plan (
  plan_id VARCHAR(32) PRIMARY KEY,
  graduate_student_id VARCHAR(32) NOT NULL REFERENCES graduate_student(graduate_student_id) ON DELETE CASCADE,
  term_id VARCHAR(20) NOT NULL,
  required_credit NUMERIC(5,1) NOT NULL,
  completed_credit NUMERIC(5,1) NOT NULL,
  plan_status VARCHAR(20) NOT NULL
);

CREATE TABLE proposal_review (
  proposal_id VARCHAR(32) PRIMARY KEY,
  graduate_student_id VARCHAR(32) NOT NULL REFERENCES graduate_student(graduate_student_id) ON DELETE CASCADE,
  topic_title VARCHAR(200) NOT NULL,
  reviewer VARCHAR(50) NOT NULL,
  review_result VARCHAR(20) NOT NULL,
  review_time TIMESTAMP NOT NULL,
  review_comment VARCHAR(300) NOT NULL
);

CREATE TABLE midterm_assessment (
  midterm_id VARCHAR(32) PRIMARY KEY,
  graduate_student_id VARCHAR(32) NOT NULL REFERENCES graduate_student(graduate_student_id) ON DELETE CASCADE,
  assessment_score NUMERIC(5,2) NOT NULL,
  assessment_result VARCHAR(20) NOT NULL,
  assessor VARCHAR(50) NOT NULL,
  assess_time TIMESTAMP NOT NULL,
  assess_comment VARCHAR(300) NOT NULL
);

CREATE TABLE thesis_basic (
  thesis_id VARCHAR(32) PRIMARY KEY,
  graduate_student_id VARCHAR(32) NOT NULL REFERENCES graduate_student(graduate_student_id) ON DELETE CASCADE,
  thesis_title VARCHAR(300) NOT NULL,
  research_field VARCHAR(100) NOT NULL,
  submit_time TIMESTAMP NOT NULL,
  thesis_status VARCHAR(20) NOT NULL
);

CREATE TABLE blind_review_task (
  task_id VARCHAR(32) PRIMARY KEY,
  thesis_id VARCHAR(32) NOT NULL REFERENCES thesis_basic(thesis_id) ON DELETE CASCADE,
  reviewer_code VARCHAR(20) NOT NULL,
  dispatch_time TIMESTAMP NOT NULL,
  review_status VARCHAR(20) NOT NULL,
  score NUMERIC(5,2) NOT NULL
);

CREATE TABLE thesis_defense (
  defense_id VARCHAR(32) PRIMARY KEY,
  thesis_id VARCHAR(32) NOT NULL REFERENCES thesis_basic(thesis_id) ON DELETE CASCADE,
  defense_time TIMESTAMP NOT NULL,
  defense_room VARCHAR(30) NOT NULL,
  defense_result VARCHAR(20) NOT NULL,
  committee_chair VARCHAR(50) NOT NULL
);

CREATE TABLE degree_award (
  award_id VARCHAR(32) PRIMARY KEY,
  graduate_student_id VARCHAR(32) NOT NULL REFERENCES graduate_student(graduate_student_id) ON DELETE CASCADE,
  degree_name VARCHAR(50) NOT NULL,
  award_date DATE NOT NULL,
  award_status VARCHAR(20) NOT NULL,
  degree_cert_no VARCHAR(40) NOT NULL UNIQUE
);
