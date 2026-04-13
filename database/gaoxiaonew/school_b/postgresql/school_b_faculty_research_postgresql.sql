SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS faculty_rank_record;
DROP TABLE IF EXISTS faculty_output_metric;
DROP TABLE IF EXISTS faculty_member;

CREATE TABLE faculty_member (
  faculty_no VARCHAR(16) PRIMARY KEY,
  employee_code VARCHAR(32) NOT NULL UNIQUE,
  faculty_name VARCHAR(64) NOT NULL,
  "rank" VARCHAR(32) NOT NULL,
  job_type VARCHAR(32) NOT NULL,
  graduated_school VARCHAR(128) NOT NULL,
  research_direction VARCHAR(256) NOT NULL,
  job_status VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL
);

CREATE TABLE faculty_output_metric (
  faculty_no VARCHAR(16) PRIMARY KEY,
  publication_total INT NOT NULL,
  patent_total INT NOT NULL,
  fund_income_3y NUMERIC(14,2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  CONSTRAINT fk_faculty_output_metric_member
    FOREIGN KEY (faculty_no) REFERENCES faculty_member(faculty_no)
);

CREATE TABLE faculty_rank_record (
  record_id VARCHAR(24) PRIMARY KEY,
  faculty_no VARCHAR(16) NOT NULL,
  rank_name VARCHAR(32) NOT NULL,
  review_result VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  CONSTRAINT fk_faculty_rank_record_member
    FOREIGN KEY (faculty_no) REFERENCES faculty_member(faculty_no)
);