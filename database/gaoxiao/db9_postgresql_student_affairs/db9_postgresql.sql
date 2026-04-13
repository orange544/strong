SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS counselor_binding;
DROP TABLE IF EXISTS difficulty_identification;
DROP TABLE IF EXISTS psychological_warning;
DROP TABLE IF EXISTS leave_application;
DROP TABLE IF EXISTS disciplinary_record;
DROP TABLE IF EXISTS student_affair;
DROP TABLE IF EXISTS student_profile;

CREATE TABLE student_profile (
  profile_id VARCHAR(32) PRIMARY KEY,
  stu_id VARCHAR(32) NOT NULL UNIQUE,
  stu_name VARCHAR(50) NOT NULL,
  xb VARCHAR(10) NOT NULL,
  student_age SMALLINT NOT NULL,
  contact_phone VARCHAR(20) NOT NULL,
  department_id VARCHAR(20) NOT NULL,
  major_code VARCHAR(20) NOT NULL,
  economic_level VARCHAR(20) NOT NULL,
  oncampus_status VARCHAR(20) NOT NULL
);

CREATE TABLE student_affair (
  affair_id VARCHAR(32) PRIMARY KEY,
  profile_id VARCHAR(32) NOT NULL REFERENCES student_profile(profile_id) ON DELETE CASCADE,
  affair_type VARCHAR(30) NOT NULL,
  affair_desc VARCHAR(200) NOT NULL,
  affair_time TIMESTAMP NOT NULL,
  affair_status VARCHAR(20) NOT NULL
);

CREATE TABLE disciplinary_record (
  discipline_id VARCHAR(32) PRIMARY KEY,
  profile_id VARCHAR(32) NOT NULL REFERENCES student_profile(profile_id) ON DELETE CASCADE,
  discipline_level VARCHAR(30) NOT NULL,
  reason VARCHAR(200) NOT NULL,
  decision_date DATE NOT NULL,
  expire_date DATE,
  record_status VARCHAR(20) NOT NULL
);

CREATE TABLE leave_application (
  leave_id VARCHAR(32) PRIMARY KEY,
  profile_id VARCHAR(32) NOT NULL REFERENCES student_profile(profile_id) ON DELETE CASCADE,
  leave_type VARCHAR(20) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  leave_reason VARCHAR(200) NOT NULL,
  leave_status VARCHAR(20) NOT NULL
);

CREATE TABLE psychological_warning (
  warning_id VARCHAR(32) PRIMARY KEY,
  profile_id VARCHAR(32) NOT NULL REFERENCES student_profile(profile_id) ON DELETE CASCADE,
  warning_level VARCHAR(20) NOT NULL,
  warning_tag VARCHAR(50) NOT NULL,
  assess_date DATE NOT NULL,
  warning_status VARCHAR(20) NOT NULL
);

CREATE TABLE difficulty_identification (
  difficulty_id VARCHAR(32) PRIMARY KEY,
  profile_id VARCHAR(32) NOT NULL REFERENCES student_profile(profile_id) ON DELETE CASCADE,
  economic_level VARCHAR(20) NOT NULL,
  identified_by VARCHAR(50) NOT NULL,
  identified_date DATE NOT NULL,
  ident_status VARCHAR(20) NOT NULL
);

CREATE TABLE counselor_binding (
  binding_id VARCHAR(32) PRIMARY KEY,
  profile_id VARCHAR(32) NOT NULL REFERENCES student_profile(profile_id) ON DELETE CASCADE,
  counselor_id VARCHAR(20) NOT NULL,
  counselor_name VARCHAR(50) NOT NULL,
  bind_date DATE NOT NULL,
  binding_status VARCHAR(20) NOT NULL
);
