SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS schedule_adjustment;
DROP TABLE IF EXISTS teaching_schedule;
DROP TABLE IF EXISTS term_calendar;
DROP TABLE IF EXISTS teacher_assignment;
DROP TABLE IF EXISTS course_section;
DROP TABLE IF EXISTS classroom;
DROP TABLE IF EXISTS course;

CREATE TABLE course (
  course_code VARCHAR(20) PRIMARY KEY,
  course_name VARCHAR(100) NOT NULL,
  credit NUMERIC(3,1) NOT NULL,
  course_type VARCHAR(20) NOT NULL,
  dept_code VARCHAR(20) NOT NULL,
  status VARCHAR(20) NOT NULL
);

CREATE TABLE classroom (
  room_id VARCHAR(30) PRIMARY KEY,
  building_name VARCHAR(60) NOT NULL,
  room_capacity INTEGER NOT NULL,
  room_type VARCHAR(30) NOT NULL,
  room_status VARCHAR(20) NOT NULL
);

CREATE TABLE course_section (
  section_id VARCHAR(32) PRIMARY KEY,
  course_code VARCHAR(20) NOT NULL REFERENCES course(course_code),
  course_name VARCHAR(100) NOT NULL,
  teacher_no VARCHAR(20) NOT NULL,
  teacher_name VARCHAR(50) NOT NULL,
  term_id VARCHAR(20) NOT NULL,
  class_capacity INTEGER NOT NULL,
  room_id VARCHAR(30) NOT NULL REFERENCES classroom(room_id),
  class_status VARCHAR(20) NOT NULL
);

CREATE TABLE teacher_assignment (
  assignment_id VARCHAR(32) PRIMARY KEY,
  section_id VARCHAR(32) NOT NULL REFERENCES course_section(section_id) ON DELETE CASCADE,
  teacher_no VARCHAR(20) NOT NULL,
  teacher_name VARCHAR(50) NOT NULL,
  role_type VARCHAR(20) NOT NULL,
  assigned_at TIMESTAMP NOT NULL
);

CREATE TABLE term_calendar (
  calendar_id VARCHAR(32) PRIMARY KEY,
  term_id VARCHAR(20) NOT NULL,
  week_no INTEGER NOT NULL,
  week_start DATE NOT NULL,
  week_end DATE NOT NULL,
  teaching_flag BOOLEAN NOT NULL
);

CREATE TABLE teaching_schedule (
  schedule_id VARCHAR(32) PRIMARY KEY,
  section_id VARCHAR(32) NOT NULL REFERENCES course_section(section_id) ON DELETE CASCADE,
  week_day SMALLINT NOT NULL CHECK (week_day BETWEEN 1 AND 7),
  start_period SMALLINT NOT NULL,
  end_period SMALLINT NOT NULL,
  room_id VARCHAR(30) NOT NULL REFERENCES classroom(room_id),
  class_mode VARCHAR(20) NOT NULL,
  schedule_status VARCHAR(20) NOT NULL
);

CREATE TABLE schedule_adjustment (
  adjustment_id VARCHAR(32) PRIMARY KEY,
  section_id VARCHAR(32) NOT NULL REFERENCES course_section(section_id) ON DELETE CASCADE,
  original_schedule_id VARCHAR(32) REFERENCES teaching_schedule(schedule_id),
  adjusted_schedule_desc VARCHAR(200) NOT NULL,
  reason VARCHAR(200) NOT NULL,
  adjusted_by VARCHAR(50) NOT NULL,
  adjusted_time TIMESTAMP NOT NULL,
  adjustment_status VARCHAR(20) NOT NULL
);
