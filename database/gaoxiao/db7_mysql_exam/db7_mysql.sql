CREATE DATABASE IF NOT EXISTS gaoxiao_db7_exam
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE gaoxiao_db7_exam;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS exam_score_result;
DROP TABLE IF EXISTS exam_violation;
DROP TABLE IF EXISTS exam_attendance;
DROP TABLE IF EXISTS admission_ticket;
DROP TABLE IF EXISTS exam_signup;
DROP TABLE IF EXISTS invigilator_assignment;
DROP TABLE IF EXISTS exam_room_arrangement;
DROP TABLE IF EXISTS exam_plan;

CREATE TABLE exam_plan (
  exam_id VARCHAR(32) NOT NULL,
  course_code VARCHAR(20) NOT NULL,
  exam_name VARCHAR(120) NOT NULL,
  term_id VARCHAR(20) NOT NULL,
  exam_type VARCHAR(20) NOT NULL,
  exam_time DATETIME NOT NULL,
  candidate_scope VARCHAR(40) NOT NULL,
  status VARCHAR(20) NOT NULL,
  PRIMARY KEY (exam_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE exam_room_arrangement (
  arrangement_id VARCHAR(32) NOT NULL,
  exam_id VARCHAR(32) NOT NULL,
  room_id VARCHAR(30) NOT NULL,
  seat_count INT NOT NULL,
  arrange_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (arrangement_id),
  KEY idx_room_exam (exam_id),
  CONSTRAINT fk_room_exam_plan
    FOREIGN KEY (exam_id) REFERENCES exam_plan(exam_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE invigilator_assignment (
  assignment_id VARCHAR(32) NOT NULL,
  exam_id VARCHAR(32) NOT NULL,
  teacher_no VARCHAR(20) NOT NULL,
  teacher_name VARCHAR(50) NOT NULL,
  duty_role VARCHAR(20) NOT NULL,
  assign_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (assignment_id),
  KEY idx_invigilator_exam (exam_id),
  CONSTRAINT fk_invigilator_exam_plan
    FOREIGN KEY (exam_id) REFERENCES exam_plan(exam_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE exam_signup (
  signup_id VARCHAR(32) NOT NULL,
  exam_id VARCHAR(32) NOT NULL,
  candidate_no VARCHAR(32) NOT NULL,
  candidate_name VARCHAR(50) NOT NULL,
  class_id VARCHAR(32) NOT NULL,
  signup_status VARCHAR(20) NOT NULL,
  signup_time DATETIME NOT NULL,
  PRIMARY KEY (signup_id),
  KEY idx_signup_exam (exam_id),
  CONSTRAINT fk_signup_exam_plan
    FOREIGN KEY (exam_id) REFERENCES exam_plan(exam_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE admission_ticket (
  ticket_id VARCHAR(32) NOT NULL,
  signup_id VARCHAR(32) NOT NULL,
  ticket_no VARCHAR(40) NOT NULL,
  print_time DATETIME NOT NULL,
  ticket_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (ticket_id),
  UNIQUE KEY uk_ticket_signup (signup_id),
  CONSTRAINT fk_ticket_signup
    FOREIGN KEY (signup_id) REFERENCES exam_signup(signup_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE exam_attendance (
  attendance_id VARCHAR(32) NOT NULL,
  signup_id VARCHAR(32) NOT NULL,
  checkin_time DATETIME NOT NULL,
  attendance_status VARCHAR(20) NOT NULL,
  room_id VARCHAR(30) NOT NULL,
  PRIMARY KEY (attendance_id),
  UNIQUE KEY uk_attendance_signup (signup_id),
  CONSTRAINT fk_attendance_signup
    FOREIGN KEY (signup_id) REFERENCES exam_signup(signup_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE exam_violation (
  violation_id VARCHAR(32) NOT NULL,
  signup_id VARCHAR(32) NOT NULL,
  violation_type VARCHAR(40) NOT NULL,
  violation_detail VARCHAR(200) NOT NULL,
  record_time DATETIME NOT NULL,
  process_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (violation_id),
  KEY idx_violation_signup (signup_id),
  CONSTRAINT fk_violation_signup
    FOREIGN KEY (signup_id) REFERENCES exam_signup(signup_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE exam_score_result (
  result_id VARCHAR(32) NOT NULL,
  signup_id VARCHAR(32) NOT NULL,
  raw_score DECIMAL(5,2) NOT NULL,
  final_score DECIMAL(5,2) NOT NULL,
  result_status VARCHAR(20) NOT NULL,
  publish_time DATETIME NOT NULL,
  PRIMARY KEY (result_id),
  UNIQUE KEY uk_result_signup (signup_id),
  CONSTRAINT fk_result_signup
    FOREIGN KEY (signup_id) REFERENCES exam_signup(signup_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
