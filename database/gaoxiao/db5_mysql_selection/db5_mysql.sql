CREATE DATABASE IF NOT EXISTS gaoxiao_db5_selection_score
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE gaoxiao_db5_selection_score;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS makeup_exam_record;
DROP TABLE IF EXISTS retake_record;
DROP TABLE IF EXISTS gpa_stat;
DROP TABLE IF EXISTS score_component;
DROP TABLE IF EXISTS course_score;
DROP TABLE IF EXISTS selection_change_log;
DROP TABLE IF EXISTS course_selection;

CREATE TABLE course_selection (
  select_id VARCHAR(32) NOT NULL,
  sid VARCHAR(32) NOT NULL,
  full_name VARCHAR(50) NOT NULL,
  sex_type VARCHAR(10) NOT NULL,
  age_year TINYINT UNSIGNED NOT NULL,
  class_id VARCHAR(32) NOT NULL,
  select_type VARCHAR(20) NOT NULL,
  selection_status VARCHAR(20) NOT NULL,
  select_time DATETIME NOT NULL,
  PRIMARY KEY (select_id),
  KEY idx_selection_sid (sid),
  KEY idx_selection_class (class_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE selection_change_log (
  change_id VARCHAR(32) NOT NULL,
  select_id VARCHAR(32) NOT NULL,
  old_status VARCHAR(20) NOT NULL,
  new_status VARCHAR(20) NOT NULL,
  change_reason VARCHAR(120) NOT NULL,
  change_time DATETIME NOT NULL,
  PRIMARY KEY (change_id),
  KEY idx_change_select (select_id),
  CONSTRAINT fk_change_selection
    FOREIGN KEY (select_id) REFERENCES course_selection(select_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE course_score (
  score_id VARCHAR(32) NOT NULL,
  select_id VARCHAR(32) NOT NULL,
  sid VARCHAR(32) NOT NULL,
  class_id VARCHAR(32) NOT NULL,
  usual_score DECIMAL(5,2) NOT NULL,
  exam_score DECIMAL(5,2) NOT NULL,
  final_score DECIMAL(5,2) NOT NULL,
  score_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (score_id),
  KEY idx_score_select (select_id),
  CONSTRAINT fk_score_selection
    FOREIGN KEY (select_id) REFERENCES course_selection(select_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE score_component (
  component_id VARCHAR(32) NOT NULL,
  score_id VARCHAR(32) NOT NULL,
  component_name VARCHAR(40) NOT NULL,
  component_weight DECIMAL(5,2) NOT NULL,
  component_score DECIMAL(5,2) NOT NULL,
  PRIMARY KEY (component_id),
  KEY idx_component_score (score_id),
  CONSTRAINT fk_component_score
    FOREIGN KEY (score_id) REFERENCES course_score(score_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE gpa_stat (
  stat_id VARCHAR(32) NOT NULL,
  sid VARCHAR(32) NOT NULL,
  term_id VARCHAR(20) NOT NULL,
  total_credit DECIMAL(6,2) NOT NULL,
  gpa DECIMAL(4,2) NOT NULL,
  rank_in_major INT NOT NULL,
  PRIMARY KEY (stat_id),
  KEY idx_gpa_sid (sid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE retake_record (
  retake_id VARCHAR(32) NOT NULL,
  sid VARCHAR(32) NOT NULL,
  class_id VARCHAR(32) NOT NULL,
  reason VARCHAR(120) NOT NULL,
  apply_time DATETIME NOT NULL,
  retake_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (retake_id),
  KEY idx_retake_sid (sid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE makeup_exam_record (
  makeup_id VARCHAR(32) NOT NULL,
  sid VARCHAR(32) NOT NULL,
  class_id VARCHAR(32) NOT NULL,
  signup_time DATETIME NOT NULL,
  exam_time DATETIME NOT NULL,
  exam_room VARCHAR(30) NOT NULL,
  makeup_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (makeup_id),
  KEY idx_makeup_sid (sid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
