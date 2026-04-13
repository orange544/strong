CREATE DATABASE IF NOT EXISTS gaoxiao_db10_aid
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE gaoxiao_db10_aid;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS aid_distribution;
DROP TABLE IF EXISTS student_loan;
DROP TABLE IF EXISTS workstudy_post;
DROP TABLE IF EXISTS grant_review;
DROP TABLE IF EXISTS scholarship_apply;

CREATE TABLE scholarship_apply (
  apply_id VARCHAR(32) NOT NULL,
  sid VARCHAR(32) NOT NULL,
  person_name VARCHAR(50) NOT NULL,
  sex VARCHAR(10) NOT NULL,
  age TINYINT UNSIGNED NOT NULL,
  college_code VARCHAR(20) NOT NULL,
  scholarship_type VARCHAR(40) NOT NULL,
  apply_term VARCHAR(20) NOT NULL,
  score_rank INT NOT NULL,
  award_status VARCHAR(20) NOT NULL,
  grant_amount DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (apply_id),
  KEY idx_apply_sid (sid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE grant_review (
  review_id VARCHAR(32) NOT NULL,
  apply_id VARCHAR(32) NOT NULL,
  reviewer VARCHAR(50) NOT NULL,
  review_result VARCHAR(20) NOT NULL,
  review_time DATETIME NOT NULL,
  review_comment VARCHAR(300) NOT NULL,
  PRIMARY KEY (review_id),
  KEY idx_review_apply (apply_id),
  CONSTRAINT fk_grant_review_apply
    FOREIGN KEY (apply_id) REFERENCES scholarship_apply(apply_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE workstudy_post (
  post_id VARCHAR(32) NOT NULL,
  sid VARCHAR(32) NOT NULL,
  post_name VARCHAR(60) NOT NULL,
  dept_name VARCHAR(60) NOT NULL,
  hourly_pay DECIMAL(8,2) NOT NULL,
  post_status VARCHAR(20) NOT NULL,
  start_date DATE NOT NULL,
  PRIMARY KEY (post_id),
  KEY idx_workstudy_sid (sid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE student_loan (
  loan_id VARCHAR(32) NOT NULL,
  sid VARCHAR(32) NOT NULL,
  loan_type VARCHAR(40) NOT NULL,
  loan_amount DECIMAL(12,2) NOT NULL,
  bank_name VARCHAR(80) NOT NULL,
  loan_status VARCHAR(20) NOT NULL,
  disburse_date DATE,
  PRIMARY KEY (loan_id),
  KEY idx_loan_sid (sid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE aid_distribution (
  distribution_id VARCHAR(32) NOT NULL,
  apply_id VARCHAR(32),
  sid VARCHAR(32) NOT NULL,
  aid_type VARCHAR(40) NOT NULL,
  amount DECIMAL(12,2) NOT NULL,
  grant_status VARCHAR(20) NOT NULL,
  pay_time DATETIME,
  voucher_no VARCHAR(40),
  PRIMARY KEY (distribution_id),
  KEY idx_dist_sid (sid),
  KEY idx_dist_apply (apply_id),
  CONSTRAINT fk_distribution_apply
    FOREIGN KEY (apply_id) REFERENCES scholarship_apply(apply_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
