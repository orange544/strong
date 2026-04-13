CREATE DATABASE IF NOT EXISTS gaoxiao_db18_staff
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE gaoxiao_db18_staff;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS staff_change_record;
DROP TABLE IF EXISTS staff_assignment;
DROP TABLE IF EXISTS labor_contract;
DROP TABLE IF EXISTS position_category;
DROP TABLE IF EXISTS staff_basic;

CREATE TABLE staff_basic (
  staff_id VARCHAR(32) NOT NULL,
  employee_no VARCHAR(32) NOT NULL,
  name VARCHAR(50) NOT NULL,
  sex VARCHAR(10) NOT NULL,
  age_year TINYINT UNSIGNED NOT NULL,
  department_id VARCHAR(30) NOT NULL,
  post_name VARCHAR(60) NOT NULL,
  contract_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (staff_id),
  UNIQUE KEY uk_staff_employee_no (employee_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE position_category (
  category_id VARCHAR(32) NOT NULL,
  category_name VARCHAR(50) NOT NULL,
  level_name VARCHAR(30) NOT NULL,
  dept_scope VARCHAR(80) NOT NULL,
  category_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE labor_contract (
  contract_id VARCHAR(32) NOT NULL,
  staff_id VARCHAR(32) NOT NULL,
  contract_type VARCHAR(30) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  contract_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (contract_id),
  KEY idx_contract_staff (staff_id),
  CONSTRAINT fk_contract_staff
    FOREIGN KEY (staff_id) REFERENCES staff_basic(staff_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE staff_assignment (
  assignment_id VARCHAR(32) NOT NULL,
  staff_id VARCHAR(32) NOT NULL,
  department_id VARCHAR(30) NOT NULL,
  post_name VARCHAR(60) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE,
  assignment_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (assignment_id),
  KEY idx_assignment_staff (staff_id),
  CONSTRAINT fk_assignment_staff
    FOREIGN KEY (staff_id) REFERENCES staff_basic(staff_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE staff_change_record (
  change_id VARCHAR(32) NOT NULL,
  staff_id VARCHAR(32) NOT NULL,
  change_type VARCHAR(30) NOT NULL,
  old_post VARCHAR(60) NOT NULL,
  new_post VARCHAR(60) NOT NULL,
  change_date DATE NOT NULL,
  change_reason VARCHAR(200) NOT NULL,
  PRIMARY KEY (change_id),
  KEY idx_change_staff (staff_id),
  CONSTRAINT fk_change_staff
    FOREIGN KEY (staff_id) REFERENCES staff_basic(staff_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
