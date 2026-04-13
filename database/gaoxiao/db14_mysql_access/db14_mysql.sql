CREATE DATABASE IF NOT EXISTS gaoxiao_db14_access
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE gaoxiao_db14_access;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS visitor_pass;
DROP TABLE IF EXISTS abnormal_access_event;
DROP TABLE IF EXISTS building_entry_log;
DROP TABLE IF EXISTS access_record;
DROP TABLE IF EXISTS access_person_map;
DROP TABLE IF EXISTS access_device;

CREATE TABLE access_device (
  door_id VARCHAR(32) NOT NULL,
  building_code VARCHAR(30) NOT NULL,
  door_name VARCHAR(80) NOT NULL,
  online_status VARCHAR(20) NOT NULL,
  alarm_flag TINYINT NOT NULL,
  PRIMARY KEY (door_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE access_person_map (
  map_id VARCHAR(32) NOT NULL,
  person_id VARCHAR(32) NOT NULL,
  person_name VARCHAR(50) NOT NULL,
  person_type VARCHAR(20) NOT NULL,
  card_no VARCHAR(40) NOT NULL,
  map_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (map_id),
  KEY idx_person_map_person (person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE access_record (
  record_id VARCHAR(32) NOT NULL,
  person_id VARCHAR(32) NOT NULL,
  person_name VARCHAR(50) NOT NULL,
  person_type VARCHAR(20) NOT NULL,
  door_id VARCHAR(32) NOT NULL,
  building_code VARCHAR(30) NOT NULL,
  access_time DATETIME NOT NULL,
  access_result VARCHAR(20) NOT NULL,
  direction VARCHAR(10) NOT NULL,
  PRIMARY KEY (record_id),
  KEY idx_record_person (person_id),
  KEY idx_record_door (door_id),
  CONSTRAINT fk_access_record_device
    FOREIGN KEY (door_id) REFERENCES access_device(door_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE building_entry_log (
  entry_id VARCHAR(32) NOT NULL,
  record_id VARCHAR(32) NOT NULL,
  building_code VARCHAR(30) NOT NULL,
  entry_result VARCHAR(20) NOT NULL,
  logged_time DATETIME NOT NULL,
  PRIMARY KEY (entry_id),
  KEY idx_entry_record (record_id),
  CONSTRAINT fk_entry_record
    FOREIGN KEY (record_id) REFERENCES access_record(record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE abnormal_access_event (
  event_id VARCHAR(32) NOT NULL,
  record_id VARCHAR(32) NOT NULL,
  abnormal_type VARCHAR(40) NOT NULL,
  event_detail VARCHAR(200) NOT NULL,
  event_status VARCHAR(20) NOT NULL,
  event_time DATETIME NOT NULL,
  PRIMARY KEY (event_id),
  KEY idx_abnormal_record (record_id),
  CONSTRAINT fk_abnormal_record
    FOREIGN KEY (record_id) REFERENCES access_record(record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE visitor_pass (
  pass_id VARCHAR(32) NOT NULL,
  visitor_name VARCHAR(50) NOT NULL,
  visitor_id_card VARCHAR(18) NOT NULL,
  host_person_id VARCHAR(32) NOT NULL,
  door_id VARCHAR(32) NOT NULL,
  valid_from DATETIME NOT NULL,
  valid_to DATETIME NOT NULL,
  pass_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (pass_id),
  KEY idx_visitor_door (door_id),
  CONSTRAINT fk_visitor_device
    FOREIGN KEY (door_id) REFERENCES access_device(door_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
