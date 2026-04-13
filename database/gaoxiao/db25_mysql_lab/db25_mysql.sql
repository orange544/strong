CREATE DATABASE IF NOT EXISTS gaoxiao_db25_lab
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE gaoxiao_db25_lab;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS safety_inspection;
DROP TABLE IF EXISTS calibration_record;
DROP TABLE IF EXISTS maintenance_record;
DROP TABLE IF EXISTS equipment_reservation;
DROP TABLE IF EXISTS equipment_info;
DROP TABLE IF EXISTS laboratory_room;

CREATE TABLE laboratory_room (
  lab_room_id VARCHAR(32) NOT NULL,
  building_code VARCHAR(20) NOT NULL,
  room_name VARCHAR(80) NOT NULL,
  room_type VARCHAR(30) NOT NULL,
  capacity INT NOT NULL,
  room_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (lab_room_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE equipment_info (
  equipment_id VARCHAR(32) NOT NULL,
  equipment_code VARCHAR(32) NOT NULL,
  equipment_name VARCHAR(120) NOT NULL,
  lab_room_id VARCHAR(32) NOT NULL,
  manager_id VARCHAR(20) NOT NULL,
  manager_name VARCHAR(50) NOT NULL,
  equipment_status VARCHAR(20) NOT NULL,
  purchase_date DATE NOT NULL,
  asset_ref_code VARCHAR(32) NOT NULL,
  PRIMARY KEY (equipment_id),
  UNIQUE KEY uk_equipment_code (equipment_code),
  KEY idx_equipment_room (lab_room_id),
  CONSTRAINT fk_equipment_room
    FOREIGN KEY (lab_room_id) REFERENCES laboratory_room(lab_room_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE equipment_reservation (
  reservation_id VARCHAR(32) NOT NULL,
  equipment_id VARCHAR(32) NOT NULL,
  user_id VARCHAR(32) NOT NULL,
  user_name VARCHAR(50) NOT NULL,
  reserve_start DATETIME NOT NULL,
  reserve_end DATETIME NOT NULL,
  reservation_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (reservation_id),
  KEY idx_reservation_equipment (equipment_id),
  CONSTRAINT fk_reservation_equipment
    FOREIGN KEY (equipment_id) REFERENCES equipment_info(equipment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE maintenance_record (
  maintenance_id VARCHAR(32) NOT NULL,
  equipment_id VARCHAR(32) NOT NULL,
  issue_desc VARCHAR(200) NOT NULL,
  maintainer VARCHAR(50) NOT NULL,
  maintenance_time DATETIME NOT NULL,
  maintenance_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (maintenance_id),
  KEY idx_maintenance_equipment (equipment_id),
  CONSTRAINT fk_maintenance_equipment
    FOREIGN KEY (equipment_id) REFERENCES equipment_info(equipment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE calibration_record (
  calibration_id VARCHAR(32) NOT NULL,
  equipment_id VARCHAR(32) NOT NULL,
  calibration_date DATE NOT NULL,
  calibration_result VARCHAR(20) NOT NULL,
  calibrator VARCHAR(50) NOT NULL,
  next_calibration_date DATE,
  PRIMARY KEY (calibration_id),
  KEY idx_calibration_equipment (equipment_id),
  CONSTRAINT fk_calibration_equipment
    FOREIGN KEY (equipment_id) REFERENCES equipment_info(equipment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE safety_inspection (
  inspection_id VARCHAR(32) NOT NULL,
  lab_room_id VARCHAR(32) NOT NULL,
  inspector VARCHAR(50) NOT NULL,
  inspection_time DATETIME NOT NULL,
  safety_score DECIMAL(5,2) NOT NULL,
  inspection_status VARCHAR(20) NOT NULL,
  PRIMARY KEY (inspection_id),
  KEY idx_inspection_room (lab_room_id),
  CONSTRAINT fk_inspection_room
    FOREIGN KEY (lab_room_id) REFERENCES laboratory_room(lab_room_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
