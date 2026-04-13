DROP DATABASE IF EXISTS wuliu_t2_mysql_db;
CREATE DATABASE wuliu_t2_mysql_db CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE wuliu_t2_mysql_db;

CREATE TABLE truck_master (
  truck_id VARCHAR(32) NOT NULL,
  plate_no VARCHAR(32) NOT NULL,
  truck_type VARCHAR(32) NOT NULL,
  cap_kg DECIMAL(10, 2) NOT NULL,
  cap_m3 DECIMAL(10, 3) NOT NULL,
  status_cd VARCHAR(32) NOT NULL,
  PRIMARY KEY (truck_id),
  UNIQUE KEY ux_truck_master_plate_no (plate_no),
  KEY idx_truck_master_status_cd (status_cd)
) ENGINE = InnoDB;

CREATE TABLE driver_master (
  driver_code VARCHAR(32) NOT NULL,
  driver_name VARCHAR(64) NOT NULL,
  mobile_no VARCHAR(32) NOT NULL,
  license_no VARCHAR(64) NOT NULL,
  license_class VARCHAR(16) NOT NULL,
  job_status VARCHAR(32) NOT NULL,
  entry_date DATE NOT NULL,
  status_cd VARCHAR(32) NOT NULL,
  PRIMARY KEY (driver_code),
  KEY idx_driver_master_driver_name (driver_name),
  KEY idx_driver_master_mobile_no (mobile_no),
  KEY idx_driver_master_status_cd (status_cd)
) ENGINE = InnoDB;

CREATE TABLE trip_plan (
  trip_no VARCHAR(64) NOT NULL,
  route_code VARCHAR(64) NOT NULL,
  truck_id VARCHAR(32) NOT NULL,
  driver_id VARCHAR(32) NOT NULL,
  plan_depart_tm DATETIME NOT NULL,
  real_depart_tm DATETIME NULL,
  plan_arrive_tm DATETIME NOT NULL,
  real_arrive_tm DATETIME NULL,
  execute_status VARCHAR(32) NOT NULL,
  PRIMARY KEY (trip_no),
  KEY idx_trip_plan_route_code (route_code),
  KEY idx_trip_plan_truck_id (truck_id),
  KEY idx_trip_plan_driver_id (driver_id),
  KEY idx_trip_plan_execute_status (execute_status),
  CONSTRAINT fk_trip_plan_truck_id FOREIGN KEY (truck_id) REFERENCES truck_master(truck_id),
  CONSTRAINT fk_trip_plan_driver_id FOREIGN KEY (driver_id) REFERENCES driver_master(driver_code)
) ENGINE = InnoDB;

CREATE TABLE shipment_task (
  shipment_task_no VARCHAR(64) NOT NULL,
  shipment_no VARCHAR(64) NOT NULL,
  trip_no VARCHAR(64) NOT NULL,
  truck_id VARCHAR(32) NOT NULL,
  driver_id VARCHAR(32) NOT NULL,
  pickup_site_id VARCHAR(32) NOT NULL,
  deliver_site_id VARCHAR(32) NOT NULL,
  execute_status VARCHAR(32) NOT NULL,
  PRIMARY KEY (shipment_task_no),
  KEY idx_shipment_task_shipment_no (shipment_no),
  KEY idx_shipment_task_trip_no (trip_no),
  KEY idx_shipment_task_execute_status (execute_status),
  KEY idx_shipment_task_pickup_deliver (pickup_site_id, deliver_site_id),
  CONSTRAINT fk_shipment_task_trip_no FOREIGN KEY (trip_no) REFERENCES trip_plan(trip_no),
  CONSTRAINT fk_shipment_task_truck_id FOREIGN KEY (truck_id) REFERENCES truck_master(truck_id),
  CONSTRAINT fk_shipment_task_driver_id FOREIGN KEY (driver_id) REFERENCES driver_master(driver_code)
) ENGINE = InnoDB;

CREATE TABLE truck_load_sheet (
  sheet_no VARCHAR(64) NOT NULL,
  trip_no VARCHAR(64) NOT NULL,
  truck_id VARCHAR(32) NOT NULL,
  load_tm DATETIME NOT NULL,
  status_cd VARCHAR(32) NOT NULL,
  PRIMARY KEY (sheet_no),
  KEY idx_truck_load_sheet_trip_no (trip_no),
  KEY idx_truck_load_sheet_truck_id (truck_id),
  KEY idx_truck_load_sheet_status_cd (status_cd),
  CONSTRAINT fk_truck_load_sheet_trip_no FOREIGN KEY (trip_no) REFERENCES trip_plan(trip_no),
  CONSTRAINT fk_truck_load_sheet_truck_id FOREIGN KEY (truck_id) REFERENCES truck_master(truck_id)
) ENGINE = InnoDB;
