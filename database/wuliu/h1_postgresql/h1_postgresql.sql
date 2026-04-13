DROP TABLE IF EXISTS sorting_exception CASCADE;
DROP TABLE IF EXISTS detention_package CASCADE;
DROP TABLE IF EXISTS cage_manifest CASCADE;
DROP TABLE IF EXISTS transfer_batch CASCADE;
DROP TABLE IF EXISTS sorting_task CASCADE;
DROP TABLE IF EXISTS sorting_chute CASCADE;
DROP TABLE IF EXISTS arrival_scan CASCADE;

CREATE TABLE arrival_scan (
  scan_id VARCHAR(32) PRIMARY KEY,
  package_no VARCHAR(64) NOT NULL,
  waybill_no VARCHAR(64) NOT NULL,
  station_id VARCHAR(32) NOT NULL,
  scan_time TIMESTAMP NOT NULL,
  scan_type VARCHAR(32) NOT NULL,
  operator_id VARCHAR(32) NOT NULL,
  device_id VARCHAR(32) NOT NULL
);

CREATE TABLE sorting_chute (
  chute_id VARCHAR(32) PRIMARY KEY,
  station_id VARCHAR(32) NOT NULL,
  chute_code VARCHAR(64) NOT NULL,
  destination_type VARCHAR(32) NOT NULL,
  capacity_qty INT NOT NULL,
  status VARCHAR(32) NOT NULL,
  bind_node_id VARCHAR(32) NOT NULL
);

CREATE TABLE sorting_task (
  sorting_task_id VARCHAR(32) PRIMARY KEY,
  station_id VARCHAR(32) NOT NULL,
  package_no VARCHAR(64) NOT NULL,
  destination_node_id VARCHAR(32) NOT NULL,
  chute_id VARCHAR(32) NOT NULL,
  task_status VARCHAR(32) NOT NULL,
  assigned_user VARCHAR(64) NOT NULL,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  CONSTRAINT fk_sorting_task_chute FOREIGN KEY (chute_id) REFERENCES sorting_chute(chute_id)
);

CREATE TABLE transfer_batch (
  batch_id VARCHAR(32) PRIMARY KEY,
  batch_no VARCHAR(64) NOT NULL UNIQUE,
  station_id VARCHAR(32) NOT NULL,
  destination_node_id VARCHAR(32) NOT NULL,
  vehicle_id VARCHAR(32) NOT NULL,
  planned_departure_time TIMESTAMP NOT NULL,
  actual_departure_time TIMESTAMP,
  batch_status VARCHAR(32) NOT NULL
);

CREATE TABLE cage_manifest (
  cage_id VARCHAR(32) PRIMARY KEY,
  cage_code VARCHAR(64) NOT NULL,
  station_id VARCHAR(32) NOT NULL,
  batch_no VARCHAR(64) NOT NULL,
  destination_node_id VARCHAR(32) NOT NULL,
  loaded_package_qty INT NOT NULL,
  seal_no VARCHAR(64) NOT NULL,
  dispatch_time TIMESTAMP,
  status VARCHAR(32) NOT NULL,
  CONSTRAINT fk_cage_manifest_batch_no FOREIGN KEY (batch_no) REFERENCES transfer_batch(batch_no)
);

CREATE TABLE detention_package (
  detention_id VARCHAR(32) PRIMARY KEY,
  package_no VARCHAR(64) NOT NULL,
  waybill_no VARCHAR(64) NOT NULL,
  station_id VARCHAR(32) NOT NULL,
  detention_reason TEXT NOT NULL,
  detention_start_time TIMESTAMP NOT NULL,
  process_status VARCHAR(32) NOT NULL,
  remark TEXT
);

CREATE TABLE sorting_exception (
  exception_id VARCHAR(32) PRIMARY KEY,
  package_no VARCHAR(64) NOT NULL,
  station_id VARCHAR(32) NOT NULL,
  exception_type VARCHAR(32) NOT NULL,
  occur_time TIMESTAMP NOT NULL,
  description TEXT NOT NULL,
  process_status VARCHAR(32) NOT NULL
);

CREATE INDEX idx_arrival_scan_package_no ON arrival_scan(package_no);
CREATE INDEX idx_arrival_scan_waybill_no ON arrival_scan(waybill_no);
CREATE INDEX idx_arrival_scan_station_id ON arrival_scan(station_id);
CREATE INDEX idx_arrival_scan_scan_time ON arrival_scan(scan_time);
CREATE INDEX idx_arrival_scan_station_time ON arrival_scan(station_id, scan_time);

CREATE INDEX idx_sorting_task_package_no ON sorting_task(package_no);
CREATE INDEX idx_sorting_task_station_id ON sorting_task(station_id);
CREATE INDEX idx_sorting_task_task_status ON sorting_task(task_status);
CREATE INDEX idx_sorting_task_destination_node_id ON sorting_task(destination_node_id);

CREATE UNIQUE INDEX ux_sorting_chute_chute_code ON sorting_chute(chute_code);
CREATE INDEX idx_sorting_chute_station_id ON sorting_chute(station_id);
CREATE INDEX idx_sorting_chute_status ON sorting_chute(status);

CREATE INDEX idx_transfer_batch_station_id ON transfer_batch(station_id);
CREATE INDEX idx_transfer_batch_destination_node_id ON transfer_batch(destination_node_id);
CREATE INDEX idx_transfer_batch_batch_status ON transfer_batch(batch_status);
CREATE INDEX idx_transfer_batch_planned_departure_time ON transfer_batch(planned_departure_time);

CREATE UNIQUE INDEX ux_cage_manifest_cage_code ON cage_manifest(cage_code);
CREATE INDEX idx_cage_manifest_batch_no ON cage_manifest(batch_no);
CREATE INDEX idx_cage_manifest_station_id ON cage_manifest(station_id);
CREATE INDEX idx_cage_manifest_status ON cage_manifest(status);

CREATE INDEX idx_detention_package_package_no ON detention_package(package_no);
CREATE INDEX idx_detention_package_waybill_no ON detention_package(waybill_no);
CREATE INDEX idx_detention_package_station_id ON detention_package(station_id);
CREATE INDEX idx_detention_package_process_status ON detention_package(process_status);

CREATE INDEX idx_sorting_exception_package_no ON sorting_exception(package_no);
CREATE INDEX idx_sorting_exception_station_id ON sorting_exception(station_id);
CREATE INDEX idx_sorting_exception_exception_type ON sorting_exception(exception_type);
CREATE INDEX idx_sorting_exception_process_status ON sorting_exception(process_status);
