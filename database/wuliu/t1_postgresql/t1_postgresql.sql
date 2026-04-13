DROP TABLE IF EXISTS maintenance_record CASCADE;
DROP TABLE IF EXISTS fuel_record CASCADE;
DROP TABLE IF EXISTS transport_exception CASCADE;
DROP TABLE IF EXISTS load_manifest_item CASCADE;
DROP TABLE IF EXISTS load_manifest CASCADE;
DROP TABLE IF EXISTS transport_task CASCADE;
DROP TABLE IF EXISTS departure_schedule CASCADE;
DROP TABLE IF EXISTS route_plan CASCADE;
DROP TABLE IF EXISTS driver CASCADE;
DROP TABLE IF EXISTS vehicle CASCADE;

CREATE TABLE vehicle (
  vehicle_id VARCHAR(32) PRIMARY KEY,
  plate_no VARCHAR(32) NOT NULL,
  vehicle_type VARCHAR(32) NOT NULL,
  load_capacity_kg NUMERIC(10, 2) NOT NULL,
  volume_capacity_m3 NUMERIC(10, 3) NOT NULL,
  temperature_type VARCHAR(16) NOT NULL,
  status VARCHAR(32) NOT NULL,
  purchase_date DATE NOT NULL
);

CREATE TABLE driver (
  driver_id VARCHAR(32) PRIMARY KEY,
  driver_name VARCHAR(64) NOT NULL,
  phone_no VARCHAR(32) NOT NULL,
  license_no VARCHAR(64) NOT NULL,
  license_type VARCHAR(16) NOT NULL,
  employment_status VARCHAR(16) NOT NULL,
  hire_date DATE NOT NULL,
  id_card_no VARCHAR(32) NOT NULL,
  driver_level VARCHAR(16) NOT NULL,
  status VARCHAR(32) NOT NULL
);

CREATE TABLE route_plan (
  route_id VARCHAR(32) PRIMARY KEY,
  route_code VARCHAR(64) NOT NULL,
  origin_node_id VARCHAR(32) NOT NULL,
  destination_node_id VARCHAR(32) NOT NULL,
  distance_km NUMERIC(10, 2) NOT NULL,
  standard_duration_hour NUMERIC(10, 2) NOT NULL,
  route_status VARCHAR(32) NOT NULL,
  transport_mode VARCHAR(32) NOT NULL,
  toll_fee_est NUMERIC(12, 2) NOT NULL,
  fuel_cost_est NUMERIC(12, 2) NOT NULL
);

CREATE TABLE departure_schedule (
  schedule_id VARCHAR(32) PRIMARY KEY,
  route_id VARCHAR(32) NOT NULL,
  vehicle_id VARCHAR(32) NOT NULL,
  driver_id VARCHAR(32) NOT NULL,
  planned_departure_time TIMESTAMP NOT NULL,
  actual_departure_time TIMESTAMP,
  planned_arrival_time TIMESTAMP NOT NULL,
  actual_arrival_time TIMESTAMP,
  schedule_status VARCHAR(32) NOT NULL,
  CONSTRAINT fk_departure_schedule_route FOREIGN KEY (route_id) REFERENCES route_plan(route_id),
  CONSTRAINT fk_departure_schedule_vehicle FOREIGN KEY (vehicle_id) REFERENCES vehicle(vehicle_id),
  CONSTRAINT fk_departure_schedule_driver FOREIGN KEY (driver_id) REFERENCES driver(driver_id)
);

CREATE TABLE transport_task (
  task_id VARCHAR(32) PRIMARY KEY,
  sub_waybill_id VARCHAR(32) NOT NULL,
  schedule_id VARCHAR(32) NOT NULL,
  vehicle_id VARCHAR(32) NOT NULL,
  driver_id VARCHAR(32) NOT NULL,
  task_status VARCHAR(32) NOT NULL,
  pickup_node_id VARCHAR(32) NOT NULL,
  delivery_node_id VARCHAR(32) NOT NULL,
  CONSTRAINT fk_transport_task_schedule FOREIGN KEY (schedule_id) REFERENCES departure_schedule(schedule_id),
  CONSTRAINT fk_transport_task_vehicle FOREIGN KEY (vehicle_id) REFERENCES vehicle(vehicle_id),
  CONSTRAINT fk_transport_task_driver FOREIGN KEY (driver_id) REFERENCES driver(driver_id)
);

CREATE TABLE load_manifest (
  manifest_id VARCHAR(32) PRIMARY KEY,
  manifest_no VARCHAR(64) NOT NULL,
  schedule_id VARCHAR(32) NOT NULL,
  vehicle_id VARCHAR(32) NOT NULL,
  load_time TIMESTAMP NOT NULL,
  loaded_by VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  CONSTRAINT fk_load_manifest_schedule FOREIGN KEY (schedule_id) REFERENCES departure_schedule(schedule_id),
  CONSTRAINT fk_load_manifest_vehicle FOREIGN KEY (vehicle_id) REFERENCES vehicle(vehicle_id)
);

CREATE TABLE load_manifest_item (
  item_id VARCHAR(32) PRIMARY KEY,
  manifest_id VARCHAR(32) NOT NULL,
  package_no VARCHAR(64) NOT NULL,
  waybill_no VARCHAR(64) NOT NULL,
  sku_id VARCHAR(32) NOT NULL,
  load_qty INT NOT NULL,
  weight_kg NUMERIC(10, 2) NOT NULL,
  volume_m3 NUMERIC(10, 3) NOT NULL,
  load_status VARCHAR(32) NOT NULL,
  CONSTRAINT fk_load_manifest_item_manifest FOREIGN KEY (manifest_id) REFERENCES load_manifest(manifest_id)
);

CREATE TABLE transport_exception (
  exception_id VARCHAR(32) PRIMARY KEY,
  task_id VARCHAR(32) NOT NULL,
  exception_type VARCHAR(32) NOT NULL,
  occur_time TIMESTAMP NOT NULL,
  occur_location VARCHAR(64) NOT NULL,
  description TEXT NOT NULL,
  process_status VARCHAR(32) NOT NULL,
  CONSTRAINT fk_transport_exception_task FOREIGN KEY (task_id) REFERENCES transport_task(task_id)
);

CREATE TABLE fuel_record (
  fuel_record_id VARCHAR(32) PRIMARY KEY,
  vehicle_id VARCHAR(32) NOT NULL,
  schedule_id VARCHAR(32) NOT NULL,
  fuel_volume NUMERIC(10, 2) NOT NULL,
  fuel_amount NUMERIC(12, 2) NOT NULL,
  fuel_time TIMESTAMP NOT NULL,
  fuel_station_name VARCHAR(128) NOT NULL,
  oil_type VARCHAR(32) NOT NULL,
  mileage_km INT NOT NULL,
  CONSTRAINT fk_fuel_record_vehicle FOREIGN KEY (vehicle_id) REFERENCES vehicle(vehicle_id),
  CONSTRAINT fk_fuel_record_schedule FOREIGN KEY (schedule_id) REFERENCES departure_schedule(schedule_id)
);

CREATE TABLE maintenance_record (
  maintenance_id VARCHAR(32) PRIMARY KEY,
  vehicle_id VARCHAR(32) NOT NULL,
  maintenance_type VARCHAR(32) NOT NULL,
  maintenance_desc TEXT NOT NULL,
  maintenance_time TIMESTAMP NOT NULL,
  cost_amount NUMERIC(12, 2) NOT NULL,
  service_vendor VARCHAR(128) NOT NULL,
  status VARCHAR(32) NOT NULL,
  CONSTRAINT fk_maintenance_record_vehicle FOREIGN KEY (vehicle_id) REFERENCES vehicle(vehicle_id)
);

CREATE UNIQUE INDEX ux_vehicle_plate_no ON vehicle(plate_no);
CREATE INDEX idx_vehicle_status ON vehicle(status);
CREATE INDEX idx_vehicle_type ON vehicle(vehicle_type);

CREATE INDEX idx_driver_name ON driver(driver_name);
CREATE INDEX idx_driver_phone_no ON driver(phone_no);
CREATE INDEX idx_driver_status ON driver(status);

CREATE INDEX idx_route_plan_route_code ON route_plan(route_code);
CREATE INDEX idx_route_plan_origin_node_id ON route_plan(origin_node_id);
CREATE INDEX idx_route_plan_destination_node_id ON route_plan(destination_node_id);
CREATE INDEX idx_route_plan_route_status ON route_plan(route_status);

CREATE INDEX idx_departure_schedule_route_id ON departure_schedule(route_id);
CREATE INDEX idx_departure_schedule_vehicle_id ON departure_schedule(vehicle_id);
CREATE INDEX idx_departure_schedule_driver_id ON departure_schedule(driver_id);
CREATE INDEX idx_departure_schedule_status ON departure_schedule(schedule_status);
CREATE INDEX idx_departure_schedule_planned_departure_time ON departure_schedule(planned_departure_time);

CREATE INDEX idx_transport_task_sub_waybill_id ON transport_task(sub_waybill_id);
CREATE INDEX idx_transport_task_schedule_id ON transport_task(schedule_id);
CREATE INDEX idx_transport_task_task_status ON transport_task(task_status);
CREATE INDEX idx_transport_task_pickup_delivery ON transport_task(pickup_node_id, delivery_node_id);

CREATE UNIQUE INDEX ux_load_manifest_manifest_no ON load_manifest(manifest_no);
CREATE INDEX idx_load_manifest_schedule_id ON load_manifest(schedule_id);
CREATE INDEX idx_load_manifest_vehicle_id ON load_manifest(vehicle_id);
CREATE INDEX idx_load_manifest_status ON load_manifest(status);

CREATE INDEX idx_load_manifest_item_manifest_id ON load_manifest_item(manifest_id);
CREATE INDEX idx_load_manifest_item_package_no ON load_manifest_item(package_no);
CREATE INDEX idx_load_manifest_item_waybill_no ON load_manifest_item(waybill_no);

CREATE INDEX idx_transport_exception_task_id ON transport_exception(task_id);
CREATE INDEX idx_transport_exception_type ON transport_exception(exception_type);
CREATE INDEX idx_transport_exception_process_status ON transport_exception(process_status);
CREATE INDEX idx_transport_exception_occur_time ON transport_exception(occur_time);

CREATE INDEX idx_fuel_record_vehicle_id ON fuel_record(vehicle_id);
CREATE INDEX idx_fuel_record_schedule_id ON fuel_record(schedule_id);
CREATE INDEX idx_fuel_record_fuel_time ON fuel_record(fuel_time);

CREATE INDEX idx_maintenance_record_vehicle_id ON maintenance_record(vehicle_id);
CREATE INDEX idx_maintenance_record_maintenance_time ON maintenance_record(maintenance_time);
CREATE INDEX idx_maintenance_record_status ON maintenance_record(status);
