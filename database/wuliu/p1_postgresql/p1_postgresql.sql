DROP SCHEMA IF EXISTS p1 CASCADE;
CREATE SCHEMA p1;
SET search_path TO p1;

CREATE TABLE shipper_account (
  shipper_id VARCHAR(32) PRIMARY KEY,
  shipper_code VARCHAR(32) NOT NULL UNIQUE,
  shipper_name VARCHAR(128) NOT NULL,
  account_level VARCHAR(16),
  industry_type VARCHAR(32),
  contact_name VARCHAR(64),
  contact_phone VARCHAR(20),
  contract_start_date DATE,
  contract_end_date DATE,
  status VARCHAR(16) NOT NULL
);

CREATE TABLE client_contract (
  contract_id VARCHAR(40) PRIMARY KEY,
  contract_no VARCHAR(40) NOT NULL UNIQUE,
  shipper_id VARCHAR(32) NOT NULL REFERENCES shipper_account(shipper_id),
  service_scope VARCHAR(128),
  pricing_rule VARCHAR(128),
  settlement_cycle VARCHAR(32),
  sla_desc VARCHAR(255),
  effective_date DATE,
  expire_date DATE,
  status VARCHAR(16) NOT NULL,
  sign_company_name VARCHAR(128),
  invoice_rule VARCHAR(128),
  penalty_rule VARCHAR(128),
  remark VARCHAR(255)
);

CREATE TABLE master_waybill (
  master_waybill_id VARCHAR(40) PRIMARY KEY,
  master_waybill_no VARCHAR(40) NOT NULL UNIQUE,
  order_id VARCHAR(40) NOT NULL,
  shipper_id VARCHAR(32) NOT NULL REFERENCES shipper_account(shipper_id),
  origin_node_id VARCHAR(40) NOT NULL,
  destination_node_id VARCHAR(40) NOT NULL,
  waybill_status VARCHAR(20) NOT NULL,
  planned_departure_time TIMESTAMP NOT NULL,
  planned_arrival_time TIMESTAMP NOT NULL,
  actual_departure_time TIMESTAMP NULL,
  actual_arrival_time TIMESTAMP NULL,
  total_package_qty INT NOT NULL,
  total_weight_kg NUMERIC(12,3) NOT NULL,
  total_volume_m3 NUMERIC(12,4) NOT NULL
);

CREATE TABLE sub_waybill (
  sub_waybill_id VARCHAR(40) PRIMARY KEY,
  sub_waybill_no VARCHAR(40) NOT NULL UNIQUE,
  master_waybill_id VARCHAR(40) NOT NULL REFERENCES master_waybill(master_waybill_id),
  carrier_id VARCHAR(32) NOT NULL,
  origin_node_id VARCHAR(40) NOT NULL,
  destination_node_id VARCHAR(40) NOT NULL,
  sub_waybill_status VARCHAR(20) NOT NULL,
  package_qty INT NOT NULL,
  weight_kg NUMERIC(12,3) NOT NULL,
  volume_m3 NUMERIC(12,4) NOT NULL
);

CREATE TABLE transport_order (
  transport_order_id VARCHAR(40) PRIMARY KEY,
  transport_order_no VARCHAR(40) NOT NULL UNIQUE,
  master_waybill_id VARCHAR(40) NOT NULL REFERENCES master_waybill(master_waybill_id),
  transport_type VARCHAR(32) NOT NULL,
  service_level VARCHAR(32) NOT NULL,
  dispatch_status VARCHAR(20) NOT NULL,
  pickup_time TIMESTAMP NOT NULL,
  delivery_deadline TIMESTAMP NOT NULL,
  remark VARCHAR(255)
);

CREATE TABLE carrier_assignment (
  assignment_id VARCHAR(40) PRIMARY KEY,
  transport_order_id VARCHAR(40) NOT NULL REFERENCES transport_order(transport_order_id),
  carrier_id VARCHAR(32) NOT NULL,
  carrier_name VARCHAR(128) NOT NULL,
  assignment_status VARCHAR(20) NOT NULL,
  assigned_time TIMESTAMP NOT NULL,
  accepted_time TIMESTAMP NULL,
  dispatch_user VARCHAR(64),
  remark VARCHAR(255)
);

CREATE TABLE freight_settlement (
  settlement_id VARCHAR(40) PRIMARY KEY,
  transport_order_id VARCHAR(40) NOT NULL REFERENCES transport_order(transport_order_id),
  carrier_id VARCHAR(32) NOT NULL,
  receivable_amount NUMERIC(12,2) NOT NULL,
  payable_amount NUMERIC(12,2) NOT NULL,
  settlement_status VARCHAR(20) NOT NULL,
  settlement_date DATE,
  invoice_no VARCHAR(64)
);

CREATE UNIQUE INDEX uk_shipper_account_id ON shipper_account(shipper_id);
CREATE UNIQUE INDEX uk_shipper_account_code ON shipper_account(shipper_code);
CREATE INDEX idx_shipper_account_name ON shipper_account(shipper_name);
CREATE INDEX idx_shipper_account_status ON shipper_account(status);

CREATE UNIQUE INDEX uk_client_contract_id ON client_contract(contract_id);
CREATE UNIQUE INDEX uk_client_contract_no ON client_contract(contract_no);
CREATE INDEX idx_client_contract_shipper_id ON client_contract(shipper_id);
CREATE INDEX idx_client_contract_status ON client_contract(status);
CREATE INDEX idx_client_contract_date_range ON client_contract(effective_date, expire_date);

CREATE UNIQUE INDEX uk_master_waybill_id ON master_waybill(master_waybill_id);
CREATE UNIQUE INDEX uk_master_waybill_no ON master_waybill(master_waybill_no);
CREATE INDEX idx_master_waybill_order_id ON master_waybill(order_id);
CREATE INDEX idx_master_waybill_shipper_id ON master_waybill(shipper_id);
CREATE INDEX idx_master_waybill_status ON master_waybill(waybill_status);
CREATE INDEX idx_master_waybill_node_pair ON master_waybill(origin_node_id, destination_node_id);
CREATE INDEX idx_master_waybill_plan_time ON master_waybill(planned_departure_time, planned_arrival_time);

CREATE UNIQUE INDEX uk_sub_waybill_id ON sub_waybill(sub_waybill_id);
CREATE UNIQUE INDEX uk_sub_waybill_no ON sub_waybill(sub_waybill_no);
CREATE INDEX idx_sub_waybill_master_id ON sub_waybill(master_waybill_id);
CREATE INDEX idx_sub_waybill_carrier_id ON sub_waybill(carrier_id);
CREATE INDEX idx_sub_waybill_status ON sub_waybill(sub_waybill_status);

CREATE UNIQUE INDEX uk_transport_order_id ON transport_order(transport_order_id);
CREATE UNIQUE INDEX uk_transport_order_no ON transport_order(transport_order_no);
CREATE INDEX idx_transport_order_master_id ON transport_order(master_waybill_id);
CREATE INDEX idx_transport_order_status ON transport_order(dispatch_status);
CREATE INDEX idx_transport_order_pickup_time ON transport_order(pickup_time);
CREATE INDEX idx_transport_order_service_status ON transport_order(service_level, dispatch_status);

CREATE UNIQUE INDEX uk_carrier_assignment_id ON carrier_assignment(assignment_id);
CREATE INDEX idx_carrier_assignment_order_id ON carrier_assignment(transport_order_id);
CREATE INDEX idx_carrier_assignment_carrier_id ON carrier_assignment(carrier_id);
CREATE INDEX idx_carrier_assignment_status ON carrier_assignment(assignment_status);
CREATE INDEX idx_carrier_assignment_carrier_status ON carrier_assignment(carrier_id, assignment_status);

CREATE UNIQUE INDEX uk_freight_settlement_id ON freight_settlement(settlement_id);
CREATE INDEX idx_freight_settlement_order_id ON freight_settlement(transport_order_id);
CREATE INDEX idx_freight_settlement_carrier_id ON freight_settlement(carrier_id);
CREATE INDEX idx_freight_settlement_status ON freight_settlement(settlement_status);
CREATE INDEX idx_freight_settlement_date ON freight_settlement(settlement_date);
