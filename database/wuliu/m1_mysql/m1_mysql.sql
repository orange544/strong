DROP DATABASE IF EXISTS wuliu_m1_mysql_db;

CREATE DATABASE IF NOT EXISTS wuliu_m1_mysql_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE wuliu_m1_mysql_db;

CREATE TABLE IF NOT EXISTS customer (
  customer_id VARCHAR(32) NOT NULL,
  customer_name VARCHAR(128) NOT NULL,
  customer_type VARCHAR(32) NOT NULL,
  special_requirement VARCHAR(255),
  detail_address VARCHAR(255) NOT NULL,
  credit_level VARCHAR(16),
  settlement_type VARCHAR(32),
  status VARCHAR(16) NOT NULL,
  PRIMARY KEY (customer_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS product_sku (
  sku_id VARCHAR(32) NOT NULL,
  sku_name VARCHAR(128) NOT NULL,
  short_name VARCHAR(64),
  product_category_name VARCHAR(64),
  brand_name VARCHAR(64),
  spec_model VARCHAR(64),
  package_type VARCHAR(128),
  units_per_box INT,
  fragile_flag CHAR(1) NOT NULL DEFAULT 'N',
  shelf_life_days INT,
  cold_chain_flag CHAR(1) NOT NULL DEFAULT 'N',
  hazardous_flag CHAR(1) NOT NULL DEFAULT 'N',
  storage_requirement VARCHAR(255),
  transport_requirement VARCHAR(255),
  PRIMARY KEY (sku_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS warehouse_master (
  warehouse_id VARCHAR(32) NOT NULL,
  warehouse_code VARCHAR(32) NOT NULL,
  warehouse_name VARCHAR(128) NOT NULL,
  warehouse_type VARCHAR(32),
  capacity_desc VARCHAR(128),
  temperature_type VARCHAR(32),
  dispatch_time_window VARCHAR(64),
  status VARCHAR(16) NOT NULL,
  PRIMARY KEY (warehouse_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sales_order (
  order_id VARCHAR(40) NOT NULL,
  order_no VARCHAR(40) NOT NULL,
  customer_id VARCHAR(32) NOT NULL,
  consignee_id VARCHAR(32) NOT NULL,
  order_status VARCHAR(16) NOT NULL,
  order_source VARCHAR(32) NOT NULL,
  order_time DATETIME NOT NULL,
  requested_ship_time DATETIME,
  requested_delivery_time DATETIME,
  priority_level VARCHAR(16),
  total_amount DECIMAL(12,2) NOT NULL,
  total_qty INT NOT NULL,
  total_weight_kg DECIMAL(12,3) NOT NULL,
  total_volume_m3 DECIMAL(12,4) NOT NULL,
  payment_status VARCHAR(16) NOT NULL,
  remark VARCHAR(255),
  PRIMARY KEY (order_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sales_order_item (
  item_id VARCHAR(40) NOT NULL,
  order_code VARCHAR(40) NOT NULL,
  sku_code VARCHAR(32) NOT NULL,
  order_qty INT NOT NULL,
  shipped_qty INT NOT NULL DEFAULT 0,
  unit_price DECIMAL(12,2) NOT NULL,
  amount DECIMAL(12,2) NOT NULL,
  batch_requirement VARCHAR(128),
  packing_requirement VARCHAR(128),
  remark VARCHAR(255),
  PRIMARY KEY (item_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS shipment_request (
  shipment_request_id VARCHAR(40) NOT NULL,
  request_no VARCHAR(40) NOT NULL,
  order_id VARCHAR(40) NOT NULL,
  source_warehouse_id VARCHAR(32) NOT NULL,
  shipping_mode VARCHAR(32) NOT NULL,
  service_level VARCHAR(32) NOT NULL,
  urgency_level VARCHAR(16) NOT NULL,
  request_status VARCHAR(16) NOT NULL,
  planned_ship_time DATETIME NOT NULL,
  actual_ship_time DATETIME NULL,
  temperature_requirement VARCHAR(64),
  loading_requirement VARCHAR(128),
  sign_requirement VARCHAR(128),
  external_waybill_no VARCHAR(64),
  remark VARCHAR(255),
  PRIMARY KEY (shipment_request_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inventory_snapshot (
  snapshot_id VARCHAR(40) NOT NULL,
  house_id VARCHAR(32) NOT NULL,
  warehouse_code VARCHAR(32) NOT NULL,
  product_id VARCHAR(32) NOT NULL,
  available_qty INT NOT NULL,
  locked_qty INT NOT NULL DEFAULT 0,
  in_transit_qty INT NOT NULL DEFAULT 0,
  safety_stock INT NOT NULL DEFAULT 0,
  batch_no VARCHAR(40),
  snapshot_time DATETIME NOT NULL,
  PRIMARY KEY (snapshot_id)
) ENGINE=InnoDB;

CREATE UNIQUE INDEX uk_customer_id ON customer (customer_id);
CREATE INDEX idx_customer_name ON customer (customer_name);
CREATE INDEX idx_customer_status ON customer (status);
CREATE INDEX idx_customer_type_status ON customer (customer_type, status);

CREATE UNIQUE INDEX uk_product_sku_id ON product_sku (sku_id);
CREATE INDEX idx_product_sku_name ON product_sku (sku_name);
CREATE INDEX idx_product_category_name ON product_sku (product_category_name);
CREATE INDEX idx_product_cold_chain_flag ON product_sku (cold_chain_flag);

CREATE UNIQUE INDEX uk_warehouse_master_id ON warehouse_master (warehouse_id);
CREATE INDEX idx_warehouse_master_code ON warehouse_master (warehouse_code);
CREATE INDEX idx_warehouse_master_name ON warehouse_master (warehouse_name);
CREATE INDEX idx_warehouse_master_status ON warehouse_master (status);

CREATE UNIQUE INDEX uk_sales_order_id ON sales_order (order_id);
CREATE UNIQUE INDEX uk_sales_order_no ON sales_order (order_no);
CREATE INDEX idx_sales_order_customer_id ON sales_order (customer_id);
CREATE INDEX idx_sales_order_status ON sales_order (order_status);
CREATE INDEX idx_sales_order_time ON sales_order (order_time);
CREATE INDEX idx_sales_order_customer_status ON sales_order (customer_id, order_status);
CREATE INDEX idx_sales_order_ship_priority ON sales_order (requested_ship_time, priority_level);

CREATE UNIQUE INDEX uk_sales_order_item_id ON sales_order_item (item_id);
CREATE INDEX idx_sales_order_item_order_code ON sales_order_item (order_code);
CREATE INDEX idx_sales_order_item_sku_code ON sales_order_item (sku_code);
CREATE INDEX idx_sales_order_item_order_sku ON sales_order_item (order_code, sku_code);

CREATE UNIQUE INDEX uk_shipment_request_id ON shipment_request (shipment_request_id);
CREATE UNIQUE INDEX uk_shipment_request_no ON shipment_request (request_no);
CREATE INDEX idx_shipment_request_order_id ON shipment_request (order_id);
CREATE INDEX idx_shipment_request_status ON shipment_request (request_status);
CREATE INDEX idx_shipment_request_plan_ship_time ON shipment_request (planned_ship_time);
CREATE INDEX idx_shipment_request_wh_status ON shipment_request (source_warehouse_id, request_status);

CREATE UNIQUE INDEX uk_inventory_snapshot_id ON inventory_snapshot (snapshot_id);
CREATE INDEX idx_inventory_snapshot_house_id ON inventory_snapshot (house_id);
CREATE INDEX idx_inventory_snapshot_product_id ON inventory_snapshot (product_id);
CREATE INDEX idx_inventory_snapshot_time ON inventory_snapshot (snapshot_time);
CREATE INDEX idx_inventory_snapshot_house_product_time ON inventory_snapshot (house_id, product_id, snapshot_time);
