DROP DATABASE IF EXISTS wuliu_w1_mysql_db;

CREATE DATABASE IF NOT EXISTS wuliu_w1_mysql_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE wuliu_w1_mysql_db;

CREATE TABLE IF NOT EXISTS warehouse (
  warehouse_id VARCHAR(32) NOT NULL,
  warehouse_code VARCHAR(32) NOT NULL,
  warehouse_name VARCHAR(128) NOT NULL,
  warehouse_type VARCHAR(32),
  province VARCHAR(32),
  city VARCHAR(32),
  district VARCHAR(32),
  detail_address VARCHAR(255),
  manager_name VARCHAR(64),
  contact_phone VARCHAR(20),
  status VARCHAR(16) NOT NULL,
  PRIMARY KEY (warehouse_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS location_bin (
  bin_id VARCHAR(40) NOT NULL,
  warehouse_id VARCHAR(32) NOT NULL,
  bin_code VARCHAR(40) NOT NULL,
  bin_type VARCHAR(32),
  zone_code VARCHAR(32),
  aisle_no VARCHAR(16),
  rack_no VARCHAR(16),
  layer_no VARCHAR(16),
  capacity_qty INT,
  capacity_weight DECIMAL(12,3),
  status VARCHAR(16) NOT NULL,
  temperature_type VARCHAR(16),
  PRIMARY KEY (bin_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inbound_order (
  inbound_order_id VARCHAR(40) NOT NULL,
  inbound_order_no VARCHAR(40) NOT NULL,
  supplier_name VARCHAR(128) NOT NULL,
  warehouse_id VARCHAR(32) NOT NULL,
  inbound_type VARCHAR(32) NOT NULL,
  planned_arrival_time DATETIME NOT NULL,
  actual_arrival_time DATETIME NULL,
  order_status VARCHAR(16) NOT NULL,
  PRIMARY KEY (inbound_order_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inbound_item (
  item_id VARCHAR(40) NOT NULL,
  inbound_order_id VARCHAR(40) NOT NULL,
  sku_id VARCHAR(32) NOT NULL,
  expected_qty INT NOT NULL,
  actual_qty INT NOT NULL DEFAULT 0,
  batch_no VARCHAR(40),
  production_date DATE,
  expire_date DATE,
  quality_result VARCHAR(16),
  owner_code VARCHAR(32),
  remark VARCHAR(255),
  PRIMARY KEY (item_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inventory_balance (
  inventory_id VARCHAR(40) NOT NULL,
  warehouse_id VARCHAR(32) NOT NULL,
  bin_id VARCHAR(40) NOT NULL,
  sku_id VARCHAR(32) NOT NULL,
  batch_no VARCHAR(40),
  available_qty INT NOT NULL,
  locked_qty INT NOT NULL DEFAULT 0,
  damaged_qty INT NOT NULL DEFAULT 0,
  last_txn_time DATETIME,
  PRIMARY KEY (inventory_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS outbound_order (
  outbound_order_id VARCHAR(40) NOT NULL,
  outbound_order_no VARCHAR(40) NOT NULL,
  warehouse_id VARCHAR(32) NOT NULL,
  shipment_request_id VARCHAR(40) NOT NULL,
  order_status VARCHAR(16) NOT NULL,
  planned_outbound_time DATETIME NOT NULL,
  actual_outbound_time DATETIME NULL,
  PRIMARY KEY (outbound_order_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pick_task (
  pick_task_id VARCHAR(40) NOT NULL,
  outbound_order_id VARCHAR(40) NOT NULL,
  sku_id VARCHAR(32) NOT NULL,
  pick_bin_id VARCHAR(40) NOT NULL,
  pick_qty INT NOT NULL,
  task_status VARCHAR(16) NOT NULL,
  picker_id VARCHAR(32) NOT NULL,
  start_time DATETIME,
  end_time DATETIME NULL,
  PRIMARY KEY (pick_task_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS inventory_txn (
  txn_id VARCHAR(40) NOT NULL,
  warehouse_id VARCHAR(32) NOT NULL,
  bin_id VARCHAR(40) NOT NULL,
  sku_id VARCHAR(32) NOT NULL,
  batch_no VARCHAR(40),
  txn_type VARCHAR(32) NOT NULL,
  qty_delta INT NOT NULL,
  before_qty INT NOT NULL,
  after_qty INT NOT NULL,
  biz_ref_no VARCHAR(64),
  operator_id VARCHAR(32),
  txn_time DATETIME NOT NULL,
  remark VARCHAR(255),
  PRIMARY KEY (txn_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS cycle_count (
  count_id VARCHAR(40) NOT NULL,
  warehouse_id VARCHAR(32) NOT NULL,
  bin_id VARCHAR(40) NOT NULL,
  sku_id VARCHAR(32) NOT NULL,
  system_qty INT NOT NULL,
  counted_qty INT NOT NULL,
  variance_qty INT NOT NULL,
  count_type VARCHAR(32),
  count_time DATETIME NOT NULL,
  counter_id VARCHAR(32),
  review_status VARCHAR(16),
  remark VARCHAR(255),
  PRIMARY KEY (count_id)
) ENGINE=InnoDB;

CREATE UNIQUE INDEX uk_warehouse_id ON warehouse (warehouse_id);
CREATE UNIQUE INDEX uk_warehouse_code ON warehouse (warehouse_code);
CREATE INDEX idx_warehouse_name ON warehouse (warehouse_name);
CREATE INDEX idx_warehouse_status ON warehouse (status);

CREATE UNIQUE INDEX uk_location_bin_id ON location_bin (bin_id);
CREATE UNIQUE INDEX uk_location_bin_code ON location_bin (bin_code);
CREATE INDEX idx_location_bin_warehouse_id ON location_bin (warehouse_id);
CREATE INDEX idx_location_bin_status ON location_bin (status);
CREATE INDEX idx_location_bin_warehouse_zone ON location_bin (warehouse_id, zone_code);

CREATE UNIQUE INDEX uk_inbound_order_id ON inbound_order (inbound_order_id);
CREATE UNIQUE INDEX uk_inbound_order_no ON inbound_order (inbound_order_no);
CREATE INDEX idx_inbound_order_warehouse_id ON inbound_order (warehouse_id);
CREATE INDEX idx_inbound_order_status ON inbound_order (order_status);
CREATE INDEX idx_inbound_order_plan_arrival ON inbound_order (planned_arrival_time);

CREATE UNIQUE INDEX uk_inbound_item_id ON inbound_item (item_id);
CREATE INDEX idx_inbound_item_order_id ON inbound_item (inbound_order_id);
CREATE INDEX idx_inbound_item_sku_id ON inbound_item (sku_id);
CREATE INDEX idx_inbound_item_batch_no ON inbound_item (batch_no);
CREATE INDEX idx_inbound_item_order_sku ON inbound_item (inbound_order_id, sku_id);

CREATE UNIQUE INDEX uk_inventory_balance_id ON inventory_balance (inventory_id);
CREATE INDEX idx_inventory_balance_warehouse_id ON inventory_balance (warehouse_id);
CREATE INDEX idx_inventory_balance_bin_id ON inventory_balance (bin_id);
CREATE INDEX idx_inventory_balance_sku_id ON inventory_balance (sku_id);
CREATE INDEX idx_inventory_balance_batch_no ON inventory_balance (batch_no);
CREATE INDEX idx_inventory_balance_wh_sku_batch ON inventory_balance (warehouse_id, sku_id, batch_no);

CREATE UNIQUE INDEX uk_outbound_order_id ON outbound_order (outbound_order_id);
CREATE UNIQUE INDEX uk_outbound_order_no ON outbound_order (outbound_order_no);
CREATE INDEX idx_outbound_order_warehouse_id ON outbound_order (warehouse_id);
CREATE INDEX idx_outbound_order_shipment_request_id ON outbound_order (shipment_request_id);
CREATE INDEX idx_outbound_order_status ON outbound_order (order_status);
CREATE INDEX idx_outbound_order_plan_time ON outbound_order (planned_outbound_time);

CREATE UNIQUE INDEX uk_pick_task_id ON pick_task (pick_task_id);
CREATE INDEX idx_pick_task_outbound_order_id ON pick_task (outbound_order_id);
CREATE INDEX idx_pick_task_status ON pick_task (task_status);
CREATE INDEX idx_pick_task_picker_id ON pick_task (picker_id);
CREATE INDEX idx_pick_task_bin_status ON pick_task (pick_bin_id, task_status);

CREATE UNIQUE INDEX uk_inventory_txn_id ON inventory_txn (txn_id);
CREATE INDEX idx_inventory_txn_warehouse_id ON inventory_txn (warehouse_id);
CREATE INDEX idx_inventory_txn_sku_id ON inventory_txn (sku_id);
CREATE INDEX idx_inventory_txn_type ON inventory_txn (txn_type);
CREATE INDEX idx_inventory_txn_time ON inventory_txn (txn_time);
CREATE INDEX idx_inventory_txn_wh_sku_time ON inventory_txn (warehouse_id, sku_id, txn_time);

CREATE UNIQUE INDEX uk_cycle_count_id ON cycle_count (count_id);
CREATE INDEX idx_cycle_count_warehouse_id ON cycle_count (warehouse_id);
CREATE INDEX idx_cycle_count_time ON cycle_count (count_time);
CREATE INDEX idx_cycle_count_review_status ON cycle_count (review_status);
