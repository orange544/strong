DROP DATABASE IF EXISTS wuliu_w2_mysql_db;

CREATE DATABASE IF NOT EXISTS wuliu_w2_mysql_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE wuliu_w2_mysql_db;

CREATE TABLE IF NOT EXISTS warehouse_ref (
  wh_id VARCHAR(32) NOT NULL,
  wh_code VARCHAR(32) NOT NULL,
  wh_name VARCHAR(128) NOT NULL,
  wh_type VARCHAR(32),
  province_name VARCHAR(32),
  city_name VARCHAR(32),
  addr_detail VARCHAR(255),
  manager VARCHAR(64),
  status_cd VARCHAR(16) NOT NULL,
  PRIMARY KEY (wh_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS asn_order (
  asn_no VARCHAR(40) NOT NULL,
  source_no VARCHAR(64) NOT NULL,
  wh_id VARCHAR(32) NOT NULL,
  arrival_plan_tm DATETIME NOT NULL,
  arrival_real_tm DATETIME NULL,
  doc_status VARCHAR(16) NOT NULL,
  PRIMARY KEY (asn_no)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS stock_balance (
  stock_id VARCHAR(40) NOT NULL,
  wh_id VARCHAR(32) NOT NULL,
  bin_no VARCHAR(40) NOT NULL,
  item_code VARCHAR(32) NOT NULL,
  lot_no VARCHAR(40),
  qty_on_hand INT NOT NULL,
  qty_hold INT NOT NULL DEFAULT 0,
  qty_bad INT NOT NULL DEFAULT 0,
  last_change_tm DATETIME NOT NULL,
  PRIMARY KEY (stock_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pick_work (
  work_no VARCHAR(40) NOT NULL,
  so_out_no VARCHAR(40) NOT NULL,
  item_code VARCHAR(32) NOT NULL,
  pick_bin_no VARCHAR(40) NOT NULL,
  pick_num INT NOT NULL,
  work_status VARCHAR(16) NOT NULL,
  worker_id VARCHAR(32) NOT NULL,
  begin_tm DATETIME,
  finish_tm DATETIME NULL,
  PRIMARY KEY (work_no)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS so_outbound (
  so_out_no VARCHAR(40) NOT NULL,
  wh_id VARCHAR(32) NOT NULL,
  dispatch_apply_no VARCHAR(40) NOT NULL,
  doc_status VARCHAR(16) NOT NULL,
  plan_out_tm DATETIME NOT NULL,
  real_out_tm DATETIME NULL,
  PRIMARY KEY (so_out_no)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS stock_flow (
  flow_id VARCHAR(40) NOT NULL,
  wh_id VARCHAR(32) NOT NULL,
  bin_no VARCHAR(40) NOT NULL,
  item_code VARCHAR(32) NOT NULL,
  lot_no VARCHAR(40),
  flow_type VARCHAR(32) NOT NULL,
  qty_chg INT NOT NULL,
  qty_before INT NOT NULL,
  qty_after INT NOT NULL,
  ref_no VARCHAR(64),
  operator_code VARCHAR(32),
  flow_tm DATETIME NOT NULL,
  memo VARCHAR(255),
  PRIMARY KEY (flow_id)
) ENGINE=InnoDB;

CREATE UNIQUE INDEX uk_warehouse_ref_id ON warehouse_ref (wh_id);
CREATE UNIQUE INDEX uk_warehouse_ref_code ON warehouse_ref (wh_code);
CREATE INDEX idx_warehouse_ref_name ON warehouse_ref (wh_name);
CREATE INDEX idx_warehouse_ref_status ON warehouse_ref (status_cd);

CREATE UNIQUE INDEX uk_asn_order_no ON asn_order (asn_no);
CREATE INDEX idx_asn_order_source_no ON asn_order (source_no);
CREATE INDEX idx_asn_order_wh_id ON asn_order (wh_id);
CREATE INDEX idx_asn_order_status ON asn_order (doc_status);
CREATE INDEX idx_asn_order_plan_tm ON asn_order (arrival_plan_tm);

CREATE UNIQUE INDEX uk_stock_balance_id ON stock_balance (stock_id);
CREATE INDEX idx_stock_balance_wh_id ON stock_balance (wh_id);
CREATE INDEX idx_stock_balance_item_code ON stock_balance (item_code);
CREATE INDEX idx_stock_balance_lot_no ON stock_balance (lot_no);
CREATE INDEX idx_stock_balance_wh_item_lot ON stock_balance (wh_id, item_code, lot_no);

CREATE UNIQUE INDEX uk_pick_work_no ON pick_work (work_no);
CREATE INDEX idx_pick_work_out_no ON pick_work (so_out_no);
CREATE INDEX idx_pick_work_status ON pick_work (work_status);
CREATE INDEX idx_pick_work_worker_id ON pick_work (worker_id);

CREATE UNIQUE INDEX uk_so_outbound_no ON so_outbound (so_out_no);
CREATE INDEX idx_so_outbound_wh_id ON so_outbound (wh_id);
CREATE INDEX idx_so_outbound_dispatch_apply_no ON so_outbound (dispatch_apply_no);
CREATE INDEX idx_so_outbound_status ON so_outbound (doc_status);
CREATE INDEX idx_so_outbound_plan_tm ON so_outbound (plan_out_tm);

CREATE UNIQUE INDEX uk_stock_flow_id ON stock_flow (flow_id);
CREATE INDEX idx_stock_flow_wh_id ON stock_flow (wh_id);
CREATE INDEX idx_stock_flow_item_code ON stock_flow (item_code);
CREATE INDEX idx_stock_flow_type ON stock_flow (flow_type);
CREATE INDEX idx_stock_flow_tm ON stock_flow (flow_tm);
CREATE INDEX idx_stock_flow_wh_item_tm ON stock_flow (wh_id, item_code, flow_tm);
