DROP DATABASE IF EXISTS wuliu_m2_mysql_db;

CREATE DATABASE IF NOT EXISTS wuliu_m2_mysql_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE wuliu_m2_mysql_db;

CREATE TABLE IF NOT EXISTS client_master (
  client_code VARCHAR(32) NOT NULL,
  client_name VARCHAR(128) NOT NULL,
  client_level VARCHAR(16),
  address_detail VARCHAR(255) NOT NULL,
  settle_mode VARCHAR(32),
  status_cd VARCHAR(16) NOT NULL,
  PRIMARY KEY (client_code)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS goods_item (
  product_id VARCHAR(32) NOT NULL,
  product_name VARCHAR(128) NOT NULL,
  brand VARCHAR(64),
  spec_desc VARCHAR(128),
  pkg_type VARCHAR(128),
  cold_flag CHAR(1) NOT NULL DEFAULT 'N',
  danger_flag CHAR(1) NOT NULL DEFAULT 'N',
  storage_rule VARCHAR(255),
  trans_rule VARCHAR(255),
  PRIMARY KEY (product_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS biz_order (
  biz_order_id VARCHAR(40) NOT NULL,
  sales_no VARCHAR(40) NOT NULL,
  client_code VARCHAR(32) NOT NULL,
  receiver_code VARCHAR(32) NOT NULL,
  status_cd VARCHAR(16) NOT NULL,
  source_type VARCHAR(32) NOT NULL,
  order_tm DATETIME NOT NULL,
  expect_ship_tm DATETIME NOT NULL,
  expect_arrive_tm DATETIME NOT NULL,
  priority_cd VARCHAR(16) NOT NULL,
  total_amt DECIMAL(12,2) NOT NULL,
  remark_txt VARCHAR(255),
  PRIMARY KEY (biz_order_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS biz_order_line (
  line_id VARCHAR(40) NOT NULL,
  biz_order_id VARCHAR(40) NOT NULL,
  product_id VARCHAR(32) NOT NULL,
  qty INT NOT NULL,
  sent_qty INT NOT NULL DEFAULT 0,
  price DECIMAL(12,2) NOT NULL,
  amount DECIMAL(12,2) NOT NULL,
  lot_rule VARCHAR(128),
  pack_rule VARCHAR(128),
  PRIMARY KEY (line_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS send_apply (
  send_apply_no VARCHAR(40) NOT NULL,
  biz_order_id VARCHAR(40) NOT NULL,
  house_id VARCHAR(32) NOT NULL,
  delivery_mode VARCHAR(32) NOT NULL,
  sla_level VARCHAR(32) NOT NULL,
  urgent_flag CHAR(1) NOT NULL DEFAULT 'N',
  apply_status VARCHAR(16) NOT NULL,
  plan_out_tm DATETIME NOT NULL,
  real_out_tm DATETIME NULL,
  temp_rule VARCHAR(64),
  sign_rule VARCHAR(128),
  carrier_bill_no VARCHAR(64),
  PRIMARY KEY (send_apply_no)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS stock_snapshot (
  snapshot_no VARCHAR(40) NOT NULL,
  house_id VARCHAR(32) NOT NULL,
  product_id VARCHAR(32) NOT NULL,
  free_stock INT NOT NULL,
  hold_stock INT NOT NULL DEFAULT 0,
  onway_stock INT NOT NULL DEFAULT 0,
  safe_stock INT NOT NULL DEFAULT 0,
  lot_no VARCHAR(40),
  snapshot_tm DATETIME NOT NULL,
  PRIMARY KEY (snapshot_no)
) ENGINE=InnoDB;

CREATE UNIQUE INDEX uk_client_master_code ON client_master (client_code);
CREATE INDEX idx_client_master_name ON client_master (client_name);
CREATE INDEX idx_client_master_status ON client_master (status_cd);

CREATE UNIQUE INDEX uk_goods_item_id ON goods_item (product_id);
CREATE INDEX idx_goods_item_name ON goods_item (product_name);
CREATE INDEX idx_goods_item_cold_flag ON goods_item (cold_flag);

CREATE UNIQUE INDEX uk_biz_order_id ON biz_order (biz_order_id);
CREATE UNIQUE INDEX uk_biz_order_sales_no ON biz_order (sales_no);
CREATE INDEX idx_biz_order_client_code ON biz_order (client_code);
CREATE INDEX idx_biz_order_status_cd ON biz_order (status_cd);
CREATE INDEX idx_biz_order_order_tm ON biz_order (order_tm);
CREATE INDEX idx_biz_order_client_status ON biz_order (client_code, status_cd);

CREATE UNIQUE INDEX uk_biz_order_line_id ON biz_order_line (line_id);
CREATE INDEX idx_biz_order_line_order_id ON biz_order_line (biz_order_id);
CREATE INDEX idx_biz_order_line_product_id ON biz_order_line (product_id);
CREATE INDEX idx_biz_order_line_order_product ON biz_order_line (biz_order_id, product_id);

CREATE UNIQUE INDEX uk_send_apply_no ON send_apply (send_apply_no);
CREATE INDEX idx_send_apply_order_id ON send_apply (biz_order_id);
CREATE INDEX idx_send_apply_status ON send_apply (apply_status);
CREATE INDEX idx_send_apply_plan_out_tm ON send_apply (plan_out_tm);
CREATE INDEX idx_send_apply_house_status ON send_apply (house_id, apply_status);

CREATE UNIQUE INDEX uk_stock_snapshot_no ON stock_snapshot (snapshot_no);
CREATE INDEX idx_stock_snapshot_house_id ON stock_snapshot (house_id);
CREATE INDEX idx_stock_snapshot_product_id ON stock_snapshot (product_id);
CREATE INDEX idx_stock_snapshot_tm ON stock_snapshot (snapshot_tm);
CREATE INDEX idx_stock_snapshot_house_product_tm ON stock_snapshot (house_id, product_id, snapshot_tm);
