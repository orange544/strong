SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS asset_holder;
DROP TABLE IF EXISTS asset_scrap;
DROP TABLE IF EXISTS asset_transfer;
DROP TABLE IF EXISTS asset_barcode;
DROP TABLE IF EXISTS asset_register;
DROP TABLE IF EXISTS fixed_asset;

CREATE TABLE fixed_asset (
  asset_id VARCHAR(32) PRIMARY KEY,
  asset_code VARCHAR(32) NOT NULL UNIQUE,
  asset_name VARCHAR(120) NOT NULL,
  asset_category VARCHAR(40) NOT NULL,
  holder_dept_id VARCHAR(20) NOT NULL,
  holder_person_id VARCHAR(32) NOT NULL,
  purchase_amount NUMERIC(14,2) NOT NULL,
  asset_status VARCHAR(20) NOT NULL,
  register_date DATE NOT NULL
);

CREATE TABLE asset_register (
  register_id VARCHAR(32) PRIMARY KEY,
  asset_id VARCHAR(32) NOT NULL REFERENCES fixed_asset(asset_id) ON DELETE CASCADE,
  finance_voucher_no VARCHAR(40) NOT NULL,
  register_operator VARCHAR(50) NOT NULL,
  register_time TIMESTAMP NOT NULL,
  register_status VARCHAR(20) NOT NULL
);

CREATE TABLE asset_barcode (
  barcode_id VARCHAR(32) PRIMARY KEY,
  asset_id VARCHAR(32) NOT NULL REFERENCES fixed_asset(asset_id) ON DELETE CASCADE,
  barcode_no VARCHAR(50) NOT NULL UNIQUE,
  print_time TIMESTAMP NOT NULL,
  bind_status VARCHAR(20) NOT NULL
);

CREATE TABLE asset_transfer (
  transfer_id VARCHAR(32) PRIMARY KEY,
  asset_id VARCHAR(32) NOT NULL REFERENCES fixed_asset(asset_id) ON DELETE CASCADE,
  from_dept_id VARCHAR(20) NOT NULL,
  to_dept_id VARCHAR(20) NOT NULL,
  transfer_date DATE NOT NULL,
  transfer_status VARCHAR(20) NOT NULL
);

CREATE TABLE asset_scrap (
  scrap_id VARCHAR(32) PRIMARY KEY,
  asset_id VARCHAR(32) NOT NULL REFERENCES fixed_asset(asset_id) ON DELETE CASCADE,
  apply_date DATE NOT NULL,
  scrap_reason VARCHAR(200) NOT NULL,
  scrap_status VARCHAR(20) NOT NULL,
  approved_date DATE
);

CREATE TABLE asset_holder (
  holder_id VARCHAR(32) PRIMARY KEY,
  asset_id VARCHAR(32) NOT NULL REFERENCES fixed_asset(asset_id) ON DELETE CASCADE,
  holder_person_id VARCHAR(32) NOT NULL,
  holder_name VARCHAR(50) NOT NULL,
  holder_dept_id VARCHAR(20) NOT NULL,
  bind_start_date DATE NOT NULL,
  bind_status VARCHAR(20) NOT NULL
);
