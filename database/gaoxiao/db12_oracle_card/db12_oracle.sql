BEGIN EXECUTE IMMEDIATE 'DROP TABLE card_loss_reissue CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE utility_payment CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE card_transaction CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE card_recharge CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE card_holder_map CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE campus_card_account CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

CREATE TABLE campus_card_account (
  card_id VARCHAR2(32) PRIMARY KEY,
  account_no VARCHAR2(32) UNIQUE NOT NULL,
  holder_name VARCHAR2(50) NOT NULL,
  holder_type VARCHAR2(20) NOT NULL,
  holder_id VARCHAR2(32) NOT NULL,
  mobile VARCHAR2(20) NOT NULL,
  balance_amount NUMBER(10,2) NOT NULL,
  account_status VARCHAR2(20) NOT NULL,
  open_date DATE NOT NULL
);

CREATE TABLE card_holder_map (
  map_id VARCHAR2(32) PRIMARY KEY,
  card_id VARCHAR2(32) NOT NULL REFERENCES campus_card_account(card_id),
  holder_id VARCHAR2(32) NOT NULL,
  holder_name VARCHAR2(50) NOT NULL,
  holder_type VARCHAR2(20) NOT NULL,
  map_status VARCHAR2(20) NOT NULL
);

CREATE TABLE card_recharge (
  recharge_id VARCHAR2(32) PRIMARY KEY,
  card_id VARCHAR2(32) NOT NULL REFERENCES campus_card_account(card_id),
  recharge_amount NUMBER(10,2) NOT NULL,
  recharge_time DATE NOT NULL,
  pay_channel VARCHAR2(30) NOT NULL,
  recharge_status VARCHAR2(20) NOT NULL
);

CREATE TABLE card_transaction (
  txn_id VARCHAR2(32) PRIMARY KEY,
  card_id VARCHAR2(32) NOT NULL REFERENCES campus_card_account(card_id),
  merchant_name VARCHAR2(100) NOT NULL,
  txn_amount NUMBER(10,2) NOT NULL,
  txn_time DATE NOT NULL,
  txn_type VARCHAR2(20) NOT NULL,
  txn_status VARCHAR2(20) NOT NULL
);

CREATE TABLE utility_payment (
  payment_id VARCHAR2(32) PRIMARY KEY,
  card_id VARCHAR2(32) NOT NULL REFERENCES campus_card_account(card_id),
  utility_type VARCHAR2(20) NOT NULL,
  amount NUMBER(10,2) NOT NULL,
  payment_time DATE NOT NULL,
  payment_status VARCHAR2(20) NOT NULL
);

CREATE TABLE card_loss_reissue (
  process_id VARCHAR2(32) PRIMARY KEY,
  card_id VARCHAR2(32) NOT NULL REFERENCES campus_card_account(card_id),
  process_type VARCHAR2(20) NOT NULL,
  process_time DATE NOT NULL,
  operator_name VARCHAR2(50) NOT NULL,
  process_status VARCHAR2(20) NOT NULL
);

INSERT INTO campus_card_account VALUES ('CARD_0001','AC20250001','张晨','student','STU_2025_0001','13812345678',126.50,'正常',DATE '2025-09-01');
INSERT INTO campus_card_account VALUES ('CARD_0002','AC20250088','李媛','student','STU_2025_0088','13988886666',68.00,'冻结',DATE '2025-09-01');
INSERT INTO campus_card_account VALUES ('CARD_0003','AC_T00021','刘强','teacher','T00021','13811112222',560.20,'正常',DATE '2020-09-01');

INSERT INTO card_holder_map VALUES ('CHM_0001','CARD_0001','STU_2025_0001','张晨','student','有效');
INSERT INTO card_holder_map VALUES ('CHM_0002','CARD_0002','STU_2025_0088','李媛','student','有效');
INSERT INTO card_holder_map VALUES ('CHM_0003','CARD_0003','T00021','刘强','teacher','有效');

INSERT INTO card_recharge VALUES ('CRG_0001','CARD_0001',200.00,DATE '2025-09-05','支付宝','成功');
INSERT INTO card_recharge VALUES ('CRG_0002','CARD_0002',100.00,DATE '2025-09-06','微信','成功');
INSERT INTO card_recharge VALUES ('CRG_0003','CARD_0003',500.00,DATE '2025-09-03','银行卡','成功');

INSERT INTO card_transaction VALUES ('CTX_0001','CARD_0001','第一食堂',18.50,DATE '2025-09-06','消费','成功');
INSERT INTO card_transaction VALUES ('CTX_0002','CARD_0002','校园超市',35.00,DATE '2025-09-07','消费','成功');
INSERT INTO card_transaction VALUES ('CTX_0003','CARD_0003','教工餐厅',22.80,DATE '2025-09-06','消费','成功');

INSERT INTO utility_payment VALUES ('UP_0001','CARD_0001','电费',25.00,DATE '2025-09-10','成功');
INSERT INTO utility_payment VALUES ('UP_0002','CARD_0002','水费',12.00,DATE '2025-09-11','成功');
INSERT INTO utility_payment VALUES ('UP_0003','CARD_0003','电费',30.00,DATE '2025-09-12','成功');

INSERT INTO card_loss_reissue VALUES ('CLR_0001','CARD_0001','挂失',DATE '2025-10-08','卡务员A','已处理');
INSERT INTO card_loss_reissue VALUES ('CLR_0002','CARD_0002','补卡',DATE '2025-10-09','卡务员B','处理中');
INSERT INTO card_loss_reissue VALUES ('CLR_0003','CARD_0003','解挂',DATE '2025-10-12','卡务员C','已处理');

COMMIT;
