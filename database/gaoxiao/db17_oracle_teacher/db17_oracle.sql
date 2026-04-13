BEGIN EXECUTE IMMEDIATE 'DROP TABLE teacher_employment CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE teacher_department CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE teacher_degree CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE teacher_title CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE teacher_entry CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE teacher_basic CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

CREATE TABLE teacher_basic (
  teacher_id VARCHAR2(20) PRIMARY KEY,
  teacher_no VARCHAR2(20) UNIQUE NOT NULL,
  teacher_name VARCHAR2(50) NOT NULL,
  gender VARCHAR2(10) NOT NULL,
  age NUMBER(3) NOT NULL,
  id_card_no VARCHAR2(18) UNIQUE NOT NULL,
  mobile VARCHAR2(20) NOT NULL,
  college_id VARCHAR2(20) NOT NULL,
  title_name VARCHAR2(30) NOT NULL,
  employment_status VARCHAR2(20) NOT NULL
);

CREATE TABLE teacher_entry (
  entry_id VARCHAR2(32) PRIMARY KEY,
  teacher_id VARCHAR2(20) NOT NULL REFERENCES teacher_basic(teacher_id),
  entry_date DATE NOT NULL,
  source_channel VARCHAR2(50) NOT NULL,
  probation_end_date DATE NOT NULL,
  entry_status VARCHAR2(20) NOT NULL
);

CREATE TABLE teacher_title (
  title_id VARCHAR2(32) PRIMARY KEY,
  teacher_id VARCHAR2(20) NOT NULL REFERENCES teacher_basic(teacher_id),
  title_name VARCHAR2(30) NOT NULL,
  evaluate_date DATE NOT NULL,
  valid_status VARCHAR2(20) NOT NULL
);

CREATE TABLE teacher_degree (
  degree_id VARCHAR2(32) PRIMARY KEY,
  teacher_id VARCHAR2(20) NOT NULL REFERENCES teacher_basic(teacher_id),
  highest_degree VARCHAR2(30) NOT NULL,
  graduate_school VARCHAR2(120) NOT NULL,
  graduation_date DATE NOT NULL,
  degree_status VARCHAR2(20) NOT NULL
);

CREATE TABLE teacher_department (
  dept_rel_id VARCHAR2(32) PRIMARY KEY,
  teacher_id VARCHAR2(20) NOT NULL REFERENCES teacher_basic(teacher_id),
  college_id VARCHAR2(20) NOT NULL,
  department_name VARCHAR2(80) NOT NULL,
  start_date DATE NOT NULL,
  rel_status VARCHAR2(20) NOT NULL
);

CREATE TABLE teacher_employment (
  employment_id VARCHAR2(32) PRIMARY KEY,
  teacher_id VARCHAR2(20) NOT NULL REFERENCES teacher_basic(teacher_id),
  employment_type VARCHAR2(30) NOT NULL,
  contract_start DATE NOT NULL,
  contract_end DATE,
  employment_status VARCHAR2(20) NOT NULL
);

INSERT INTO teacher_basic VALUES ('T00021','JG200021','刘强','男',42,'320102198304156519','13811112222','COL_010','教授','在职');
INSERT INTO teacher_basic VALUES ('T00108','JG200108','周敏','女',38,'110108198711236528','13922223333','COL_023','副教授','在职');
INSERT INTO teacher_basic VALUES ('T00356','JG200356','陈浩','男',47,'420106197802144517','13733334444','COL_018','讲师','挂职');

INSERT INTO teacher_entry VALUES ('TE_0001','T00021',DATE '2012-09-01','博士引进',DATE '2013-08-31','已转正');
INSERT INTO teacher_entry VALUES ('TE_0002','T00108',DATE '2016-07-01','公开招聘',DATE '2017-06-30','已转正');
INSERT INTO teacher_entry VALUES ('TE_0003','T00356',DATE '2024-02-01','调入',DATE '2025-01-31','挂职');

INSERT INTO teacher_title VALUES ('TT_0001','T00021','教授',DATE '2021-12-01','有效');
INSERT INTO teacher_title VALUES ('TT_0002','T00108','副教授',DATE '2020-12-01','有效');
INSERT INTO teacher_title VALUES ('TT_0003','T00356','讲师',DATE '2018-12-01','有效');

INSERT INTO teacher_degree VALUES ('TD_0001','T00021','博士','清华大学',DATE '2011-06-30','已核验');
INSERT INTO teacher_degree VALUES ('TD_0002','T00108','博士','北京大学',DATE '2014-06-30','已核验');
INSERT INTO teacher_degree VALUES ('TD_0003','T00356','硕士','华中科技大学',DATE '2006-06-30','已核验');

INSERT INTO teacher_department VALUES ('TDR_0001','T00021','COL_010','计算机科学系',DATE '2012-09-01','有效');
INSERT INTO teacher_department VALUES ('TDR_0002','T00108','COL_023','数学系',DATE '2016-07-01','有效');
INSERT INTO teacher_department VALUES ('TDR_0003','T00356','COL_018','电气工程系',DATE '2024-02-01','有效');

INSERT INTO teacher_employment VALUES ('TEMP_0001','T00021','事业编',DATE '2012-09-01',NULL,'在职');
INSERT INTO teacher_employment VALUES ('TEMP_0002','T00108','事业编',DATE '2016-07-01',NULL,'在职');
INSERT INTO teacher_employment VALUES ('TEMP_0003','T00356','挂职交流',DATE '2024-02-01',DATE '2026-01-31','挂职');

COMMIT;
