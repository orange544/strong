BEGIN EXECUTE IMMEDIATE 'DROP TABLE repair_request CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE late_return_record CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dorm_transfer CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE checkin_record CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE bed_allocation CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dorm_room CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE dorm_building CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/

CREATE TABLE dorm_building (
  building_id VARCHAR2(20) PRIMARY KEY,
  building_no VARCHAR2(20) NOT NULL,
  campus_code VARCHAR2(20) NOT NULL,
  building_gender VARCHAR2(10) NOT NULL,
  manager_name VARCHAR2(50) NOT NULL,
  building_status VARCHAR2(20) NOT NULL
);

CREATE TABLE dorm_room (
  room_id VARCHAR2(20) PRIMARY KEY,
  building_id VARCHAR2(20) NOT NULL REFERENCES dorm_building(building_id),
  room_no VARCHAR2(20) NOT NULL,
  floor_no NUMBER(2) NOT NULL,
  capacity NUMBER(2) NOT NULL,
  room_status VARCHAR2(20) NOT NULL
);

CREATE TABLE bed_allocation (
  allocation_id VARCHAR2(32) PRIMARY KEY,
  resident_id VARCHAR2(32) NOT NULL,
  student_name VARCHAR2(50) NOT NULL,
  sex_code VARCHAR2(10) NOT NULL,
  age_year NUMBER(3) NOT NULL,
  college_id VARCHAR2(20) NOT NULL,
  building_no VARCHAR2(20) NOT NULL,
  room_no VARCHAR2(20) NOT NULL,
  bed_no VARCHAR2(20) NOT NULL,
  live_status VARCHAR2(20) NOT NULL,
  checkin_date DATE NOT NULL
);

CREATE TABLE checkin_record (
  checkin_id VARCHAR2(32) PRIMARY KEY,
  allocation_id VARCHAR2(32) NOT NULL REFERENCES bed_allocation(allocation_id),
  checkin_time DATE NOT NULL,
  luggage_count NUMBER(3) NOT NULL,
  operator_name VARCHAR2(50) NOT NULL,
  checkin_status VARCHAR2(20) NOT NULL
);

CREATE TABLE dorm_transfer (
  transfer_id VARCHAR2(32) PRIMARY KEY,
  allocation_id VARCHAR2(32) NOT NULL REFERENCES bed_allocation(allocation_id),
  from_room_no VARCHAR2(20) NOT NULL,
  to_room_no VARCHAR2(20) NOT NULL,
  transfer_date DATE NOT NULL,
  transfer_reason VARCHAR2(200) NOT NULL,
  transfer_status VARCHAR2(20) NOT NULL
);

CREATE TABLE late_return_record (
  late_id VARCHAR2(32) PRIMARY KEY,
  resident_id VARCHAR2(32) NOT NULL,
  room_no VARCHAR2(20) NOT NULL,
  late_time DATE NOT NULL,
  reason VARCHAR2(200) NOT NULL,
  process_status VARCHAR2(20) NOT NULL
);

CREATE TABLE repair_request (
  repair_id VARCHAR2(32) PRIMARY KEY,
  room_id VARCHAR2(20) NOT NULL REFERENCES dorm_room(room_id),
  request_desc VARCHAR2(200) NOT NULL,
  request_time DATE NOT NULL,
  repair_status VARCHAR2(20) NOT NULL,
  finish_time DATE
);

INSERT INTO dorm_building VALUES ('DBLD_05','5栋','MAIN','M','宿管张老师','启用');
INSERT INTO dorm_building VALUES ('DBLD_08','8栋','MAIN','F','宿管李老师','启用');
INSERT INTO dorm_building VALUES ('DBLD_12','12栋','EAST','U','宿管王老师','启用');

INSERT INTO dorm_room VALUES ('ROOM_0502','DBLD_05','502',5,4,'在用');
INSERT INTO dorm_room VALUES ('ROOM_8306','DBLD_08','306',3,4,'在用');
INSERT INTO dorm_room VALUES ('ROOM_12218','DBLD_12','218',2,4,'待清洁');

INSERT INTO bed_allocation VALUES ('BA_0001','STU_2025_0001','张晨','M',18,'COL_010','5栋','502','2号床','已入住',DATE '2025-09-01');
INSERT INTO bed_allocation VALUES ('BA_0002','STU_2025_0088','李媛','F',19,'COL_023','8栋','306','1号床','已入住',DATE '2025-09-01');
INSERT INTO bed_allocation VALUES ('BA_0003','STU_2025_1566','王磊','U',17,'COL_018','12栋','218','4号床','待分配',DATE '2025-09-05');

INSERT INTO checkin_record VALUES ('CR_0001','BA_0001',DATE '2025-09-01',2,'宿管张老师','完成');
INSERT INTO checkin_record VALUES ('CR_0002','BA_0002',DATE '2025-09-01',1,'宿管李老师','完成');
INSERT INTO checkin_record VALUES ('CR_0003','BA_0003',DATE '2025-09-05',3,'宿管王老师','待办理');

INSERT INTO dorm_transfer VALUES ('DT_0001','BA_0001','502','503',DATE '2025-09-20','室友冲突协调','已完成');
INSERT INTO dorm_transfer VALUES ('DT_0002','BA_0002','306','307',DATE '2025-10-08','采光调整','审批中');
INSERT INTO dorm_transfer VALUES ('DT_0003','BA_0003','218','219',DATE '2025-09-10','新生集中安排','待处理');

INSERT INTO late_return_record VALUES ('LR_0001','STU_2025_0001','502',DATE '2025-10-15','实验项目加班','已登记');
INSERT INTO late_return_record VALUES ('LR_0002','STU_2025_0088','306',DATE '2025-10-20','校外活动返校晚','已教育');
INSERT INTO late_return_record VALUES ('LR_0003','STU_2025_1566','218',DATE '2025-11-02','实习返校','待核实');

INSERT INTO repair_request VALUES ('RP_0001','ROOM_0502','空调漏水',DATE '2025-09-18','已完成',DATE '2025-09-19');
INSERT INTO repair_request VALUES ('RP_0002','ROOM_8306','照明故障',DATE '2025-10-03','处理中',NULL);
INSERT INTO repair_request VALUES ('RP_0003','ROOM_12218','门锁损坏',DATE '2025-10-11','待受理',NULL);

COMMIT;
