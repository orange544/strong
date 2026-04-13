USE gaoxiao_db25_lab;

INSERT INTO laboratory_room (
  lab_room_id, building_code, room_name, room_type, capacity, room_status
) VALUES
  ('LAB_C305', 'BLD_C', '智能计算实验室', '计算机实验室', 60, '可用'),
  ('LAB_E201', 'BLD_E', '嵌入式系统实验室', '电子实验室', 40, '可用'),
  ('LAB_E202', 'BLD_E', '信号测试实验室', '仪器实验室', 35, '维护中');

INSERT INTO equipment_info (
  equipment_id, equipment_code, equipment_name, lab_room_id, manager_id,
  manager_name, equipment_status, purchase_date, asset_ref_code
) VALUES
  ('EQ_0001', 'EQC20250001', 'GPU训练服务器', 'LAB_C305', 'T00021', '刘强', '可预约', '2025-03-12', 'A20250001'),
  ('EQ_0002', 'EQC20250088', 'FPGA开发板', 'LAB_E201', 'T00356', '陈浩', '维修中', '2024-10-03', 'A20250088'),
  ('EQ_0003', 'EQC20251566', '波形信号分析仪', 'LAB_E202', 'T00108', '周敏', '停用', '2023-09-18', 'A20251566');

INSERT INTO equipment_reservation (
  reservation_id, equipment_id, user_id, user_name, reserve_start, reserve_end, reservation_status
) VALUES
  ('ER_0001', 'EQ_0001', 'STU_2025_0001', '张晨', '2026-04-02 09:00:00', '2026-04-02 12:00:00', '已确认'),
  ('ER_0002', 'EQ_0002', 'STU_2025_0088', '李媛', '2026-04-03 14:00:00', '2026-04-03 16:00:00', '已拒绝'),
  ('ER_0003', 'EQ_0003', 'STU_2025_1566', '王磊', '2026-04-04 10:00:00', '2026-04-04 11:00:00', '待审核');

INSERT INTO maintenance_record (
  maintenance_id, equipment_id, issue_desc, maintainer, maintenance_time, maintenance_status
) VALUES
  ('MR_0001', 'EQ_0001', '风扇噪声偏高，完成清灰', '工程师A', '2026-03-20 10:00:00', '已完成'),
  ('MR_0002', 'EQ_0002', '电源模块故障待更换', '工程师B', '2026-03-21 11:00:00', '处理中'),
  ('MR_0003', 'EQ_0003', '探头老化，需要停机维护', '工程师C', '2026-03-22 09:30:00', '待处理');

INSERT INTO calibration_record (
  calibration_id, equipment_id, calibration_date, calibration_result, calibrator, next_calibration_date
) VALUES
  ('CR_0001', 'EQ_0001', '2026-03-01', '通过', '计量员A', '2027-03-01'),
  ('CR_0002', 'EQ_0002', '2026-03-05', '不通过', '计量员B', '2026-06-05'),
  ('CR_0003', 'EQ_0003', '2026-03-10', '待复检', '计量员C', '2026-05-10');

INSERT INTO safety_inspection (
  inspection_id, lab_room_id, inspector, inspection_time, safety_score, inspection_status
) VALUES
  ('SI_0001', 'LAB_C305', '安全员A', '2026-03-25 15:00:00', 95.00, '合格'),
  ('SI_0002', 'LAB_E201', '安全员B', '2026-03-25 15:30:00', 88.00, '整改中'),
  ('SI_0003', 'LAB_E202', '安全员C', '2026-03-25 16:00:00', 76.00, '不合格');
