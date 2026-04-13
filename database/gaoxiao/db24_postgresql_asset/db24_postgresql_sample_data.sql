INSERT INTO fixed_asset (
  asset_id, asset_code, asset_name, asset_category, holder_dept_id,
  holder_person_id, purchase_amount, asset_status, register_date
) VALUES
  ('FA_0001', 'A20250001', '高性能服务器', 'IT设备', 'D_INFO', 'T00021', 68000.00, '在用', '2025-03-12'),
  ('FA_0002', 'A20250088', '示波器', '实验仪器', 'D_EE', 'T00356', 22000.00, '在用', '2025-05-16'),
  ('FA_0003', 'A20251566', '图书自助借还机', '图书设备', 'D_LIB', 'S00013', 45000.00, '报废审批中', '2024-11-03');

INSERT INTO asset_register (
  register_id, asset_id, finance_voucher_no, register_operator, register_time, register_status
) VALUES
  ('AR_0001', 'FA_0001', 'FV2025031201', '资产管理员A', '2025-03-12 10:00:00', '已入账'),
  ('AR_0002', 'FA_0002', 'FV2025051601', '资产管理员B', '2025-05-16 11:00:00', '已入账'),
  ('AR_0003', 'FA_0003', 'FV2024110301', '资产管理员C', '2024-11-03 09:30:00', '已入账');

INSERT INTO asset_barcode (
  barcode_id, asset_id, barcode_no, print_time, bind_status
) VALUES
  ('AB_0001', 'FA_0001', 'BC_A20250001', '2025-03-12 10:30:00', '已绑定'),
  ('AB_0002', 'FA_0002', 'BC_A20250088', '2025-05-16 11:30:00', '已绑定'),
  ('AB_0003', 'FA_0003', 'BC_A20251566', '2024-11-03 10:00:00', '已绑定');

INSERT INTO asset_transfer (
  transfer_id, asset_id, from_dept_id, to_dept_id, transfer_date, transfer_status
) VALUES
  ('AT_0001', 'FA_0001', 'D_INFO', 'D_CS', '2025-09-01', '已完成'),
  ('AT_0002', 'FA_0002', 'D_EE', 'D_EE', '2025-09-15', '无需调拨'),
  ('AT_0003', 'FA_0003', 'D_LIB', 'D_ASSET', '2026-01-10', '审批中');

INSERT INTO asset_scrap (
  scrap_id, asset_id, apply_date, scrap_reason, scrap_status, approved_date
) VALUES
  ('AS_0001', 'FA_0001', '2030-03-01', '性能落后拟更新', '未申请', NULL),
  ('AS_0002', 'FA_0002', '2032-06-01', '精度下降', '未申请', NULL),
  ('AS_0003', 'FA_0003', '2026-02-01', '核心部件故障且维修成本高', '审批中', NULL);

INSERT INTO asset_holder (
  holder_id, asset_id, holder_person_id, holder_name, holder_dept_id, bind_start_date, bind_status
) VALUES
  ('AH_0001', 'FA_0001', 'T00021', '刘强', 'D_INFO', '2025-03-12', '有效'),
  ('AH_0002', 'FA_0002', 'T00356', '陈浩', 'D_EE', '2025-05-16', '有效'),
  ('AH_0003', 'FA_0003', 'S00013', '陈倩', 'D_LIB', '2024-11-03', '待变更');
