USE gaoxiao_db14_access;

INSERT INTO access_device (
  door_id,
  building_code,
  door_name,
  online_status,
  alarm_flag
) VALUES
  ('ACC_DEV_001', 'BLD_A', '东门闸机1', 'online', 0),
  ('ACC_DEV_002', 'BLD_LIB', '图书馆北门', 'offline', 1),
  ('ACC_DEV_003', 'BLD_DORM5', '宿舍5栋入口', 'online', 0);

INSERT INTO access_person_map (
  map_id, person_id, person_name, person_type, card_no, map_status
) VALUES
  ('APM_0001', 'STU_2025_0001', '张晨', 'student', 'CARD0001', 'active'),
  ('APM_0002', 'STU_2025_0088', '李媛', 'student', 'CARD0088', 'active'),
  ('APM_0003', 'T00021', '刘强', 'teacher', 'CARDT021', 'active');

INSERT INTO access_record (
  record_id, person_id, person_name, person_type, door_id, building_code,
  access_time, access_result, direction
) VALUES
  ('AR_0001', 'STU_2025_0001', '张晨', 'student', 'ACC_DEV_003', 'BLD_DORM5', '2026-03-29 07:12:03', 'success', 'in'),
  ('AR_0002', 'STU_2025_0088', '李媛', 'student', 'ACC_DEV_003', 'BLD_DORM5', '2026-03-29 22:35:11', 'success', 'out'),
  ('AR_0003', 'T00021', '刘强', 'teacher', 'ACC_DEV_002', 'BLD_LIB', '2026-03-29 08:05:42', 'denied', 'in');

INSERT INTO building_entry_log (
  entry_id, record_id, building_code, entry_result, logged_time
) VALUES
  ('BEL_0001', 'AR_0001', 'BLD_DORM5', '允许通行', '2026-03-29 07:12:05'),
  ('BEL_0002', 'AR_0002', 'BLD_DORM5', '允许通行', '2026-03-29 22:35:13'),
  ('BEL_0003', 'AR_0003', 'BLD_LIB', '拒绝通行', '2026-03-29 08:05:45');

INSERT INTO abnormal_access_event (
  event_id, record_id, abnormal_type, event_detail, event_status, event_time
) VALUES
  ('AAE_0001', 'AR_0001', '无', '正常通行，无异常', 'closed', '2026-03-29 07:13:00'),
  ('AAE_0002', 'AR_0002', '夜间高频进出', '22点后短时重复刷卡', 'processing', '2026-03-29 22:36:00'),
  ('AAE_0003', 'AR_0003', '权限不足', '教师卡无该门权限', 'open', '2026-03-29 08:06:00');

INSERT INTO visitor_pass (
  pass_id, visitor_name, visitor_id_card, host_person_id, door_id,
  valid_from, valid_to, pass_status
) VALUES
  ('VP_0001', '周访客', '320102199901011234', 'STU_2025_0001', 'ACC_DEV_001', '2026-03-29 09:00:00', '2026-03-29 18:00:00', 'valid'),
  ('VP_0002', '孙访客', '110101199802023456', 'STU_2025_0088', 'ACC_DEV_003', '2026-03-29 13:00:00', '2026-03-29 20:00:00', 'expired'),
  ('VP_0003', '吴访客', '420106199703034567', 'T00021', 'ACC_DEV_002', '2026-03-29 10:00:00', '2026-03-29 17:00:00', 'revoked');
