USE gaoxiao_db15_library;

INSERT INTO reader_master (
  reader_id, reader_code, reader_name, sex, age, cert_no,
  mobile_no, reader_type, dept_id, reader_status
) VALUES
  ('R_0001', 'RD20250001', '张晨', '男', 18, '320583200701015612', '13812345678', '本科生', 'D010', '正常'),
  ('R_0002', 'RD20250088', '李媛', '女', 19, '370102200609086527', '13988886666', '本科生', 'D023', '欠费停借'),
  ('R_0003', 'RD_T00021', '刘强', '男', 42, '320102198304156519', '13811112222', '教师', 'D010', '正常');

INSERT INTO book_info (
  book_id, isbn, book_title, author_name, publisher, pub_year, category_code, status
) VALUES
  ('BOOK_0001', '9787302523123', '数据库系统概论', '王珊', '清华大学出版社', 2023, 'TP311', '在架'),
  ('BOOK_0002', '9787111712046', '深入理解计算机系统', 'Randal E. Bryant', '机械工业出版社', 2022, 'TP301', '在架'),
  ('BOOK_0003', '9787121449871', '机器学习实战', '周志华', '电子工业出版社', 2021, 'TP181', '维护中');

INSERT INTO book_copy (
  copy_id, book_id, barcode, shelf_location, copy_status
) VALUES
  ('CP_0001', 'BOOK_0001', 'BC00000001', 'A-01-03', '可借'),
  ('CP_0002', 'BOOK_0002', 'BC00000002', 'B-02-08', '借出'),
  ('CP_0003', 'BOOK_0003', 'BC00000003', 'C-03-02', '禁借');

INSERT INTO borrow_record (
  borrow_id, reader_id, copy_id, borrow_time, due_date, borrow_status
) VALUES
  ('BR_0001', 'R_0001', 'CP_0001', '2026-03-20 09:30:00', '2026-04-20', '借阅中'),
  ('BR_0002', 'R_0002', 'CP_0002', '2026-03-10 14:00:00', '2026-04-10', '超期'),
  ('BR_0003', 'R_0003', 'CP_0003', '2026-03-01 10:20:00', '2026-04-01', '已归还');

INSERT INTO return_record (
  return_id, borrow_id, return_time, return_condition, operator_name
) VALUES
  ('RR_0001', 'BR_0001', '2026-03-25 11:20:00', '完好', '馆员A'),
  ('RR_0002', 'BR_0002', '2026-04-15 16:05:00', '轻微磨损', '馆员B'),
  ('RR_0003', 'BR_0003', '2026-03-18 13:00:00', '完好', '馆员C');

INSERT INTO reservation_record (
  reserve_id, reader_id, book_id, reserve_time, reserve_status
) VALUES
  ('RSV_0001', 'R_0001', 'BOOK_0002', '2026-03-22 10:00:00', '已预约'),
  ('RSV_0002', 'R_0002', 'BOOK_0001', '2026-03-22 10:05:00', '已取消'),
  ('RSV_0003', 'R_0003', 'BOOK_0003', '2026-03-22 10:10:00', '待取书');

INSERT INTO overdue_record (
  overdue_id, borrow_id, overdue_days, fine_amount, process_status
) VALUES
  ('OD_0001', 'BR_0001', 0, 0.00, '无'),
  ('OD_0002', 'BR_0002', 5, 2.50, '待缴费'),
  ('OD_0003', 'BR_0003', 2, 1.00, '已处理');

INSERT INTO fine_payment (
  payment_id, overdue_id, reader_id, pay_amount, pay_time, payment_status, voucher_no
) VALUES
  ('FP_0001', 'OD_0001', 'R_0001', 0.00, '2026-03-25 11:25:00', '免缴', 'VLIB0001'),
  ('FP_0002', 'OD_0002', 'R_0002', 2.50, '2026-04-16 09:30:00', '已支付', 'VLIB0002'),
  ('FP_0003', 'OD_0003', 'R_0003', 1.00, '2026-03-19 08:45:00', '已支付', 'VLIB0003');
