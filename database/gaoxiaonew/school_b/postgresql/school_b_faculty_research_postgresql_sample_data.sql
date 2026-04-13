INSERT INTO faculty_member (
  faculty_no, employee_code, faculty_name, "rank", job_type,
  graduated_school, research_direction, job_status, status
) VALUES
('F000221', '20120511', '周宁', '副教授', '专任教师', '武汉大学', '知识图谱与信息检索', '在岗', 'active'),
('F000222', '20161203', '李倩', '教授', '专任教师', '郑州大学', '教育大数据分析', '在岗', 'active'),
('F000223', '20190821', '韩旭', '讲师', '教学科研岗', '西安交通大学', '语义计算', '在岗', 'active'),
('F000224', '20110319', '高峰', '教授', '专任教师', '华中科技大学', '智能制造系统', '在岗', 'active');

INSERT INTO faculty_output_metric (
  faculty_no, publication_total, patent_total, fund_income_3y, status
) VALUES
('F000221', 24, 5, 960000.00, 'valid'),
('F000222', 31, 7, 1350000.00, 'valid');

INSERT INTO faculty_rank_record (
  record_id, faculty_no, rank_name, review_result, status
) VALUES
('FR2020001', 'F000221', '副教授', '通过', 'archived');