INSERT INTO research_project (
  project_id, project_name, project_code, project_level,
  project_source, principal_id, received_amount, status
) VALUES
('RP2024-00125', '面向异构教育数据的语义统一方法研究', 'NSFC62400012', '国家级', '国家自然科学基金', 'T000123', 290000.00, 'active'),
('RP2025-00018', '跨校课程资源共享与语义检索研究', 'JSKJ2025-118', '省部级', '江苏省科技计划', 'T000124', 180000.00, 'active');

INSERT INTO paper_output (
  paper_id, title, journal_name, index_type,
  doi, citation_count, paper_status, status
) VALUES
('P2025-00398', 'A Semantic Unification Framework for Multi-University Data Sharing', 'Expert Systems with Applications', 'SCI', '10.1016/j.eswa.2025.125890', 12, '已见刊', 'valid'),
('P2025-00421', 'Cross-Domain Query Routing over Heterogeneous Academic Databases', 'Information Sciences', 'SCI', '10.1016/j.ins.2025.121008', 7, 'online', 'valid');

INSERT INTO patent_record (
  patent_id, patent_name, patent_type, application_no,
  transfer_amount, current_status, status
) VALUES
('PAT2023-0087', '一种跨域字段语义归并方法与系统', '发明专利', 'CN202310123456.8', 120000.00, '已授权', 'valid');