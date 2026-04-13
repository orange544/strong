INSERT INTO grant_award (
  grant_id, grant_name, grant_level, funding_source, leader_id, arrival_fee, status
) VALUES
('GA2024-016', '多高校数据共享语义映射研究', '省部级', '河南省科技厅', 'F000221', 180000.00, 'active'),
('GA2025-007', '跨域教育资源知识组织与检索方法研究', '厅局级', '河南省教育厅', 'F000222', 95000.00, 'active');

INSERT INTO publication_record (
  publication_id, paper_title, journal_title, include_type, cited_times, publish_status, status
) VALUES
('PUB2025-88', 'Semantic Alignment for Inter-University Data Sharing', 'Information Processing & Management', 'SCI', 9, 'online', 'valid'),
('PUB2025-89', 'Schema Heterogeneity Resolution in Academic Information Systems', 'Knowledge-Based Systems', 'SCI', 6, 'published', 'valid');

INSERT INTO ip_asset (
  ip_id, ip_name, ip_type, conversion_income, legal_status, status
) VALUES
('IP2023-15', '一种字段语义归并方法', '发明专利', 60000.00, '已授权', 'valid');