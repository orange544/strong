# 高校行业数据库部署工程（Batch 1-7）

本目录基于 `高校行业完整数据库设计_完整正式稿.md` 落地，当前完成七批数据库实例部署脚本，满足“建表/建集合/键空间 + 样本值 + 存在性校验”。

## Batch 1（4库）

- `db1_mysql_admission`：DB1 招生录取数据库（MySQL）
- `db2_mongodb_material`：DB2 新生档案材料库（MongoDB）
- `db4_postgresql_schedule`：DB4 课程排课数据库（PostgreSQL）
- `db5_mysql_selection`：DB5 选课与成绩数据库（MySQL）

## Batch 2（4库）

- `db6_postgresql_graduate`：DB6 研究生培养数据库（PostgreSQL）
- `db7_mysql_exam`：DB7 考试事务数据库（MySQL）
- `db9_postgresql_student_affairs`：DB9 学生事务数据库（PostgreSQL）
- `db10_mysql_aid`：DB10 奖助勤贷数据库（MySQL）

## Batch 3（4库）

- `db14_mysql_access`：DB14 门禁通行记录数据库（MySQL）
- `db15_mysql_library`：DB15 图书借阅数据库（MySQL）
- `db16_mongodb_eresource`：DB16 电子资源访问库（MongoDB）
- `db18_mysql_staff`：DB18 职工劳动关系数据库（MySQL）

## Batch 4（4库）

- `db19_mysql_performance`：DB19 教师绩效考核数据库（MySQL）
- `db20_postgresql_teacher_dev`：DB20 教师培训发展数据库（PostgreSQL）
- `db21_postgresql_research`：DB21 科研项目数据库（PostgreSQL）
- `db22_mongodb_achievement`：DB22 科研成果与附件库（MongoDB）

## Batch 5（3库）

- `db13_redis_realtime`：DB13 门禁实时状态库（Redis）
- `db24_postgresql_asset`：DB24 固定资产数据库（PostgreSQL）
- `db25_mysql_lab`：DB25 实验室设备数据库（MySQL）

## Batch 6（4库）

- `db3_oracle_student`：DB3 本科学籍与培养数据库（Oracle）
- `db11_oracle_dorm`：DB11 宿舍管理数据库（Oracle）
- `db12_oracle_card`：DB12 校园卡消费数据库（Oracle）
- `db17_oracle_teacher`：DB17 教师人事主数据库（Oracle）

## Batch 7（3库）

- `db8_elasticsearch_question`：DB8 题库与测评文本库（Elasticsearch）
- `db23_oracle_finance`：DB23 财务预算与报销数据库（Oracle）
- `db26_neo4j_semantic`：DB26 语义治理与共享图谱库（Neo4j）

## 执行

```powershell
cd "D:\Program Files\BISHE\program\database\gaoxiao"
.\setup_gaoxiao_batch1.ps1
.\setup_gaoxiao_batch2.ps1
.\setup_gaoxiao_batch3.ps1
.\setup_gaoxiao_batch4.ps1
.\setup_gaoxiao_batch5.ps1
.\setup_gaoxiao_batch6.ps1
.\setup_gaoxiao_batch7.ps1
```

## 说明

- Oracle 已验证连接：`system/Oracle123!@127.0.0.1:1521/FREEPDB1`
- Batch7 默认 Elasticsearch 端点：`http://127.0.0.1:9200`
- Batch7 默认 Neo4j 连接：`bolt://127.0.0.1:17687`（`neo4j/12345678`）
- 若本机已有 Neo4j 占用 `7687` 且密码未知，建议使用 Batch7 默认端口的独立实例。