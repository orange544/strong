// Neo4j sample graph for DB26 semantic governance.

MATCH (n) DETACH DELETE n;

UNWIND [
  {system_id:'SYS_ACAD', system_name:'教务处', domain:'教学管理'},
  {system_id:'SYS_EXAM', system_name:'考试中心', domain:'考试测评'},
  {system_id:'SYS_STU', system_name:'学生工作处', domain:'学生事务'},
  {system_id:'SYS_DORM', system_name:'后勤与宿舍管理中心', domain:'后勤宿舍'},
  {system_id:'SYS_CARD', system_name:'校园卡中心', domain:'校园支付'},
  {system_id:'SYS_LIB', system_name:'图书馆', domain:'资源借阅'},
  {system_id:'SYS_HR', system_name:'人事处', domain:'教师人事'},
  {system_id:'SYS_RES', system_name:'科研管理处', domain:'科研管理'},
  {system_id:'SYS_FIN', system_name:'财务处', domain:'财务支付'},
  {system_id:'SYS_IT', system_name:'信息化中心', domain:'语义治理'}
] AS row
MERGE (n:System {system_id: row.system_id})
SET n.system_name = row.system_name,
    n.domain = row.domain;

UNWIND [
  {db_id:'DB3', db_name:'db3_oracle_student', db_type:'Oracle'},
  {db_id:'DB8', db_name:'db8_elasticsearch_question', db_type:'Elasticsearch'},
  {db_id:'DB9', db_name:'db9_postgresql_student_affairs', db_type:'PostgreSQL'},
  {db_id:'DB11', db_name:'db11_oracle_dorm', db_type:'Oracle'},
  {db_id:'DB12', db_name:'db12_oracle_card', db_type:'Oracle'},
  {db_id:'DB15', db_name:'db15_mysql_library', db_type:'MySQL'},
  {db_id:'DB17', db_name:'db17_oracle_teacher', db_type:'Oracle'},
  {db_id:'DB21', db_name:'db21_postgresql_research', db_type:'PostgreSQL'},
  {db_id:'DB23', db_name:'db23_oracle_finance', db_type:'Oracle'},
  {db_id:'DB26', db_name:'db26_neo4j_semantic', db_type:'Neo4j'}
] AS row
MERGE (n:Database {db_id: row.db_id})
SET n.db_name = row.db_name,
    n.db_type = row.db_type;

UNWIND [
  {table_id:'TB_0001', table_name:'student_basic'},
  {table_id:'TB_0002', table_name:'question_bank_index'},
  {table_id:'TB_0003', table_name:'student_profile'},
  {table_id:'TB_0004', table_name:'bed_allocation'},
  {table_id:'TB_0005', table_name:'campus_card_account'},
  {table_id:'TB_0006', table_name:'reader_master'},
  {table_id:'TB_0007', table_name:'teacher_basic'},
  {table_id:'TB_0008', table_name:'research_project'},
  {table_id:'TB_0009', table_name:'payment_record'},
  {table_id:'TB_0010', table_name:'semantic_field_dict'}
] AS row
MERGE (n:Table {table_id: row.table_id})
SET n.table_name = row.table_name;

UNWIND [
  {
    field_id:'FIELD_0001',
    field_name:'student_id',
    source_db:'db3_oracle',
    source_table:'student_basic',
    source_subject:'教务处',
    sample_values:['STU_2025_0001','STU_2025_0088'],
    concept_name:'学生统一标识',
    conflict_type:'同义异名'
  },
  {
    field_id:'FIELD_0002',
    field_name:'stu_id',
    source_db:'db9_pg',
    source_table:'student_profile',
    source_subject:'学生工作处',
    sample_values:['STU_2025_0001','STU_2025_0088'],
    concept_name:'学生统一标识',
    conflict_type:'同义异名'
  },
  {
    field_id:'FIELD_0003',
    field_name:'reader_id',
    source_db:'db15_mysql',
    source_table:'reader_master',
    source_subject:'图书馆',
    sample_values:['R_0001','R_0002'],
    concept_name:'读者身份映射',
    conflict_type:'角色映射冲突'
  },
  {
    field_id:'FIELD_0004',
    field_name:'teacher_id',
    source_db:'db17_oracle',
    source_table:'teacher_basic',
    source_subject:'人事处',
    sample_values:['T00021','T00108'],
    concept_name:'教师统一标识',
    conflict_type:'同义异名'
  },
  {
    field_id:'FIELD_0005',
    field_name:'leader_id',
    source_db:'db21_pg',
    source_table:'research_project',
    source_subject:'科研管理处',
    sample_values:['T00021','T00108'],
    concept_name:'教师统一标识',
    conflict_type:'同义异名'
  },
  {
    field_id:'FIELD_0006',
    field_name:'status',
    source_db:'db8_es',
    source_table:'question_bank_index',
    source_subject:'考试中心',
    sample_values:['启用','停用'],
    concept_name:'题目可用状态',
    conflict_type:'同名异义'
  },
  {
    field_id:'FIELD_0007',
    field_name:'payment_status',
    source_db:'db23_oracle',
    source_table:'payment_record',
    source_subject:'财务处',
    sample_values:['已支付','审核中'],
    concept_name:'财务支付状态',
    conflict_type:'同名异义'
  },
  {
    field_id:'FIELD_0008',
    field_name:'resident_id',
    source_db:'db11_oracle',
    source_table:'bed_allocation',
    source_subject:'后勤与宿舍管理中心',
    sample_values:['STU_2025_0001','STU_2025_0088'],
    concept_name:'住宿身份标识',
    conflict_type:'同义异名'
  },
  {
    field_id:'FIELD_0009',
    field_name:'holder_id',
    source_db:'db12_oracle',
    source_table:'campus_card_account',
    source_subject:'校园卡中心',
    sample_values:['STU_2025_0001','STU_2025_0088'],
    concept_name:'校园卡持有人标识',
    conflict_type:'同义异名'
  }
] AS row
MERGE (n:Field {field_id: row.field_id})
SET n.field_name = row.field_name,
    n.source_db = row.source_db,
    n.source_table = row.source_table,
    n.source_subject = row.source_subject,
    n.sample_values = row.sample_values,
    n.concept_name = row.concept_name,
    n.conflict_type = row.conflict_type;

UNWIND [
  {concept_id:'BC_0001', concept_name:'学生统一标识', concept_type:'Identity'},
  {concept_id:'BC_0002', concept_name:'教师统一标识', concept_type:'Identity'},
  {concept_id:'BC_0003', concept_name:'读者身份映射', concept_type:'IdentityMap'},
  {concept_id:'BC_0004', concept_name:'题目可用状态', concept_type:'Status'},
  {concept_id:'BC_0005', concept_name:'财务支付状态', concept_type:'Status'},
  {concept_id:'BC_0006', concept_name:'住宿身份标识', concept_type:'Identity'},
  {concept_id:'BC_0007', concept_name:'校园卡持有人标识', concept_type:'Identity'}
] AS row
MERGE (n:BusinessConcept {concept_id: row.concept_id})
SET n.concept_name = row.concept_name,
    n.concept_type = row.concept_type;

UNWIND [
  {student_id:'STU_2025_0001', student_name:'张晨', grade_year:2025},
  {student_id:'STU_2025_0088', student_name:'李媛', grade_year:2025},
  {student_id:'STU_2025_1566', student_name:'王磊', grade_year:2025}
] AS row
MERGE (n:Student {student_id: row.student_id})
SET n.student_name = row.student_name,
    n.grade_year = row.grade_year;

UNWIND [
  {teacher_id:'T00021', teacher_name:'刘强', college_id:'COL_010'},
  {teacher_id:'T00108', teacher_name:'周敏', college_id:'COL_023'},
  {teacher_id:'T00356', teacher_name:'陈浩', college_id:'COL_018'}
] AS row
MERGE (n:Teacher {teacher_id: row.teacher_id})
SET n.teacher_name = row.teacher_name,
    n.college_id = row.college_id;

UNWIND [
  {staff_id:'S00011', staff_name:'赵凯', dept_name:'学生工作处'}
] AS row
MERGE (n:Staff {staff_id: row.staff_id})
SET n.staff_name = row.staff_name,
    n.dept_name = row.dept_name;

UNWIND [
  {project_id:'RP_0001', project_name:'面向多数据域的语义统一机制研究', leader_id:'T00021'},
  {project_id:'RP_0002', project_name:'面向高校数据共享的联邦知识图谱构建', leader_id:'T00108'}
] AS row
MERGE (n:Project {project_id: row.project_id})
SET n.project_name = row.project_name,
    n.leader_id = row.leader_id;

UNWIND [
  {asset_id:'FA_0001', asset_name:'高性能服务器', asset_status:'在用'},
  {asset_id:'FA_0002', asset_name:'示波器', asset_status:'在用'}
] AS row
MERGE (n:Asset {asset_id: row.asset_id})
SET n.asset_name = row.asset_name,
    n.asset_status = row.asset_status;

UNWIND [
  {room_id:'ROOM_0502', building_no:'5栋', room_no:'502'},
  {room_id:'ROOM_8306', building_no:'8栋', room_no:'306'}
] AS row
MERGE (n:DormRoom {room_id: row.room_id})
SET n.building_no = row.building_no,
    n.room_no = row.room_no;

UNWIND [
  {account_id:'CARD_0001', account_type:'校园卡', holder_id:'STU_2025_0001'},
  {account_id:'CARD_0002', account_type:'校园卡', holder_id:'STU_2025_0088'},
  {account_id:'CARD_0003', account_type:'校园卡', holder_id:'T00021'}
] AS row
MERGE (n:Account {account_id: row.account_id})
SET n.account_type = row.account_type,
    n.holder_id = row.holder_id;

UNWIND [
  {course_code:'CS101', course_name:'程序设计基础'},
  {course_code:'MATH204', course_name:'高等代数'},
  {course_code:'EE310', course_name:'数字电路'}
] AS row
MERGE (n:Course {course_code: row.course_code})
SET n.course_name = row.course_name;

UNWIND [
  {db_id:'DB3', system_id:'SYS_ACAD'},
  {db_id:'DB8', system_id:'SYS_EXAM'},
  {db_id:'DB9', system_id:'SYS_STU'},
  {db_id:'DB11', system_id:'SYS_DORM'},
  {db_id:'DB12', system_id:'SYS_CARD'},
  {db_id:'DB15', system_id:'SYS_LIB'},
  {db_id:'DB17', system_id:'SYS_HR'},
  {db_id:'DB21', system_id:'SYS_RES'},
  {db_id:'DB23', system_id:'SYS_FIN'},
  {db_id:'DB26', system_id:'SYS_IT'}
] AS row
MATCH (d:Database {db_id: row.db_id})
MATCH (s:System {system_id: row.system_id})
MERGE (d)-[:BELONGS_TO]->(s);

UNWIND [
  {table_id:'TB_0001', db_id:'DB3'},
  {table_id:'TB_0002', db_id:'DB8'},
  {table_id:'TB_0003', db_id:'DB9'},
  {table_id:'TB_0004', db_id:'DB11'},
  {table_id:'TB_0005', db_id:'DB12'},
  {table_id:'TB_0006', db_id:'DB15'},
  {table_id:'TB_0007', db_id:'DB17'},
  {table_id:'TB_0008', db_id:'DB21'},
  {table_id:'TB_0009', db_id:'DB23'},
  {table_id:'TB_0010', db_id:'DB26'}
] AS row
MATCH (t:Table {table_id: row.table_id})
MATCH (d:Database {db_id: row.db_id})
MERGE (t)-[:BELONGS_TO]->(d);

UNWIND [
  {field_id:'FIELD_0001', table_id:'TB_0001'},
  {field_id:'FIELD_0002', table_id:'TB_0003'},
  {field_id:'FIELD_0003', table_id:'TB_0006'},
  {field_id:'FIELD_0004', table_id:'TB_0007'},
  {field_id:'FIELD_0005', table_id:'TB_0008'},
  {field_id:'FIELD_0006', table_id:'TB_0002'},
  {field_id:'FIELD_0007', table_id:'TB_0009'},
  {field_id:'FIELD_0008', table_id:'TB_0004'},
  {field_id:'FIELD_0009', table_id:'TB_0005'}
] AS row
MATCH (f:Field {field_id: row.field_id})
MATCH (t:Table {table_id: row.table_id})
MERGE (f)-[:BELONGS_TO]->(t);

UNWIND [
  {field_id:'FIELD_0001', concept_id:'BC_0001'},
  {field_id:'FIELD_0002', concept_id:'BC_0001'},
  {field_id:'FIELD_0003', concept_id:'BC_0003'},
  {field_id:'FIELD_0004', concept_id:'BC_0002'},
  {field_id:'FIELD_0005', concept_id:'BC_0002'},
  {field_id:'FIELD_0006', concept_id:'BC_0004'},
  {field_id:'FIELD_0007', concept_id:'BC_0005'},
  {field_id:'FIELD_0008', concept_id:'BC_0006'},
  {field_id:'FIELD_0009', concept_id:'BC_0007'}
] AS row
MATCH (f:Field {field_id: row.field_id})
MATCH (c:BusinessConcept {concept_id: row.concept_id})
MERGE (f)-[:MAPS_TO]->(c);

UNWIND [
  {left_field:'FIELD_0006', right_field:'FIELD_0007', reason:'status字段语义冲突'},
  {left_field:'FIELD_0001', right_field:'FIELD_0003', reason:'学生身份与读者身份映射冲突'},
  {left_field:'FIELD_0008', right_field:'FIELD_0009', reason:'生活域身份键命名冲突'}
] AS row
MATCH (l:Field {field_id: row.left_field})
MATCH (r:Field {field_id: row.right_field})
MERGE (l)-[rel:CONFLICTS_WITH]->(r)
SET rel.reason = row.reason;

UNWIND [
  {student_id:'STU_2025_0001', account_id:'CARD_0001'},
  {student_id:'STU_2025_0088', account_id:'CARD_0002'}
] AS row
MATCH (s:Student {student_id: row.student_id})
MATCH (a:Account {account_id: row.account_id})
MERGE (s)-[:SAME_AS]->(a);

UNWIND [
  {student_id:'STU_2025_0001', course_code:'CS101'},
  {student_id:'STU_2025_0088', course_code:'MATH204'},
  {student_id:'STU_2025_1566', course_code:'EE310'}
] AS row
MATCH (s:Student {student_id: row.student_id})
MATCH (c:Course {course_code: row.course_code})
MERGE (s)-[:USES]->(c);

UNWIND [
  {student_id:'STU_2025_0001', room_id:'ROOM_0502'},
  {student_id:'STU_2025_0088', room_id:'ROOM_8306'}
] AS row
MATCH (s:Student {student_id: row.student_id})
MATCH (r:DormRoom {room_id: row.room_id})
MERGE (s)-[:ASSOCIATED_WITH]->(r);

UNWIND [
  {account_id:'CARD_0001', room_id:'ROOM_0502'},
  {account_id:'CARD_0002', room_id:'ROOM_8306'}
] AS row
MATCH (a:Account {account_id: row.account_id})
MATCH (r:DormRoom {room_id: row.room_id})
MERGE (a)-[:ASSOCIATED_WITH]->(r);

UNWIND [
  {teacher_id:'T00021', student_id:'STU_2025_0001'},
  {teacher_id:'T00108', student_id:'STU_2025_0088'}
] AS row
MATCH (t:Teacher {teacher_id: row.teacher_id})
MATCH (s:Student {student_id: row.student_id})
MERGE (t)-[:GUIDES]->(s);

UNWIND [
  {project_id:'RP_0001', asset_id:'FA_0001'},
  {project_id:'RP_0002', asset_id:'FA_0002'}
] AS row
MATCH (p:Project {project_id: row.project_id})
MATCH (a:Asset {asset_id: row.asset_id})
MERGE (p)-[:FUNDS]->(a);
