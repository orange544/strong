const dbRef = db.getSiblingDB('university_document_db');

dbRef.teaching_materials.replaceOne(
  { _id: 'TM_CSE3001_2025FALL' },
  {
    _id: 'TM_CSE3001_2025FALL',
    course_code: 'CSE3001',
    course_name: '数据库系统原理',
    version: '2025Fall-v1',
    teaching_objectives: ['掌握关系数据库基本原理', '理解事务与并发控制', '能够完成模式设计'],
    assessment_plan: { usual: 30, experiment: 20, final: 50 },
    keywords: ['数据库', 'SQL', '事务'],
    status: 'published'
  },
  { upsert: true }
);

dbRef.teaching_materials.replaceOne(
  { _id: 'TM_CSE3002_2025FALL' },
  {
    _id: 'TM_CSE3002_2025FALL',
    course_code: 'CSE3002',
    course_name: '知识图谱导论',
    version: '2025Fall-v1',
    teaching_objectives: ['理解知识表示方法', '掌握图谱构建流程', '了解图谱检索场景'],
    assessment_plan: { usual: 40, project: 20, final: 40 },
    keywords: ['知识图谱', 'RDF', '语义网'],
    status: 'published'
  },
  { upsert: true }
);

dbRef.student_growth_archive.replaceOne(
  { _id: 'SGA_202100234' },
  {
    _id: 'SGA_202100234',
    student_id: 'S202100234',
    competitions: [
      {
        name: '中国大学生计算机设计大赛',
        level: '省级',
        award: '二等奖'
      }
    ],
    internships: [
      {
        company: '南京数语科技有限公司',
        role: '数据开发实习生',
        months: 3
      }
    ],
    volunteer_hours: 28,
    status: 'active'
  },
  { upsert: true }
);

dbRef.student_growth_archive.replaceOne(
  { _id: 'SGA_202100235' },
  {
    _id: 'SGA_202100235',
    student_id: 'S202100235',
    competitions: [
      {
        name: '蓝桥杯程序设计竞赛',
        level: '国家级',
        award: '三等奖'
      }
    ],
    internships: [
      {
        company: '苏州智联信息技术有限公司',
        role: '后端开发实习生',
        months: 2
      }
    ],
    volunteer_hours: 16,
    status: 'active'
  },
  { upsert: true }
);

print('School A MongoDB sample data initialized: university_document_db');