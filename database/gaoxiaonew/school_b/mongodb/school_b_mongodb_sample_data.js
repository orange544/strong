const dbRef = db.getSiblingDB('university_document_db');

dbRef.teaching_materials.replaceOne(
  { _id: 'TM_CUR3001_2025FALL' },
  {
    _id: 'TM_CUR3001_2025FALL',
    curriculum_id: 'CUR3001',
    subject_name: '数据库系统原理',
    outline_version: '2025-fall-v1',
    knowledge_units: ['关系模型', 'SQL', '索引与优化'],
    assessment: { usual: 30, lab: 20, exam: 50 },
    keywords: ['database', 'sql', 'index'],
    status: 'published'
  },
  { upsert: true }
);

dbRef.teaching_materials.replaceOne(
  { _id: 'TM_CUR3002_2025FALL' },
  {
    _id: 'TM_CUR3002_2025FALL',
    curriculum_id: 'CUR3002',
    subject_name: '信息检索',
    outline_version: '2025-fall-v1',
    knowledge_units: ['布尔检索', '向量空间模型', '排序检索'],
    assessment: { usual: 40, project: 20, exam: 40 },
    keywords: ['IR', 'ranking', 'retrieval'],
    status: 'published'
  },
  { upsert: true }
);

dbRef.student_growth_archive.replaceOne(
  { _id: 'SGA_L20210088' },
  {
    _id: 'SGA_L20210088',
    learner_id: 'L20210088',
    awards: [
      {
        name: '互联网+大学生创新创业大赛',
        level: '省级',
        result: '银奖'
      }
    ],
    certificates: ['大学英语六级', '计算机二级'],
    volunteer_hours: 20,
    status: 'active'
  },
  { upsert: true }
);

dbRef.student_growth_archive.replaceOne(
  { _id: 'SGA_L20210089' },
  {
    _id: 'SGA_L20210089',
    learner_id: 'L20210089',
    awards: [
      {
        name: '数学建模竞赛',
        level: '校级',
        result: '一等奖'
      }
    ],
    certificates: ['普通话二甲'],
    volunteer_hours: 12,
    status: 'active'
  },
  { upsert: true }
);

print('School B MongoDB sample data initialized: university_document_db');