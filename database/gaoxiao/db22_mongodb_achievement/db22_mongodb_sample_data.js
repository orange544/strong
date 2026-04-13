const dbRef = db.getSiblingDB('gaoxiao_db22_achievement');

dbRef.paper_result.insertMany([
  {
    paper_id: 'PA_0001',
    teacher_id: 'T00021',
    teacher_name: '刘强',
    paper_title: 'A Decentralized Semantic Alignment Method',
    journal_name: 'IEEE TBD',
    publish_time: '2025-11-10',
    index_type: 'SCI一区',
    attachment_url: '/paper/pa_0001.pdf',
    achievement_status: '已认定'
  },
  {
    paper_id: 'PA_0002',
    teacher_id: 'T00108',
    teacher_name: '周敏',
    paper_title: 'Federated KG for Campus Data Sharing',
    journal_name: 'Information Sciences',
    publish_time: '2025-09-03',
    index_type: 'SCI二区',
    attachment_url: '/paper/pa_0002.pdf',
    achievement_status: '待审核'
  },
  {
    paper_id: 'PA_0003',
    teacher_id: 'T00356',
    teacher_name: '陈浩',
    paper_title: 'Schema Unification in Heterogeneous Databases',
    journal_name: '软件学报',
    publish_time: '2025-12-21',
    index_type: 'EI',
    attachment_url: '/paper/pa_0003.pdf',
    achievement_status: '已入库'
  }
]);

dbRef.patent_result.insertMany([
  {
    patent_id: 'PT_0001',
    teacher_id: 'T00021',
    patent_name: '语义对齐方法与系统',
    patent_no: 'CN202510000001',
    apply_date: '2025-07-12',
    patent_status: '已授权'
  },
  {
    patent_id: 'PT_0002',
    teacher_id: 'T00108',
    patent_name: '联邦图谱构建装置',
    patent_no: 'CN202510000002',
    apply_date: '2025-07-25',
    patent_status: '实审中'
  },
  {
    patent_id: 'PT_0003',
    teacher_id: 'T00356',
    patent_name: '异构字段映射工具',
    patent_no: 'CN202510000003',
    apply_date: '2025-08-01',
    patent_status: '受理'
  }
]);

dbRef.book_result.insertMany([
  {
    book_id: 'BK_0001',
    teacher_id: 'T00021',
    book_title: '高校数据治理导论',
    publisher: '高等教育出版社',
    publish_date: '2025-06-01',
    status: '已出版'
  },
  {
    book_id: 'BK_0002',
    teacher_id: 'T00108',
    book_title: '联邦学习与知识图谱',
    publisher: '机械工业出版社',
    publish_date: '2025-09-20',
    status: '已出版'
  },
  {
    book_id: 'BK_0003',
    teacher_id: 'T00356',
    book_title: '数据库模式统一技术',
    publisher: '人民邮电出版社',
    publish_date: '2025-12-10',
    status: '待出版'
  }
]);

dbRef.award_result.insertMany([
  {
    award_id: 'AW_0001',
    teacher_id: 'T00021',
    award_name: '省科技进步二等奖',
    award_level: '省部级',
    award_date: '2025-11-01',
    status: '已认定'
  },
  {
    award_id: 'AW_0002',
    teacher_id: 'T00108',
    award_name: '教学成果一等奖',
    award_level: '校级',
    award_date: '2025-10-18',
    status: '待审核'
  },
  {
    award_id: 'AW_0003',
    teacher_id: 'T00356',
    award_name: '青年教师创新奖',
    award_level: '校级',
    award_date: '2025-12-05',
    status: '已入库'
  }
]);

dbRef.achievement_attachment.insertMany([
  {
    attachment_id: 'ATT_0001',
    owner_type: 'paper',
    owner_id: 'PA_0001',
    file_url: '/paper/pa_0001.pdf',
    file_type: 'pdf',
    upload_time: new Date('2025-11-12T10:00:00+08:00')
  },
  {
    attachment_id: 'ATT_0002',
    owner_type: 'patent',
    owner_id: 'PT_0002',
    file_url: '/patent/pt_0002.pdf',
    file_type: 'pdf',
    upload_time: new Date('2025-11-12T10:10:00+08:00')
  },
  {
    attachment_id: 'ATT_0003',
    owner_type: 'award',
    owner_id: 'AW_0003',
    file_url: '/award/aw_0003.jpg',
    file_type: 'image',
    upload_time: new Date('2025-11-12T10:20:00+08:00')
  }
]);

const checks = [
  'paper_result',
  'patent_result',
  'book_result',
  'award_result',
  'achievement_attachment'
];

for (const name of checks) {
  const count = dbRef.getCollection(name).countDocuments({});
  if (count < 1) {
    throw new Error(`DB22 MongoDB collection has no sample data: ${name}`);
  }
}

print('DB22 MongoDB sample data initialized and verified: gaoxiao_db22_achievement');
