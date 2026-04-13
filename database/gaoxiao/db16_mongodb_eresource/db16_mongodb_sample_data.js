const dbRef = db.getSiblingDB('gaoxiao_db16_eresource');

dbRef.e_resource_meta.insertMany([
  {
    resource_code: 'RES_CNKI',
    resource_name: 'CNKI',
    provider: '同方知网',
    resource_type: '期刊数据库',
    status: 'active'
  },
  {
    resource_code: 'RES_WOS',
    resource_name: 'Web of Science',
    provider: 'Clarivate',
    resource_type: '引文数据库',
    status: 'active'
  },
  {
    resource_code: 'RES_IEEE',
    resource_name: 'IEEE Xplore',
    provider: 'IEEE',
    resource_type: '全文数据库',
    status: 'maintenance'
  }
]);

dbRef.search_log.insertMany([
  {
    log_id: 'SRCH_0001',
    user_ref_id: 'STU_2025_0001',
    keyword: 'while循环',
    resource_name: 'CNKI',
    search_time: new Date('2026-03-29T09:14:00+08:00')
  },
  {
    log_id: 'SRCH_0002',
    user_ref_id: 'STU_2025_0088',
    keyword: '行列式',
    resource_name: 'Web of Science',
    search_time: new Date('2026-03-29T10:40:00+08:00')
  },
  {
    log_id: 'SRCH_0003',
    user_ref_id: 'T00021',
    keyword: '译码器',
    resource_name: 'IEEE Xplore',
    search_time: new Date('2026-03-29T11:07:00+08:00')
  }
]);

dbRef.access_log.insertMany([
  {
    log_id: 'ER_0001',
    user_ref_id: 'STU_2025_0001',
    real_name: '张晨',
    user_type: 'student',
    resource_name: 'CNKI',
    access_time: new Date('2026-03-29T09:15:22+08:00'),
    access_ip: '10.12.3.15',
    action_type: 'view',
    session_id: 'S_0001'
  },
  {
    log_id: 'ER_0002',
    user_ref_id: 'STU_2025_0088',
    real_name: '李媛',
    user_type: 'student',
    resource_name: 'Web of Science',
    access_time: new Date('2026-03-29T10:42:31+08:00'),
    access_ip: '10.12.8.26',
    action_type: 'download',
    session_id: 'S_0002'
  },
  {
    log_id: 'ER_0003',
    user_ref_id: 'T00021',
    real_name: '刘强',
    user_type: 'teacher',
    resource_name: 'IEEE Xplore',
    access_time: new Date('2026-03-29T11:08:17+08:00'),
    access_ip: '10.12.9.41',
    action_type: 'search',
    session_id: 'S_0003'
  }
]);

dbRef.download_log.insertMany([
  {
    dl_id: 'DL_0001',
    log_id: 'ER_0001',
    file_name: 'database_intro.pdf',
    file_size_kb: 1250,
    download_time: new Date('2026-03-29T09:16:10+08:00'),
    result: 'success'
  },
  {
    dl_id: 'DL_0002',
    log_id: 'ER_0002',
    file_name: 'algebra_matrix.pdf',
    file_size_kb: 980,
    download_time: new Date('2026-03-29T10:43:20+08:00'),
    result: 'success'
  },
  {
    dl_id: 'DL_0003',
    log_id: 'ER_0003',
    file_name: 'decoder_design.pdf',
    file_size_kb: 1560,
    download_time: new Date('2026-03-29T11:09:00+08:00'),
    result: 'failed'
  }
]);

dbRef.session_trace.insertMany([
  {
    session_id: 'S_0001',
    user_ref_id: 'STU_2025_0001',
    start_time: new Date('2026-03-29T09:10:00+08:00'),
    end_time: new Date('2026-03-29T09:30:00+08:00'),
    session_status: 'closed'
  },
  {
    session_id: 'S_0002',
    user_ref_id: 'STU_2025_0088',
    start_time: new Date('2026-03-29T10:35:00+08:00'),
    end_time: new Date('2026-03-29T10:50:00+08:00'),
    session_status: 'closed'
  },
  {
    session_id: 'S_0003',
    user_ref_id: 'T00021',
    start_time: new Date('2026-03-29T11:05:00+08:00'),
    end_time: new Date('2026-03-29T11:20:00+08:00'),
    session_status: 'aborted'
  }
]);

const checks = [
  'e_resource_meta',
  'search_log',
  'access_log',
  'download_log',
  'session_trace'
];

for (const name of checks) {
  const count = dbRef.getCollection(name).countDocuments({});
  if (count < 1) {
    throw new Error(`DB16 MongoDB collection has no sample data: ${name}`);
  }
}

print('DB16 MongoDB sample data initialized and verified: gaoxiao_db16_eresource');
