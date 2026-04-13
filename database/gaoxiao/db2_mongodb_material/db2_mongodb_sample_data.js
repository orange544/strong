const dbRef = db.getSiblingDB('gaoxiao_db2_material');

dbRef.student_material.insertMany([
  {
    material_id: 'MAT_0001',
    person_id: 'CAND_2025_0001',
    name: '张晨',
    sex_code: 'M',
    doc_type: '身份证扫描件',
    file_url: '/files/admission/id_0001.pdf',
    review_status: '已通过',
    upload_time: new Date('2025-08-01T10:15:00+08:00')
  },
  {
    material_id: 'MAT_0088',
    person_id: 'CAND_2025_0088',
    name: '李媛',
    sex_code: 'F',
    doc_type: '录取通知书',
    file_url: '/files/admission/notice_0088.pdf',
    review_status: '待审核',
    upload_time: new Date('2025-08-02T11:20:00+08:00')
  },
  {
    material_id: 'MAT_1566',
    person_id: 'CAND_2025_1566',
    name: '王磊',
    sex_code: 'U',
    doc_type: '家庭情况说明',
    file_url: '/files/admission/family_1566.pdf',
    review_status: '已退回',
    upload_time: new Date('2025-08-03T09:45:00+08:00')
  }
]);

dbRef.identity_document.insertMany([
  {
    material_id: 'IDDOC_0001',
    person_id: 'CAND_2025_0001',
    id_card_no: '320583200701015612',
    issue_org: '苏州市公安局',
    valid_until: '2045-07-01',
    review_status: '已通过'
  },
  {
    material_id: 'IDDOC_0088',
    person_id: 'CAND_2025_0088',
    id_card_no: '370102200609086527',
    issue_org: '济南市公安局',
    valid_until: '2044-12-10',
    review_status: '待审核'
  },
  {
    material_id: 'IDDOC_1566',
    person_id: 'CAND_2025_1566',
    id_card_no: '110108200812123216',
    issue_org: '北京市公安局',
    valid_until: '2046-03-18',
    review_status: '已退回'
  }
]);

dbRef.admission_notice_file.insertMany([
  {
    material_id: 'NOTICE_0001',
    person_id: 'CAND_2025_0001',
    notice_no: 'ADMIT20250001',
    file_url: '/files/admission/notice_0001.pdf',
    status: '已上传'
  },
  {
    material_id: 'NOTICE_0088',
    person_id: 'CAND_2025_0088',
    notice_no: 'ADMIT20250088',
    file_url: '/files/admission/notice_0088.pdf',
    status: '已上传'
  },
  {
    material_id: 'NOTICE_1566',
    person_id: 'CAND_2025_1566',
    notice_no: 'ADMIT20251566',
    file_url: '/files/admission/notice_1566.pdf',
    status: '待上传'
  }
]);

dbRef.family_background_doc.insertMany([
  {
    material_id: 'FAM_0001',
    person_id: 'CAND_2025_0001',
    family_income_level: '中等',
    emergency_contact: '张建国',
    relation: '父亲',
    contact_mobile: '13811110001'
  },
  {
    material_id: 'FAM_0088',
    person_id: 'CAND_2025_0088',
    family_income_level: '一般',
    emergency_contact: '李梅',
    relation: '母亲',
    contact_mobile: '13922220088'
  },
  {
    material_id: 'FAM_1566',
    person_id: 'CAND_2025_1566',
    family_income_level: '困难',
    emergency_contact: '王海',
    relation: '父亲',
    contact_mobile: '13733331566'
  }
]);

dbRef.qualification_review_doc.insertMany([
  {
    material_id: 'QR_0001',
    person_id: 'CAND_2025_0001',
    review_item: '入学资格复审',
    review_result: '通过',
    reviewer: '审核员A',
    review_time: new Date('2025-08-05T13:20:00+08:00')
  },
  {
    material_id: 'QR_0088',
    person_id: 'CAND_2025_0088',
    review_item: '入学资格复审',
    review_result: '待审核',
    reviewer: '审核员B',
    review_time: new Date('2025-08-05T14:10:00+08:00')
  },
  {
    material_id: 'QR_1566',
    person_id: 'CAND_2025_1566',
    review_item: '入学资格复审',
    review_result: '退回补充',
    reviewer: '审核员C',
    review_time: new Date('2025-08-05T15:05:00+08:00')
  }
]);

const checks = [
  'student_material',
  'identity_document',
  'admission_notice_file',
  'family_background_doc',
  'qualification_review_doc'
];

for (const name of checks) {
  const count = dbRef.getCollection(name).countDocuments({});
  if (count < 1) {
    throw new Error(`DB2 MongoDB collection has no sample data: ${name}`);
  }
}

print('DB2 MongoDB sample data initialized and verified: gaoxiao_db2_material');
