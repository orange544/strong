const dbName = 'gaoxiao_db2_material';
const collections = [
  'student_material',
  'identity_document',
  'admission_notice_file',
  'family_background_doc',
  'qualification_review_doc'
];

const dbRef = db.getSiblingDB(dbName);

for (const name of collections) {
  if (dbRef.getCollectionNames().includes(name)) {
    dbRef.getCollection(name).drop();
  }
}

for (const name of collections) {
  dbRef.createCollection(name);
}

dbRef.student_material.createIndex({ material_id: 1 }, { unique: true });
dbRef.student_material.createIndex({ person_id: 1 });
dbRef.identity_document.createIndex({ material_id: 1 }, { unique: true });
dbRef.admission_notice_file.createIndex({ material_id: 1 }, { unique: true });
dbRef.family_background_doc.createIndex({ material_id: 1 }, { unique: true });
dbRef.qualification_review_doc.createIndex({ material_id: 1 }, { unique: true });

print('DB2 MongoDB schema initialized: gaoxiao_db2_material');
