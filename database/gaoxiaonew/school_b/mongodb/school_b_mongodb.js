const dbName = 'university_document_db';
const collections = ['teaching_materials', 'student_growth_archive'];

const dbRef = db.getSiblingDB(dbName);

for (const name of collections) {
  if (!dbRef.getCollectionNames().includes(name)) {
    dbRef.createCollection(name);
  }
}

function ensureIndex(coll, keySpec) {
  const keyJson = JSON.stringify(keySpec);
  const hasSameKey = coll.getIndexes().some((idx) => JSON.stringify(idx.key) === keyJson);
  if (!hasSameKey) {
    coll.createIndex(keySpec);
  }
}

ensureIndex(dbRef.teaching_materials, { curriculum_id: 1 });
ensureIndex(dbRef.student_growth_archive, { learner_id: 1 });

print('School B MongoDB schema initialized: university_document_db');
