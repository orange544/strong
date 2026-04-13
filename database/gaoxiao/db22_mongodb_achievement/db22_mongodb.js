const dbName = 'gaoxiao_db22_achievement';
const collections = [
  'paper_result',
  'patent_result',
  'book_result',
  'award_result',
  'achievement_attachment'
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

dbRef.paper_result.createIndex({ paper_id: 1 }, { unique: true });
dbRef.patent_result.createIndex({ patent_id: 1 }, { unique: true });
dbRef.book_result.createIndex({ book_id: 1 }, { unique: true });
dbRef.award_result.createIndex({ award_id: 1 }, { unique: true });
dbRef.achievement_attachment.createIndex({ attachment_id: 1 }, { unique: true });

print('DB22 MongoDB schema initialized: gaoxiao_db22_achievement');
