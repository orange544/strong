const dbName = 'gaoxiao_db16_eresource';
const collections = [
  'e_resource_meta',
  'search_log',
  'access_log',
  'download_log',
  'session_trace'
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

dbRef.e_resource_meta.createIndex({ resource_code: 1 }, { unique: true });
dbRef.search_log.createIndex({ log_id: 1 }, { unique: true });
dbRef.access_log.createIndex({ log_id: 1 }, { unique: true });
dbRef.download_log.createIndex({ dl_id: 1 }, { unique: true });
dbRef.session_trace.createIndex({ session_id: 1 }, { unique: true });

print('DB16 MongoDB schema initialized: gaoxiao_db16_eresource');
