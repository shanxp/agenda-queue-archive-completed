
const usage= 'Usage:  /usr/local/bin/node index.js <username> <password> <host/ip> <database> <auth_database> <source_collection> <queue_type> <target_collection> <batch_size>';
const maxNumOfParams = 10;

if(process.argv.length < maxNumOfParams ) {
  console.error('Check parameters!');
  console.error(usage);
  process.exit(1);
}
else {
  const username = process.argv[2];
  const password = process.argv[3];
  const hostname = process.argv[4];
  const database        = process.argv[5];
  const authSource = process.argv[6];
  const originalCollection = process.argv[7];
  const queueName = process.argv[8];
  const destCollection = process.argv[9];
  const limit = parseInt(process.argv[10]) || 10;

  const MongoClient = require('mongodb').MongoClient;

  const mongodb = {
    username:   username,
    password:   password,
    hostname:   hostname,
    db:         database,
    authSource: authSource
  }

  const connectionString = `mongodb://${mongodb.username}:${mongodb.password}@${mongodb.hostname}/${mongodb.db}?authSource=${mongodb.authSource}`;

  // success
  const condition = {
    "name" : queueName ,
    "nextRunAt": { $eq: null },
    "lastModifiedBy": { $eq: null },
    "lockedAt": { $eq: null },
    "lastRunAt" : { $ne: null },
    "lastFinishedAt" : { $ne: null },
    $or : [ { "failedAt": { $eq: null }} , { $where : "this.lastFinishedAt > this.failedAt" } ]
  };

  ( async () => {
    try {
        const client = await MongoClient.connect(connectionString, { useNewUrlParser: true });
        const db = await client.db(database);
        const source = await db.collection(originalCollection);
        const target = await db.collection(destCollection);
        const items = await source.find(condition).limit(limit).toArray();  
    
        let insertedIds = [];
        let docsToInsert = [];
        let id;
        let docObj;
        items.forEach(doc => {
          id = doc._id;
          docObj = {
            updateOne : {
              filter: {_id: id},
              update: doc,
              upsert: true
            }
          };
          docsToInsert.push(docObj);        
          insertedIds.push(id);
        });
        //Something to move over?
        if(docsToInsert.length && insertedIds.length) {
          console.log(`Total ${docsToInsert.length} docs found`);
          await target.bulkWrite(docsToInsert);
          console.log('Insert complete');
          await source.deleteMany( { _id: { $in: insertedIds } });
          console.log('Delete complete');
        } else {
          console.log('Nothing to move!');
        }
        process.exit(1);
    } catch(e) {
      console.error(e);
      process.exit(1);
    }
  })();

}