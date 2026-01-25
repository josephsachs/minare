#!/bin/bash

if [ -z "$1" ]; then
  echo "Error: Database name required"
  echo "Usage: $0 <database_name>"
  exit 1
fi

DB_NAME="$1"

docker exec -it mongodb mongosh --quiet --eval "
var dbName = '$DB_NAME';
var allSnapshots = [];
db.getSiblingDB(dbName).getCollectionNames().filter(c => c.startsWith('snapshot_')).forEach(c => {
  var meta = db.getSiblingDB(dbName)[c].findOne({_id: 'metadata'});
  if(meta) {
    allSnapshots.push({collection: c, createdAt: meta.createdAt.toNumber()});
  }
});
allSnapshots.sort((a,b) => a.createdAt - b.createdAt).forEach(s => {
  print('\\n=== ' + s.collection + ' (' + new Date(s.createdAt) + ') ===');
  db.getSiblingDB(dbName)[s.collection].find().forEach(printjson);
});"