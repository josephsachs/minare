#!/bin/bash
echo "Waiting for MongoDB to start..."
until mongosh --quiet --eval "db.runCommand({ ping: 1 })"; do
  sleep 2
done

echo "Initiating replica set..."
mongosh --eval 'rs.initiate({_id: "rs0", members: [{ _id: 0, host: "mongo:27017" }]})'

echo "Replica set initiated."
