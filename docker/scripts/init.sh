#!/bin/bash
set -e

# Start MongoDB in the background
mongod --replSet rs0 --bind_ip_all &

# Wait for MongoDB to be ready
until mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
  echo "Waiting for MongoDB to be ready..."
  sleep 2
done

# Check if replica set is already initialized
REPLICA_STATUS=$(mongosh --quiet --eval "rs.status()" || echo "NotYetInitialized")

if echo "$REPLICA_STATUS" | grep -q "NotYetInitialized"; then
    echo "Initializing replica set..."
    mongosh --eval '
        rs.initiate({
            _id: "rs0",
            members: [{
                _id: 0,
                host: "mongodb-rs:27017"
            }]
        })
    '

    # Wait for replica set to stabilize
    until mongosh --eval "rs.isMaster().ismaster" | grep -q "true"; do
        echo "Waiting for replica set to elect primary..."
        sleep 2
    done

    echo "Replica set initialized successfully"
else
    echo "Replica set already initialized"
fi

# Keep container running with MongoDB in foreground
wait