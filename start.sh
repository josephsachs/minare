#!/bin/bash

docker-compose -f docker/docker-compose.yaml up -d

MONGO_URI=mongodb://localhost:27017/minare_example?replicaSet=rs0 \
REDIS_URI=redis://localhost:6379 \
RESET_DB=true \
mvn exec:java -pl minare-example