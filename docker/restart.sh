#!/bin/bash

cd /Users/josephsachs/documents/dev/minare/docker/ 
docker compose down

cd ..
mvn package

cd docker

rm logs/websocket-server.log
rm logs2/websocket-server.log

docker compose build --no-cache
docker compose up -d
docker compose logs -f
