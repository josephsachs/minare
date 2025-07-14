#!/bin/bash

cd /Users/josephsachs/documents/dev/minare/docker/ 
docker compose down
cd ..
mvn package
cd docker
docker compose build --no-cache
docker compose up -d
docker compose logs -f
