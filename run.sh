#!/bin/bash

# Parse command line arguments
WORKER_COUNT=${1:-1}

cd docker
docker compose down -v

cd ..
mvn package

cd docker

# Clean up all log files
rm -rf logs/*.log
rm -rf data/{db,redis,kafka}/*
chmod 755 data/{db,redis,kafka}

# Create log directory if it doesn't exist
mkdir -p logs

docker compose build --no-cache

# Start services with specified worker count
WORKER_COUNT=${WORKER_COUNT} docker compose up -d --scale worker=${WORKER_COUNT}

# Follow logs for all services
docker compose logs -f