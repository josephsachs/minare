#!/bin/bash

WORKER_COUNT=${1:-1}
shift 2>/dev/null || true

BUILD_ONLY=false
START_NODEGRAPH=false
START_INTEGRATION=false

while getopts "bni" flag; do
  case "${flag}" in
    b) BUILD_ONLY=true ;;
    n) START_NODEGRAPH=true ;;
    i) START_INTEGRATION=true ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

build_framework() {
    echo "Building minare-framework..."
    cd minare-framework
    mvn clean install -DskipTests
    cd ..
}

build_nodegraph() {
    echo "Building nodegraph..."
    cd minare-example/nodegraph
    mvn clean package -DskipTests
    cd ../..
}

build_integration() {
    echo "Building integration tests..."
    cd minare-example/integration
    mvn clean package -DskipTests
    cd ../..
}

start_docker() {
    cd docker
    docker compose down -v

    rm -rf logs/*.log
    rm -rf data/{db,redis,kafka}/*
    chmod 755 data/{db,redis,kafka}
    mkdir -p logs

    docker compose build --no-cache
    WORKER_COUNT=${WORKER_COUNT} docker compose up -d --scale worker=${WORKER_COUNT}
    docker compose logs -f
}

if $BUILD_ONLY; then
    build_framework
    echo "Framework build complete."
    exit 0
fi

if $START_NODEGRAPH; then
    echo "Building and starting NodeGraph..."
    build_framework
    build_nodegraph
    start_docker
elif $START_INTEGRATION; then
    echo "Building and starting Integration Tests..."
    build_framework
    build_integration
    start_docker
else
    echo "Usage: ./run.sh [WORKER_COUNT] -n|-i|-b"
    echo "  -n: Build and run NodeGraph application"
    echo "  -i: Build and run Integration test application"
    echo "  -b: Build framework only (no docker)"
    exit 1
fi