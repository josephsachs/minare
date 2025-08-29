#!/bin/sh
# Dynamic Infrastructure Management Container Entrypoint
# Automatically discovers and registers worker instances

set -e

# Configuration with defaults
COORDINATOR_HOST="${COORDINATOR_HOST:-app-coordinator}"
COORDINATOR_PORT="${COORDINATOR_PORT:-9090}"
STARTUP_DELAY="${STARTUP_DELAY:-5}"
RETRY_INTERVAL="${RETRY_INTERVAL:-2}"
MAX_RETRIES="${MAX_RETRIES:-10}"
WORKER_PREFIX="${WORKER_PREFIX:-minare_worker}"
WORKER_COUNT="${WORKER_COUNT:-1}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFRA]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[INFRA]${NC} $1"
}

log_error() {
    echo -e "${RED}[INFRA]${NC} $1"
}

# Function to send event bus message
send_event() {
    local address=$1
    local body=$2

    curl -s -w "\n%{http_code}" -X POST \
        "http://${COORDINATOR_HOST}:${COORDINATOR_PORT}/eventbus" \
        -H "Content-Type: application/json" \
        -d "{
            \"address\": \"${address}\",
            \"body\": ${body}
        }"
}

# Function to discover worker containers
discover_workers() {
    log_info "Discovering worker containers..."

    # Docker Compose creates containers with naming pattern: projectname_servicename_index
    # For scaled services, they'll be: minare_worker_1, minare_worker_2, etc.

    local workers=""
    for i in $(seq 1 ${WORKER_COUNT}); do
        worker_name="${WORKER_PREFIX}_${i}"
        log_info "Looking for worker: ${worker_name}"

        # Check if the worker hostname resolves (Docker's internal DNS)
        if nslookup "${worker_name}" >/dev/null 2>&1; then
            workers="${workers} ${worker_name}"
            log_info "Found worker: ${worker_name}"
        else
            log_warn "Worker ${worker_name} not found (might still be starting)"
        fi
    done

    echo "${workers}"
}

# Function to create Kafka topics
create_kafka_topics() {
    log_info "Creating Kafka topics..."

    # Wait for Kafka to be ready
    while ! docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 >/dev/null 2>&1; do
        log_info "Waiting for Kafka to be ready..."
        sleep 2
    done

    # Create topics
    for topic in "minare.operations" "minare.system.events"; do
        log_info "Creating topic: ${topic}"
        docker exec kafka kafka-topics --create --if-not-exists \
            --bootstrap-server localhost:9092 \
            --topic "${topic}" \
            --partitions 3 \
            --replication-factor 1 || log_warn "Topic ${topic} may already exist"
    done

    log_info "Kafka topics created successfully"
}

# Function to add a worker
add_worker() {
    local worker_id=$1
    local retry_count=0

    log_info "Adding worker: ${worker_id}"

    while [ ${retry_count} -lt ${MAX_RETRIES} ]; do
        response=$(send_event "minare.coordinator.infra.add-worker" "{\"workerId\": \"${worker_id}\"}")

        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | head -n-1)

        if [ "${http_code}" = "200" ] || [ "${http_code}" = "204" ]; then
            log_info "Successfully added worker: ${worker_id}"
            return 0
        else
            retry_count=$((retry_count + 1))
            log_warn "Failed to add worker ${worker_id} (attempt ${retry_count}/${MAX_RETRIES})"
            log_warn "Response: ${http_code} - ${body}"

            if [ ${retry_count} -lt ${MAX_RETRIES} ]; then
                log_info "Retrying in ${RETRY_INTERVAL}s..."
                sleep ${RETRY_INTERVAL}
            fi
        fi
    done

    log_error "Failed to add worker ${worker_id} after ${MAX_RETRIES} attempts"
    return 1
}

# Function to wait for all workers to be healthy
wait_for_workers() {
    local expected_count=$1
    local timeout=60
    local elapsed=0

    log_info "Waiting for ${expected_count} workers to be healthy..."

    while [ ${elapsed} -lt ${timeout} ]; do
        local healthy_count=0

        for i in $(seq 1 ${expected_count}); do
            worker_name="${WORKER_PREFIX}_${i}"

            # Try to health check the worker
            if curl -f "http://${worker_name}:8080/health" >/dev/null 2>&1; then
                healthy_count=$((healthy_count + 1))
            fi
        done

        if [ ${healthy_count} -eq ${expected_count} ]; then
            log_info "All ${expected_count} workers are healthy!"
            return 0
        fi

        log_info "Healthy workers: ${healthy_count}/${expected_count}"
        sleep 5
        elapsed=$((elapsed + 5))
    done

    log_warn "Timeout waiting for workers to be healthy"
    return 1
}

# Main execution
main() {
    log_info "Dynamic Infrastructure management container starting..."
    log_info "Configuration:"
    log_info "  Coordinator: ${COORDINATOR_HOST}:${COORDINATOR_PORT}"
    log_info "  Worker prefix: ${WORKER_PREFIX}"
    log_info "  Expected worker count: ${WORKER_COUNT}"
    log_info "  Startup delay: ${STARTUP_DELAY}s"

    # Install required tools if not present
    if ! command -v curl >/dev/null 2>&1 || ! command -v nslookup >/dev/null 2>&1; then
        log_info "Installing required tools..."
        apk add --no-cache curl bind-tools
    fi

    # Setup Kafka topics
    create_kafka_topics

    # Wait for coordinator to be ready
    log_info "Waiting ${STARTUP_DELAY}s for coordinator to stabilize..."
    sleep ${STARTUP_DELAY}

    # Wait for workers to be healthy
    wait_for_workers ${WORKER_COUNT}

    # Discover and add workers
    workers=$(discover_workers)

    if [ -z "${workers}" ]; then
        log_error "No workers discovered!"
        exit 1
    fi

    success=true
    for worker in ${workers}; do
        if ! add_worker "${worker}"; then
            success=false
        fi
    done

    if [ "$success" = true ]; then
        log_info "All workers added successfully"
    else
        log_error "Some workers failed to add"
    fi

    log_info "Initialization complete. Container staying alive for monitoring..."

    # Keep container alive and periodically check for new workers
    while true; do
        sleep 30

        # Could add logic here to detect scaled workers and register them
        # For now, just stay alive
    done
}

# Run main function
main "$@"