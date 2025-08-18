#!/bin/sh
# Infrastructure Management Container Entrypoint
# Handles cluster initialization and ongoing infrastructure operations

set -e

# Configuration with defaults
COORDINATOR_HOST="${COORDINATOR_HOST:-app-coordinator}"
COORDINATOR_PORT="${COORDINATOR_PORT:-9090}"
STARTUP_DELAY="${STARTUP_DELAY:-5}"
RETRY_INTERVAL="${RETRY_INTERVAL:-2}"
MAX_RETRIES="${MAX_RETRIES:-10}"
WORKER_IDS="${WORKER_IDS:-app-worker}"

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

# Function to execute custom scripts
run_custom_scripts() {
    if [ -d /scripts/custom ]; then
        log_info "Looking for custom scripts in /scripts/custom..."
        for script in /scripts/custom/*.sh; do
            if [ -f "$script" ]; then
                log_info "Executing custom script: $(basename $script)"
                sh "$script" || log_warn "Custom script $(basename $script) failed with exit code $?"
            fi
        done
    fi
}

# Main execution
main() {
    log_info "Infrastructure management container starting..."
    log_info "Configuration:"
    log_info "  Coordinator: ${COORDINATOR_HOST}:${COORDINATOR_PORT}"
    log_info "  Workers: ${WORKER_IDS}"
    log_info "  Startup delay: ${STARTUP_DELAY}s"

    # Install required tools if not present
    if ! command -v curl >/dev/null 2>&1; then
        log_info "Installing required tools..."
        apk add --no-cache curl jq
    fi

    # Setup Kafka topic
    create_kafka_topics

    # Wait for coordinator to be ready
    log_info "Waiting ${STARTUP_DELAY}s for coordinator to stabilize..."
    sleep ${STARTUP_DELAY}

    # Add all configured workers
    success=true
    for worker in $(echo ${WORKER_IDS} | tr "," " "); do
        if ! add_worker "${worker}"; then
            success=false
        fi
    done

    if [ "$success" = true ]; then
        log_info "All workers added successfully"
    else
        log_error "Some workers failed to add"
        # Don't exit - keep running for other tasks
    fi

    # Run any initialization scripts
    run_custom_scripts

    log_info "Initialization complete. Container staying alive for future tasks..."

    # Keep container alive, checking for new scripts periodically
    while true; do
        sleep 3600
        # Could add periodic tasks here
    done
}

# Run main function
main "$@"