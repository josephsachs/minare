#!/bin/bash
set -e

echo "Starting Minare Application..."

# Check if we have the required JAR file
if [ ! -f "/app/app.jar" ]; then
    echo "ERROR: Application JAR not found at /app/app.jar"
    echo "Please ensure the JAR is mounted as a volume:"
    echo "  volumes:"
    echo "    - ./minare-example/target/minare-example-1.0-SNAPSHOT-fat.jar:/app/app.jar:ro"
    exit 1
fi

# Function to test NTP capabilities
# test_ntp_capability() {
#    echo "Testing NTP/time adjustment capabilities..."

    # Check if we have SYS_TIME capability
#    if capsh --print | grep -q "Current:.*cap_sys_time"; then
#        echo "✓ SYS_TIME capability detected"
#        return 0
#    else
#        echo "⚠ SYS_TIME capability not detected"
#        echo "  Time adjustment may not work in this container"
#        echo "  Add 'cap_add: [SYS_TIME]' to docker-compose.yaml"
#        return 1
#    fi
#}

# Function to perform initial NTP sync (if capabilities allow)
#sync_time() {
#    echo "Attempting initial NTP synchronization..."

    # Use ntpd for time sync if we have capabilities
#    if test_ntp_capability; then
        # Try to sync time using ntpd
#        if command -v ntpd >/dev/null 2>&1; then
#            echo "Using ntpd for time synchronization..."
            # Run ntpd once to sync time, then exit
#            timeout 30s ntpd -n -q -p pool.ntp.org || {
#                echo "⚠ NTP sync failed or timed out"
#                echo "  Continuing with application startup..."
#            }
#        else
#            echo "⚠ ntpd not available, skipping time sync"
#        fi
#    else
#        echo "⚠ Cannot adjust system time without SYS_TIME capability"
#        echo "  Application will start with current container time"
#    fi
#}

# Function to setup Java options
setup_java_opts() {
    # Default Java options for containerized environment
    DEFAULT_JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"

    # Add debug options if enabled
    if [ "${JAVA_DEBUG:-false}" = "true" ]; then
        DEBUG_PORT="${JAVA_DEBUG_PORT:-5005}"
        DEBUG_SUSPEND="${JAVA_DEBUG_SUSPEND:-y}"
        DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND},address=*:${DEBUG_PORT}"

        echo "🐛 Debug mode enabled:"
        echo "  Port: ${DEBUG_PORT}"
        echo "  Suspend: ${DEBUG_SUSPEND}"
        if [ "${DEBUG_SUSPEND}" = "y" ]; then
            echo "  ⏸️  Application will wait for debugger to connect"
        else
            echo "  ▶️  Application will start, debugger can attach anytime"
        fi

        DEFAULT_JAVA_OPTS="${DEFAULT_JAVA_OPTS} ${DEBUG_OPTS}"
    fi

    # Combine with user-provided options
    export JAVA_OPTS="${JAVA_OPTS:-} ${DEFAULT_JAVA_OPTS}"

    echo "Java options: ${JAVA_OPTS}"
}

# Function to wait for dependencies
wait_for_dependencies() {
    echo "Waiting for dependencies..."

    # Wait for MongoDB
    if [ -n "${MONGO_URI}" ]; then
        echo "Waiting for MongoDB..."
        # Extract host and port from URI
        MONGO_HOST=$(echo "${MONGO_URI}" | sed -n 's/.*\/\/\([^:]*\):.*/\1/p')
        MONGO_PORT=$(echo "${MONGO_URI}" | sed -n 's/.*:\([0-9]*\)\/.*/\1/p')

        timeout 60s bash -c "until curl -s ${MONGO_HOST}:${MONGO_PORT} >/dev/null 2>&1; do sleep 2; done" || {
            echo "⚠ MongoDB connection timeout"
        }
    fi

    # Wait for Redis
    # if [ -n "${REDIS_URI}" ]; then
    #    echo "Waiting for Redis..."
    #    REDIS_HOST=$(echo "${REDIS_URI}" | sed -n 's/.*\/\/\([^:]*\):.*/\1/p')
    #    REDIS_PORT=$(echo "${REDIS_URI}" | sed -n 's/.*:\([0-9]*\).*/\1/p')

    #    timeout 30s bash -c "until curl -s ${REDIS_HOST}:${REDIS_PORT} >/dev/null 2>&1; do sleep 2; done" || {
    #        echo "⚠ Redis connection timeout"
    #    }
    #fi

    echo "✓ Dependency checks complete"
}

# Main startup sequence
main() {
    echo "==========================================="
    echo "Minare Application Container Starting"
    echo "Time: $(date)"
    echo "==========================================="

    # Perform NTP sync
    # sync_time

    # Setup Java environment
    setup_java_opts

    # Wait for external dependencies
    wait_for_dependencies

    echo "Starting application..."
    echo "Command: $*"

    # Execute the provided command (typically: java -jar /app/app.jar)
    exec "$@"
}

# Handle signals for graceful shutdown
trap 'echo "Received shutdown signal, stopping application..."; exit 0' SIGTERM SIGINT

# Run main function with all provided arguments
main "$@"