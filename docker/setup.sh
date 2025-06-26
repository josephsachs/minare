#!/bin/bash
# Development setup script for Minare Docker environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"

echo "Minare Docker Development Setup"
echo "==============================="

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
check_prerequisites() {
    echo "Checking prerequisites..."

    if ! command_exists docker; then
        echo "❌ Docker not found. Please install Docker Desktop."
        exit 1
    fi

    if ! command_exists docker-compose; then
        echo "❌ docker-compose not found. Please install docker-compose."
        exit 1
    fi

    if ! command_exists mvn; then
        echo "❌ Maven not found. Please install Maven."
        exit 1
    fi

    echo "✓ All prerequisites found"
}

# Create required directories
setup_directories() {
    echo "Creating required directories..."

    cd "${SCRIPT_DIR}"

    # Create directories if they don't exist
    mkdir -p data/db
    mkdir -p data/redis
    mkdir -p logs
    mkdir -p logs2
    mkdir -p haproxy

    echo "✓ Directories created"
}

# Build the application
build_application() {
    echo "Building application..."

    cd "${PROJECT_ROOT}"

    # Clean and build the fat JAR
    mvn clean package -DskipTests

    if [ ! -f "minare-example/target/minare-app-fat.jar" ]; then
        echo "❌ Build failed - JAR not found"
        exit 1
    fi

    echo "✓ Application built successfully"
}

# Setup HAProxy configuration
setup_haproxy() {
    echo "Setting up HAProxy configuration..."

    # The haproxy.cfg is already created as an artifact
    # Just ensure the directory exists
    mkdir -p "${SCRIPT_DIR}/haproxy"

    echo "✓ HAProxy directory ready"
    echo "  Copy the haproxy.cfg artifact to docker/haproxy/haproxy.cfg"
}

# Start the environment
start_environment() {
    echo "Starting Docker environment..."

    cd "${SCRIPT_DIR}"

    # Start services
    docker-compose up -d mongodb redis

    echo "Waiting for services to be healthy..."

    # Wait for MongoDB and Redis to be healthy
    timeout 120s bash -c 'until docker-compose ps | grep -E "(mongodb|redis)" | grep -q "healthy"; do sleep 5; done'

    echo "✓ Core services started"

    # Build and start application
    docker-compose build app
    docker-compose up -d app haproxy

    echo "✓ Application and proxy started"
}

# Show status and useful commands
show_status() {
    echo ""
    echo "🎉 Setup complete!"
    echo ""
    echo "Services:"
    echo "  • Application: http://localhost:8080"
    echo "  • HAProxy Stats: http://localhost:8404/stats"
    echo "  • MongoDB: localhost:27017"
    echo "  • Redis: localhost:6379"
    echo ""
    echo "WebSocket Endpoints:"
    echo "  • Up Socket (commands): ws://localhost:4225/command"
    echo "  • Down Socket (updates): ws://localhost:4226/update"
    echo ""
    echo "Debug Modes:"
    echo "  • Enable debug: JAVA_DEBUG=true docker-compose restart app"
    echo "  • Debug port: localhost:5005 (configurable with JAVA_DEBUG_PORT)"
    echo "  • Suspend modes:"
    echo "    - JAVA_DEBUG_SUSPEND=y (wait for debugger, good for clustering/startup)"
    echo "    - JAVA_DEBUG_SUSPEND=n (attach anytime, good for runtime issues)"
    echo ""
    echo "Useful commands:"
    echo "  • View logs: docker-compose logs -f app"
    echo "  • Restart app: docker-compose restart app"
    echo "  • Scale app: docker-compose up -d --scale app=2"
    echo "  • Stop all: docker-compose down"
    echo "  • Rebuild after code changes:"
    echo "    mvn package -DskipTests && docker-compose restart app"
    echo ""
}

# Main setup process
main() {
    check_prerequisites
    setup_directories
    build_application
    setup_haproxy
    start_environment
    show_status
}

# Handle script arguments
case "${1:-setup}" in
    "setup")
        main
        ;;
    "build")
        build_application
        echo "✓ Application rebuilt. Run 'docker-compose restart app' to reload."
        ;;
    "start")
        cd "${SCRIPT_DIR}"
        docker-compose up -d
        ;;
    "stop")
        cd "${SCRIPT_DIR}"
        docker-compose down
        ;;
    "logs")
        cd "${SCRIPT_DIR}"
        docker-compose logs -f "${2:-app}"
        ;;
    "restart")
        cd "${SCRIPT_DIR}"
        docker-compose restart "${2:-app}"
        ;;
    *)
        echo "Usage: $0 [setup|build|start|stop|logs|restart]"
        echo ""
        echo "Commands:"
        echo "  setup   - Full environment setup (default)"
        echo "  build   - Rebuild application JAR only"
        echo "  start   - Start all services"
        echo "  stop    - Stop all services"
        echo "  logs    - View application logs"
        echo "  restart - Restart application"
        exit 1
        ;;
esac