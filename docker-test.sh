#!/bin/bash
# Docker-based test runner for InfluxDB 3 plugins
# This script uses Docker Compose to run tests in a containerized environment

set -e

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_usage() {
    echo "Docker-based test runner for InfluxDB 3 plugins"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  all              Test all plugins in influxdata organization"
    echo "  plugin <path>    Test a specific plugin (e.g., influxdata/basic_transformation)"
    echo "  toml <path>      Test a plugin with TOML configuration"
    echo "  shell            Start a test container with interactive shell"
    echo "  clean            Stop and remove all test containers"
    echo ""
    echo "Examples:"
    echo "  $0 all"
    echo "  $0 plugin influxdata/basic_transformation"
    echo "  $0 toml influxdata/basic_transformation basic_transformation.py --toml-config config_scheduler.toml"
    echo "  $0 shell"
    echo "  $0 clean"
    echo ""
    echo "Environment variables:"
    echo "  PLUGIN_PATH     Plugin path (for 'plugin' command)"
    echo "  PLUGIN_FILE     Plugin filename (for 'toml' command)"
    echo "  TOML_CONFIG     TOML config file (for 'toml' command)"
    echo "  PACKAGES        Python packages to install (default: pint)"
}

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Build the test image if needed
build_test_image() {
    print_status "Building test Docker image..."
    docker compose build plugin-tester
}

# Ensure InfluxDB is running
ensure_influxdb() {
    print_status "Starting InfluxDB 3 Core..."
    docker compose up -d influxdb3-core
    
    # Wait for InfluxDB to be ready
    print_status "Waiting for InfluxDB to be ready..."
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if docker compose exec influxdb3-core curl -f http://localhost:8181/health >/dev/null 2>&1; then
            print_status "InfluxDB is ready!"
            return 0
        fi
        ((attempt++))
        echo -n "."
        sleep 2
    done
    
    print_error "InfluxDB failed to start"
    return 1
}

# Main command handling
case "$1" in
    all)
        build_test_image
        ensure_influxdb
        print_status "Testing all influxdata plugins..."
        docker compose run --rm test-all
        ;;
        
    plugin)
        if [ -z "$2" ]; then
            print_error "Plugin path required"
            print_usage
            exit 1
        fi
        build_test_image
        ensure_influxdb
        print_status "Testing plugin: $2"
        PLUGIN_PATH="$2" docker compose run --rm test-specific
        ;;
        
    toml)
        if [ -z "$2" ] || [ -z "$3" ]; then
            print_error "Plugin path and filename required"
            print_usage
            exit 1
        fi
        build_test_image
        ensure_influxdb
        
        # Parse additional arguments
        PLUGIN_PATH="$2"
        PLUGIN_FILE="$3"
        shift 3
        
        # Extract TOML config if provided
        TOML_CONFIG=""
        PACKAGES="pint"
        
        while [[ $# -gt 0 ]]; do
            case $1 in
                --toml-config)
                    TOML_CONFIG="$2"
                    shift 2
                    ;;
                --packages)
                    PACKAGES="$2"
                    shift 2
                    ;;
                *)
                    shift
                    ;;
            esac
        done
        
        print_status "Testing plugin with TOML configuration..."
        print_status "Plugin: $PLUGIN_PATH/$PLUGIN_FILE"
        [ -n "$TOML_CONFIG" ] && print_status "Config: $TOML_CONFIG"
        
        PLUGIN_PATH="$PLUGIN_PATH" PLUGIN_FILE="$PLUGIN_FILE" TOML_CONFIG="$TOML_CONFIG" PACKAGES="$PACKAGES" \
            docker compose run --rm test-toml
        ;;
        
    shell)
        build_test_image
        ensure_influxdb
        print_status "Starting interactive shell in test container..."
        docker compose run --rm --entrypoint /bin/bash plugin-tester
        ;;
        
    clean)
        print_status "Stopping and removing test containers..."
        docker compose down -v
        print_status "Cleanup complete"
        ;;
        
    *)
        if [ -n "$1" ] && [ "$1" != "-h" ] && [ "$1" != "--help" ]; then
            print_error "Unknown command: $1"
            echo ""
        fi
        print_usage
        exit 1
        ;;
esac