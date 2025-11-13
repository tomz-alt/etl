#!/bin/bash

# Script to run Apache Doris integration tests
# This script starts a Doris instance using Docker Compose,
# waits for it to be ready, runs the integration tests,
# and then cleans up.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-doris.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Apache Doris Integration Test Runner${NC}"
echo -e "${GREEN}========================================${NC}"
echo

# Function to cleanup on exit
cleanup() {
    echo
    echo -e "${YELLOW}Cleaning up...${NC}"
    docker-compose -f "$COMPOSE_FILE" down -v
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Register cleanup function
trap cleanup EXIT

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: docker-compose is not installed${NC}"
    echo "Please install docker-compose to run integration tests"
    exit 1
fi

# Start Doris
echo -e "${YELLOW}Starting Apache Doris...${NC}"
docker-compose -f "$COMPOSE_FILE" up -d

# Wait for Doris FE to be healthy
echo -e "${YELLOW}Waiting for Doris Frontend to be ready...${NC}"
max_attempts=60
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if docker-compose -f "$COMPOSE_FILE" ps | grep -q "doris-fe.*healthy"; then
        echo -e "${GREEN}Doris Frontend is ready!${NC}"
        break
    fi

    attempt=$((attempt + 1))
    if [ $attempt -eq $max_attempts ]; then
        echo -e "${RED}Error: Doris Frontend did not become healthy in time${NC}"
        docker-compose -f "$COMPOSE_FILE" logs doris-fe
        exit 1
    fi

    echo -n "."
    sleep 2
done

# Wait a bit more for Backend to fully initialize
echo -e "${YELLOW}Waiting for Doris Backend to initialize...${NC}"
sleep 10

# Create test database
echo -e "${YELLOW}Creating test database...${NC}"
docker exec doris-fe-test mysql -h127.0.0.1 -P9030 -uroot -e "CREATE DATABASE IF NOT EXISTS test_db;" || true

# Show Doris status
echo -e "${YELLOW}Doris cluster status:${NC}"
docker-compose -f "$COMPOSE_FILE" ps

# Run integration tests
echo
echo -e "${GREEN}Running integration tests...${NC}"
cd "$SCRIPT_DIR/.."

# Set environment variables for tests
export DORIS_HOST=localhost
export DORIS_QUERY_PORT=9030
export DORIS_HTTP_PORT=8030
export DORIS_DATABASE=test_db
export DORIS_USER=root
export DORIS_PASSWORD=""

# Run the tests
if cargo test --package etl-destinations --features doris --test doris_integration -- --ignored --test-threads=1; then
    echo
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}All integration tests passed! ✓${NC}"
    echo -e "${GREEN}========================================${NC}"
    exit 0
else
    echo
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}Integration tests failed ✗${NC}"
    echo -e "${RED}========================================${NC}"
    echo
    echo -e "${YELLOW}Doris FE logs:${NC}"
    docker-compose -f "$COMPOSE_FILE" logs --tail=50 doris-fe
    echo
    echo -e "${YELLOW}Doris BE logs:${NC}"
    docker-compose -f "$COMPOSE_FILE" logs --tail=50 doris-be
    exit 1
fi
