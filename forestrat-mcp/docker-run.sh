#!/bin/bash

# Forestrat MCP Server Docker Management Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CONTAINER_NAME="forestrat-mcp-server"
IMAGE_NAME="forestrat-mcp"
DATABASE_PATH="../multi_exchange_data_lake.duckdb"

# Functions
print_usage() {
    echo "Usage: $0 {build|start|stop|restart|logs|status|shell|dev|clean}"
    echo ""
    echo "Commands:"
    echo "  build    - Build the Docker image"
    echo "  start    - Start the MCP server container"
    echo "  stop     - Stop the MCP server container"
    echo "  restart  - Restart the MCP server container"
    echo "  logs     - Show container logs"
    echo "  status   - Show container status"
    echo "  shell    - Open shell in running container"
    echo "  dev      - Start development container"
    echo "  clean    - Remove container and image"
}

check_database() {
    if [ ! -f "$DATABASE_PATH" ]; then
        echo -e "${RED}Error: Database file not found at $DATABASE_PATH${NC}"
        echo "Please ensure the DuckDB database file exists before starting the container."
        exit 1
    fi
}

build_image() {
    echo -e "${BLUE}Building Docker image...${NC}"
    docker build -t $IMAGE_NAME .
    echo -e "${GREEN}Image built successfully!${NC}"
}

start_container() {
    check_database
    
    echo -e "${BLUE}Starting Forestrat MCP server...${NC}"
    
    # Create logs directory if it doesn't exist
    mkdir -p ./logs
    
    # Stop existing container if running
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    
    # Start new container
    docker run -d \
        --name $CONTAINER_NAME \
        --restart unless-stopped \
        -e DATABASE_PATH=/app/data/multi_exchange_data_lake.duckdb \
        -e LOG_LEVEL=INFO \
        -e PYTHONUNBUFFERED=1 \
        -v "$(pwd)/$DATABASE_PATH:/app/data/multi_exchange_data_lake.duckdb:ro" \
        -v "$(pwd)/logs:/app/logs" \
        -v "$(pwd)/config.py:/app/config.py:ro" \
        $IMAGE_NAME
    
    echo -e "${GREEN}Container started successfully!${NC}"
    echo "Container name: $CONTAINER_NAME"
    echo "Use '$0 logs' to see the logs"
    echo "Use '$0 status' to check status"
}

stop_container() {
    echo -e "${YELLOW}Stopping container...${NC}"
    docker stop $CONTAINER_NAME 2>/dev/null || echo "Container not running"
    docker rm $CONTAINER_NAME 2>/dev/null || echo "Container not found"
    echo -e "${GREEN}Container stopped and removed${NC}"
}

restart_container() {
    stop_container
    start_container
}

show_logs() {
    echo -e "${BLUE}Showing container logs...${NC}"
    docker logs -f $CONTAINER_NAME
}

show_status() {
    echo -e "${BLUE}Container status:${NC}"
    docker ps -a --filter name=$CONTAINER_NAME --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}\t{{.Image}}"
    
    echo -e "\n${BLUE}Health status:${NC}"
    docker inspect $CONTAINER_NAME --format='{{.State.Health.Status}}' 2>/dev/null || echo "Not available"
}

open_shell() {
    echo -e "${BLUE}Opening shell in container...${NC}"
    docker exec -it $CONTAINER_NAME /bin/bash
}

start_dev() {
    check_database
    
    echo -e "${BLUE}Starting development container...${NC}"
    
    # Create logs directory if it doesn't exist
    mkdir -p ./logs
    
    # Stop existing dev container if running
    docker stop forestrat-mcp-dev 2>/dev/null || true
    docker rm forestrat-mcp-dev 2>/dev/null || true
    
    # Start development container with source code mounted
    docker run -it \
        --name forestrat-mcp-dev \
        -e DATABASE_PATH=/app/data/multi_exchange_data_lake.duckdb \
        -e LOG_LEVEL=DEBUG \
        -e PYTHONUNBUFFERED=1 \
        -v "$(pwd)/main_fixed.py:/app/main_fixed.py" \
        -v "$(pwd)/config.py:/app/config.py" \
        -v "$(pwd)/database.py:/app/database.py" \
        -v "$(pwd)/$DATABASE_PATH:/app/data/multi_exchange_data_lake.duckdb" \
        -v "$(pwd)/logs:/app/logs" \
        $IMAGE_NAME /bin/bash
}

clean_all() {
    echo -e "${YELLOW}Cleaning up containers and images...${NC}"
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    docker stop forestrat-mcp-dev 2>/dev/null || true
    docker rm forestrat-mcp-dev 2>/dev/null || true
    docker rmi $IMAGE_NAME 2>/dev/null || true
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Main script logic
case "$1" in
    build)
        build_image
        ;;
    start)
        start_container
        ;;
    stop)
        stop_container
        ;;
    restart)
        restart_container
        ;;
    logs)
        show_logs
        ;;
    status)
        show_status
        ;;
    shell)
        open_shell
        ;;
    dev)
        start_dev
        ;;
    clean)
        clean_all
        ;;
    *)
        print_usage
        exit 1
        ;;
esac

exit 0 