#!/bin/bash

# NiFi API Helper Script for Market Data Loading
# Usage: ./nifi-api-helper.sh [command] [arguments]

NIFI_URL="http://localhost:8080/nifi-api"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Get parameter context ID
get_context_id() {
    curl -s $NIFI_URL/parameter-contexts | jq -r '.parameterContexts[] | select(.component.name=="market-data-params") | .id'
}

# Get processor ID
get_processor_id() {
    curl -s $NIFI_URL/process-groups/root/processors | jq -r '.processors[] | select(.component.name=="Load January Data") | .id'
}

# Get current revision for parameter context
get_context_revision() {
    local context_id=$1
    curl -s $NIFI_URL/parameter-contexts/$context_id | jq -r '.revision.version'
}

# Get current revision for processor
get_processor_revision() {
    local processor_id=$1
    curl -s $NIFI_URL/processors/$processor_id | jq -r '.revision.version'
}

# List all parameters
list_parameters() {
    log_info "Fetching current parameters..."
    local context_id=$(get_context_id)
    if [ -z "$context_id" ]; then
        log_error "Parameter context 'market-data-params' not found"
        return 1
    fi
    
    echo -e "\n${GREEN}Current Parameters:${NC}"
    curl -s $NIFI_URL/parameter-contexts/$context_id | jq -r '.component.parameters[] | "  \(.parameter.name) = \(.parameter.value)"'
}

# Update TARGET_DATE parameter
update_date() {
    local new_date=$1
    if [ -z "$new_date" ]; then
        log_error "Please provide a date (YYYY-MM-DD format)"
        echo "Usage: $0 update_date 2025-01-20"
        return 1
    fi
    
    log_info "Updating TARGET_DATE to $new_date..."
    local context_id=$(get_context_id)
    local revision=$(get_context_revision $context_id)
    
    local response=$(curl -s -X PUT $NIFI_URL/parameter-contexts/$context_id \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": '$revision'},
            "component": {
                "id": "'$context_id'",
                "name": "market-data-params",
                "parameters": [
                    {
                        "parameter": {
                            "name": "TARGET_DATE",
                            "value": "'$new_date'",
                            "sensitive": false
                        }
                    }
                ]
            }
        }')
    
    if echo "$response" | jq -e '.component' > /dev/null 2>&1; then
        log_success "TARGET_DATE updated to $new_date"
    else
        log_error "Failed to update TARGET_DATE"
        echo "$response" | jq -r '.message // empty'
    fi
}

# Update EXCHANGES parameter
update_exchanges() {
    local new_exchanges="$1"
    if [ -z "$new_exchanges" ]; then
        log_error "Please provide exchanges (e.g., 'LSE CME NYQ')"
        echo "Usage: $0 update_exchanges 'LSE CME'"
        return 1
    fi
    
    log_info "Updating EXCHANGES to '$new_exchanges'..."
    local context_id=$(get_context_id)
    local revision=$(get_context_revision $context_id)
    
    local response=$(curl -s -X PUT $NIFI_URL/parameter-contexts/$context_id \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": '$revision'},
            "component": {
                "id": "'$context_id'",
                "name": "market-data-params",
                "parameters": [
                    {
                        "parameter": {
                            "name": "EXCHANGES",
                            "value": "'$new_exchanges'",
                            "sensitive": false
                        }
                    }
                ]
            }
        }')
    
    if echo "$response" | jq -e '.component' > /dev/null 2>&1; then
        log_success "EXCHANGES updated to '$new_exchanges'"
    else
        log_error "Failed to update EXCHANGES"
        echo "$response" | jq -r '.message // empty'
    fi
}

# Get processor status
get_status() {
    log_info "Fetching processor status..."
    local processor_id=$(get_processor_id)
    if [ -z "$processor_id" ]; then
        log_error "Processor 'Load January Data' not found"
        return 1
    fi
    
    local status=$(curl -s $NIFI_URL/processors/$processor_id | jq -r '.component.state')
    local run_status=$(curl -s $NIFI_URL/processors/$processor_id | jq -r '.status.runStatus')
    
    echo -e "\n${GREEN}Processor Status:${NC}"
    echo "  State: $status"
    echo "  Run Status: $run_status"
    
    # Get execution stats
    local stats=$(curl -s $NIFI_URL/processors/$processor_id | jq -r '.status.aggregateSnapshot')
    echo -e "\n${GREEN}Execution Stats:${NC}"
    echo "  Tasks Completed: $(echo $stats | jq -r '.tasksCompleted')"
    echo "  Tasks Failed: $(echo $stats | jq -r '.tasksFailed')"
    echo "  Average Duration: $(echo $stats | jq -r '.averageDuration')"
}

# Stop processor
stop_processor() {
    log_info "Stopping processor..."
    local processor_id=$(get_processor_id)
    local revision=$(get_processor_revision $processor_id)
    
    local response=$(curl -s -X PUT $NIFI_URL/processors/$processor_id \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": '$revision'},
            "component": {
                "id": "'$processor_id'",
                "state": "STOPPED"
            }
        }')
    
    if echo "$response" | jq -e '.component' > /dev/null 2>&1; then
        log_success "Processor stopped"
    else
        log_error "Failed to stop processor"
    fi
}

# Start processor
start_processor() {
    log_info "Starting processor..."
    local processor_id=$(get_processor_id)
    local revision=$(get_processor_revision $processor_id)
    
    local response=$(curl -s -X PUT $NIFI_URL/processors/$processor_id \
        -H "Content-Type: application/json" \
        -d '{
            "revision": {"version": '$revision'},
            "component": {
                "id": "'$processor_id'",
                "state": "RUNNING"
            }
        }')
    
    if echo "$response" | jq -e '.component' > /dev/null 2>&1; then
        log_success "Processor started"
    else
        log_error "Failed to start processor"
    fi
}

# Run with new date (stop, update, start)
run_date() {
    local new_date=$1
    if [ -z "$new_date" ]; then
        log_error "Please provide a date (YYYY-MM-DD format)"
        echo "Usage: $0 run_date 2025-01-20"
        return 1
    fi
    
    log_info "Running data load for date: $new_date"
    
    # Stop processor
    stop_processor
    sleep 2
    
    # Update date
    update_date $new_date
    sleep 1
    
    # Start processor
    start_processor
    
    log_success "Data load initiated for $new_date"
    log_info "Monitor progress with: $0 status"
}

# Show help
show_help() {
    echo -e "${BLUE}NiFi API Helper for Market Data Loading${NC}"
    echo ""
    echo "Usage: $0 [command] [arguments]"
    echo ""
    echo "Commands:"
    echo "  list                     List current parameters"
    echo "  update_date DATE         Update TARGET_DATE parameter"
    echo "  update_exchanges EXCH    Update EXCHANGES parameter"
    echo "  status                   Show processor status"
    echo "  start                    Start processor"
    echo "  stop                     Stop processor"
    echo "  run_date DATE            Stop, update date, and start processor"
    echo "  help                     Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 list"
    echo "  $0 update_date 2025-01-20"
    echo "  $0 update_exchanges 'LSE CME'"
    echo "  $0 run_date 2025-01-25"
    echo "  $0 status"
}

# Main script
case "$1" in
    "list")
        list_parameters
        ;;
    "update_date")
        update_date "$2"
        ;;
    "update_exchanges")
        update_exchanges "$2"
        ;;
    "status")
        get_status
        ;;
    "start")
        start_processor
        ;;
    "stop")
        stop_processor
        ;;
    "run_date")
        run_date "$2"
        ;;
    "help"|"--help"|"-h"|"")
        show_help
        ;;
    *)
        log_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac 