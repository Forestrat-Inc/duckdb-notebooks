#!/bin/bash

# Forestrat MCP Server Restart Script
# Usage: ./restart_server.sh [server_file] [--dev]

echo "🔄 Forestrat MCP Server Restart Script"
echo "======================================"

# Parse arguments
SERVER_FILE="main_fixed.py"
DEV_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dev|--development)
            DEV_MODE=true
            shift
            ;;
        *.py)
            SERVER_FILE="$1"
            shift
            ;;
        *)
            if [[ "$1" != --* ]]; then
                SERVER_FILE="$1"
            fi
            shift
            ;;
    esac
done

# Check if server file exists
if [ ! -f "$SERVER_FILE" ]; then
    echo "❌ Error: Server file '$SERVER_FILE' not found!"
    echo "Available server files:"
    ls -1 main*.py 2>/dev/null || echo "No main*.py files found"
    exit 1
fi

echo "📋 Using server file: $SERVER_FILE"
if [ "$DEV_MODE" = true ]; then
    echo "🛠️  Mode: Development"
else
    echo "🏭 Mode: Production"
fi

# Function to find and kill existing server processes
kill_existing_servers() {
    echo "🔍 Checking for existing MCP server processes..."
    
    # Find processes running the MCP server
    PIDS=$(pgrep -f "$SERVER_FILE" || true)
    
    if [ -n "$PIDS" ]; then
        echo "🛑 Found existing server processes: $PIDS"
        
        # Show what mode the existing servers are running in
        for PID in $PIDS; do
            CMDLINE=$(ps -p $PID -o command= 2>/dev/null || true)
            if echo "$CMDLINE" | grep -q "\-\-dev\|\-\-development"; then
                echo "   PID $PID: Development mode"
            else
                echo "   PID $PID: Production mode"
            fi
        done
        
        echo "🔪 Killing existing processes..."
        
        # Kill processes gracefully first
        echo "$PIDS" | xargs kill -TERM 2>/dev/null || true
        sleep 2
        
        # Force kill if still running
        REMAINING=$(pgrep -f "$SERVER_FILE" || true)
        if [ -n "$REMAINING" ]; then
            echo "💀 Force killing remaining processes: $REMAINING"
            echo "$REMAINING" | xargs kill -KILL 2>/dev/null || true
        fi
        
        echo "✅ Existing processes terminated"
    else
        echo "ℹ️  No existing server processes found"
    fi
}

# Function to start the server
start_server() {
    echo "🚀 Starting MCP server..."
    echo "📝 Server will log to: forestrat_mcp_server.log"
    echo "🔧 To stop the server, use: Ctrl+C or ./stop_server.sh"
    echo ""
    echo "Starting in 3 seconds..."
    sleep 1
    echo "2..."
    sleep 1  
    echo "1..."
    sleep 1
    echo "🎯 Server starting now!"
    echo ""
    
    # Start the server with appropriate flags
    if [ "$DEV_MODE" = true ]; then
        echo "🛠️  Starting in development mode..."
        python3 "$SERVER_FILE" --dev
    else
        echo "🏭 Starting in production mode..."
    python3 "$SERVER_FILE"
    fi
}

# Main execution
echo ""
kill_existing_servers
echo ""
start_server 