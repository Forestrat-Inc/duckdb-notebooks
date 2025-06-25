#!/bin/bash

# Forestrat MCP Server Restart Script
# Usage: ./restart_server.sh [server_file]

echo "🔄 Forestrat MCP Server Restart Script"
echo "======================================"

# Default server file
SERVER_FILE="${1:-main_fixed.py}"

# Check if server file exists
if [ ! -f "$SERVER_FILE" ]; then
    echo "❌ Error: Server file '$SERVER_FILE' not found!"
    echo "Available server files:"
    ls -1 main*.py 2>/dev/null || echo "No main*.py files found"
    exit 1
fi

echo "📋 Using server file: $SERVER_FILE"

# Function to find and kill existing server processes
kill_existing_servers() {
    echo "🔍 Checking for existing MCP server processes..."
    
    # Find processes running the MCP server
    PIDS=$(pgrep -f "$SERVER_FILE" || true)
    
    if [ -n "$PIDS" ]; then
        echo "🛑 Found existing server processes: $PIDS"
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
    
    # Start the server
    python3 "$SERVER_FILE"
}

# Main execution
echo ""
kill_existing_servers
echo ""
start_server 