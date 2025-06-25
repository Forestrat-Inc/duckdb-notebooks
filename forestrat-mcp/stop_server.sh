#!/bin/bash

# Forestrat MCP Server Stop Script
# Usage: ./stop_server.sh [server_file]

echo "🛑 Forestrat MCP Server Stop Script"
echo "===================================="

# Default server file
SERVER_FILE="${1:-main_fixed.py}"

echo "📋 Looking for server processes: $SERVER_FILE"

# Function to find and kill existing server processes
kill_server_processes() {
    echo "🔍 Checking for MCP server processes..."
    
    # Find processes running the MCP server
    PIDS=$(pgrep -f "$SERVER_FILE" || true)
    
    if [ -n "$PIDS" ]; then
        echo "🛑 Found server processes: $PIDS"
        echo "🔪 Stopping server processes..."
        
        # Kill processes gracefully first
        echo "$PIDS" | xargs kill -TERM 2>/dev/null || true
        sleep 2
        
        # Check if processes are still running
        REMAINING=$(pgrep -f "$SERVER_FILE" || true)
        if [ -n "$REMAINING" ]; then
            echo "💀 Force killing remaining processes: $REMAINING"
            echo "$REMAINING" | xargs kill -KILL 2>/dev/null || true
            sleep 1
        fi
        
        # Final check
        FINAL_CHECK=$(pgrep -f "$SERVER_FILE" || true)
        if [ -z "$FINAL_CHECK" ]; then
            echo "✅ Server processes stopped successfully"
        else
            echo "❌ Some processes may still be running: $FINAL_CHECK"
            exit 1
        fi
    else
        echo "ℹ️  No server processes found"
    fi
}

# Function to show process status
show_status() {
    echo ""
    echo "📊 Current MCP server process status:"
    CURRENT_PIDS=$(pgrep -f "$SERVER_FILE" || true)
    if [ -n "$CURRENT_PIDS" ]; then
        echo "🟢 Running processes:"
        ps -p $CURRENT_PIDS || true
    else
        echo "🔴 No MCP server processes running"
    fi
}

# Main execution
echo ""
kill_server_processes
show_status
echo ""
echo "🏁 Stop script completed" 