#!/bin/bash

# Forestrat MCP Server Status Script
# Usage: ./server_status.sh [server_file]

echo "📊 Forestrat MCP Server Status"
echo "==============================="

# Default server file
SERVER_FILE="${1:-main_fixed.py}"

echo "📋 Checking status for: $SERVER_FILE"
echo ""

# Function to show detailed process status
show_detailed_status() {
    PIDS=$(pgrep -f "$SERVER_FILE" || true)
    
    if [ -n "$PIDS" ]; then
        echo "🟢 Server Status: RUNNING"
        echo "📍 Process IDs: $PIDS"
        echo ""
        echo "📋 Process Details:"
        ps -p $PIDS || true
        echo ""
        echo "🕐 Process Start Times:"
        ps -p $PIDS -o pid,lstart 2>/dev/null || ps -p $PIDS || true
        echo ""
        
        # Check if log file exists and show recent entries
        if [ -f "forestrat_mcp_server.log" ]; then
            echo "📝 Recent Log Entries (last 10 lines):"
            echo "----------------------------------------"
            tail -10 forestrat_mcp_server.log
            echo "----------------------------------------"
            echo "📂 Full log file: forestrat_mcp_server.log"
        else
            echo "📝 Log file not found: forestrat_mcp_server.log"
        fi
        
    else
        echo "🔴 Server Status: STOPPED"
        echo "ℹ️  No processes found for $SERVER_FILE"
        
        # Check if log file exists
        if [ -f "forestrat_mcp_server.log" ]; then
            echo ""
            echo "📝 Last Log Entries (last 5 lines):"
            echo "------------------------------------"
            tail -5 forestrat_mcp_server.log
            echo "------------------------------------"
        fi
    fi
}

# Function to show system resources
show_system_info() {
    echo ""
    echo "💻 System Information:"
    echo "----------------------"
    echo "🖥️  Host: $(hostname)"
    echo "👤 User: $(whoami)"
    echo "📁 Directory: $(pwd)"
    echo "🐍 Python: $(python3 --version 2>/dev/null || echo 'Not found')"
    echo "💾 Memory: $(vm_stat 2>/dev/null | head -5 | tail -1 | awk '{print $3}' || echo 'N/A')"
    echo "⏰ Current Time: $(date)"
}

# Function to show available server files
show_available_servers() {
    echo ""
    echo "📂 Available Server Files:"
    echo "--------------------------"
    if ls main*.py >/dev/null 2>&1; then
        for file in main*.py; do
            if [ -f "$file" ]; then
                SIZE=$(ls -lh "$file" | awk '{print $5}')
                MODIFIED=$(ls -l "$file" | awk '{print $6, $7, $8}')
                if [ "$file" = "$SERVER_FILE" ]; then
                    echo "👉 $file ($SIZE) - Modified: $MODIFIED [CURRENT]"
                else
                    echo "   $file ($SIZE) - Modified: $MODIFIED"
                fi
            fi
        done
    else
        echo "❌ No main*.py files found"
    fi
}

# Function to show quick commands
show_quick_commands() {
    echo ""
    echo "🚀 Quick Commands:"
    echo "------------------"
    echo "Start/Restart: ./restart_server.sh"
    echo "Stop Server:   ./stop_server.sh"
    echo "Check Status:  ./server_status.sh"
    echo "View Logs:     tail -f forestrat_mcp_server.log"
    echo "Test Server:   python3 test_activity_fixed.py"
}

# Main execution
show_detailed_status
show_system_info
show_available_servers
show_quick_commands
echo ""
echo "✅ Status check completed" 