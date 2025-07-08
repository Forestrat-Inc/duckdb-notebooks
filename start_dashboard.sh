#!/bin/bash

# Market Data Dashboard Startup Script
echo "🚀 Starting Market Data Dashboard..."

# Check if virtual environment is activated
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✅ Virtual environment active: $VIRTUAL_ENV"
else
    echo "⚠️  Virtual environment not detected. Activating..."
    source venv/bin/activate
fi

# Check if required packages are installed
python3 -c "import flask" 2>/dev/null || {
    echo "❌ Flask not found. Installing..."
    pip install flask
}

# Create logs directory if it doesn't exist
mkdir -p logs

# Kill any existing dashboard process
pkill -f "python.*dashboard_app.py" 2>/dev/null && echo "🛑 Stopped existing dashboard process"

# Start the dashboard
echo "🌐 Starting dashboard on http://localhost:12345"
echo "📝 Logs will be written to logs/dashboard.log"

# Run in background with nohup
nohup python3 dashboard_app.py > logs/dashboard.log 2>&1 &

# Get the process ID
DASHBOARD_PID=$!
echo "📋 Dashboard PID: $DASHBOARD_PID"

# Wait a moment for startup
sleep 3

# Check if process is still running
if ps -p $DASHBOARD_PID > /dev/null; then
    echo "✅ Dashboard started successfully!"
    echo "🌐 Access at: http://localhost:12345"
    echo "📝 View logs: tail -f logs/dashboard.log"
    echo "🛑 Stop with: pkill -f dashboard_app.py"
else
    echo "❌ Dashboard failed to start. Check logs/dashboard.log for details"
    exit 1
fi 