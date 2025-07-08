#!/bin/bash

# Start Dashboard for January 2025 Data Loading
# This script starts the web dashboard for monitoring data loading progress

echo "🚀 Starting January 2025 Data Loading Dashboard..."
echo "=================================================="

# Check if Python is available
if ! command -v python &> /dev/null; then
    echo "❌ Python is not installed or not in PATH"
    exit 1
fi

# Check if required packages are available
echo "📦 Checking dependencies..."
python -c "import flask" 2>/dev/null || {
    echo "❌ Flask is not installed. Installing required packages..."
    pip install flask flask-cors
}

# Check if database file exists
if [ ! -f "../multi_exchange_data_lake.duckdb" ]; then
    echo "⚠️  Database file not found: ../multi_exchange_data_lake.duckdb"
    echo "   Run the data loading script first to create the database"
    echo "   Example: python ../load_january_simple.py --idempotent"
    echo ""
fi

# Create logs directory if it doesn't exist
mkdir -p ../logs

# Start the dashboard
echo "🌐 Starting dashboard server..."
echo "   Dashboard URL: http://localhost:12345"
echo "   Press Ctrl+C to stop the dashboard"
echo ""
echo "📊 Dashboard Features:"
echo "   • Real-time progress monitoring"
echo "   • Interactive charts and statistics"
echo "   • Shutdown/resume controls"
echo "   • Error tracking and analysis"
echo ""
echo "🔧 Control Options:"
echo "   • Use dashboard buttons to shutdown/resume script"
echo "   • Monitor progress in real-time"
echo "   • View detailed statistics and performance metrics"
echo ""
echo "=================================================="

# Start the dashboard application
python dashboard_app.py 