# Simple January 2025 Market Data Loading Guide

## Overview
This guide provides two simple options for loading January 2025 market data with transaction handling and comprehensive statistics tracking:

1. **Enhanced Python Script** (`load_january_simple.py`) - Run directly with statistics
2. **Simple NiFi Process Group** (`Simple-January-2025-Loader.json`) - NiFi 2.4 flow

Both options provide:
- ✅ **Transaction handling** for non-blocking reads
- ✅ **Progress tracking** with detailed statistics
- ✅ **Daily statistics** (files, records, performance)
- ✅ **Weekly rolling statistics** (7-day averages)
- ✅ **Error handling** with rollback support
- ✅ **Real-time monitoring** through database views

---

## Option 1: Enhanced Python Script ⚡ (Recommended for Quick Start)

### Features Added
- **Transaction Support**: Uses `BEGIN/COMMIT/ROLLBACK` for non-blocking reads
- **Progress Tracking**: Real-time status in `bronze.load_progress`
- **Daily Statistics**: Comprehensive metrics in `gold.daily_load_stats` 
- **Weekly Rolling Stats**: 7-day averages in `gold.weekly_load_stats`
- **Performance Metrics**: Processing time, records per second, throughput

### Quick Usage

1. **Run the enhanced script**:
   ```bash
   cd /Users/kaushal/Documents/Forestrat/duckdb
   python load_january_simple.py
   ```

2. **Monitor progress** (in another terminal):
   ```bash
   # View current progress
   duckdb multi_exchange_data_lake.duckdb -c "SELECT * FROM bronze.load_progress ORDER BY start_time DESC LIMIT 10"
   
   # View daily statistics
   duckdb multi_exchange_data_lake.duckdb -c "SELECT * FROM gold.daily_load_stats ORDER BY stats_date, exchange"
   
   # View weekly rolling statistics  
   duckdb multi_exchange_data_lake.duckdb -c "SELECT * FROM gold.weekly_load_stats ORDER BY week_ending DESC"
   ```

### Expected Output
```
2025-01-20 10:15:23 - INFO - Loading LSE data for date: 2025-01-01
2025-01-20 10:15:23 - INFO - Starting transaction for LSE 2025-01-01
2025-01-20 10:15:25 - INFO - ✅ Successfully loaded 45,231 records for LSE 2025-01-01 in 2.34s

================================================================================
DAILY STATISTICS SUMMARY
================================================================================
LSE - 2025-01-01:
  Files: 1 total, 1 successful, 0 failed
  Records: 45,231 total, 45,231 avg per file
  Performance: 2.34s total, 19,327 records/sec

================================================================================
WEEKLY ROLLING STATISTICS
================================================================================
LSE - Week ending 2025-01-07:
  Daily averages: 1.0 files, 45,231 records
  Weekly totals: 7 files, 316,617 records
  Avg processing time: 2.45s
```

---

## Option 2: Simple NiFi Process Group 🔄 (For Workflow Management)

### Features
- **Visual Workflow**: Easy to monitor and modify
- **Same Transaction Logic**: Matches Python script behavior
- **Built-in Error Handling**: Automatic rollback on failures
- **Parallel Processing**: Handles multiple files concurrently
- **Real-time Statistics**: Updates stats as files are processed

### Setup Instructions

1. **Start NiFi**:
   ```bash
   cd /Users/kaushal/Documents/Forestrat/nifi/nifi-2.3.0
   ./bin/nifi.sh start
   ```

2. **Import Template**:
   - Open NiFi UI: http://localhost:8080/nifi
   - Upload template: `nifi/Simple-January-2025-Loader.json`
   - Drag Process Group to canvas
   - Select "Simple-January-2025-Loader"

3. **Configure Controller Services**:
   - Right-click Process Group → Configure → Controller Services
   - Configure AWS credentials (your access key/secret)
   - Configure DuckDB connection pool
   - Enable all services

4. **Set Parameters**:
   - Create Parameter Context: "simple-market-data-params"
   - Set `aws.access.key.id` and `aws.secret.access.key`

5. **Start Process Group**:
   - Right-click → Start
   - Monitor progress in NiFi UI

### Process Flow
```
Generate Dates → Split Dates → Add Exchange Attrs → Split by Exchange → 
Set Exchange Name → Build S3 Path → Start Progress Tracking → 
Begin Transaction → Fetch S3 → Decompress → Load to DB → 
Add Metadata → Commit Transaction → Count Records → Update Progress → 
Update Daily Stats → Log Success
                ↓ (on failure)
             Rollback → Update Progress Failed → Log Failure
```

---

## Database Tables Created

### Progress Tracking
```sql
-- Real-time progress monitoring
bronze.load_progress (
    id, exchange, data_date, file_path, 
    start_time, end_time, status, records_loaded, 
    error_message, created_at
)
```

### Daily Statistics
```sql
-- Daily aggregated statistics
gold.daily_load_stats (
    stats_date, exchange, total_files, successful_files, 
    failed_files, total_records, avg_records_per_file, 
    total_processing_time_seconds
)
```

### Weekly Rolling Statistics
```sql
-- 7-day rolling window analytics
gold.weekly_load_stats (
    week_ending, exchange, avg_daily_files, avg_daily_records, 
    total_files, total_records, avg_processing_time_seconds
)
```

---

## Key Benefits

### 🔒 **Transaction Handling**
- **Non-blocking Reads**: Other processes can read data while loading
- **Atomicity**: Each file load is atomic (all-or-nothing)
- **Rollback Support**: Failed loads don't leave partial data

### 📊 **Comprehensive Statistics**
- **Progress Tracking**: See exactly which files are processing
- **Performance Metrics**: Records/second, processing time, throughput
- **Daily Summaries**: File counts, success rates, record volumes
- **Weekly Trends**: Rolling averages and trend analysis

### 🚀 **Enhanced Monitoring**
```sql
-- Monitor current progress
SELECT exchange, COUNT(*) as files, 
       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
       SUM(records_loaded) as total_records
FROM bronze.load_progress 
GROUP BY exchange;

-- Performance analysis
SELECT exchange, 
       AVG(records_loaded) as avg_records_per_file,
       AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_seconds,
       AVG(records_loaded / NULLIF(EXTRACT(EPOCH FROM (end_time - start_time)), 0)) as records_per_second
FROM bronze.load_progress 
WHERE status = 'completed'
GROUP BY exchange;
```

---

## Expected Performance

### Timeline
- **Total Files**: 93 files (31 days × 3 exchanges)
- **Estimated Time**: 2-4 hours for complete January
- **Progress Updates**: Real-time in database
- **Statistics**: Updated after each file

### Resource Usage
- **Memory**: ~1GB for Python script
- **CPU**: Moderate (mostly I/O bound)
- **Network**: S3 download bandwidth dependent
- **Storage**: Final data size ~10-50GB depending on content

---

## Troubleshooting

### Common Issues
1. **Database Lock**: Close other DuckDB connections
2. **AWS Access**: Verify S3 credentials and permissions
3. **Missing Files**: Check S3 bucket and file paths
4. **NiFi Import**: Ensure NiFi 2.4 compatibility

### Monitoring Commands
```bash
# Check progress
duckdb multi_exchange_data_lake.duckdb -c "
SELECT exchange, status, COUNT(*) 
FROM bronze.load_progress 
GROUP BY exchange, status
"

# Check performance
duckdb multi_exchange_data_lake.duckdb -c "
SELECT * FROM gold.daily_load_stats 
ORDER BY stats_date DESC, total_records DESC
"

# Check for errors
duckdb multi_exchange_data_lake.duckdb -c "
SELECT exchange, data_date, error_message 
FROM bronze.load_progress 
WHERE status = 'failed'
"
```

---

## Choose Your Approach

### Use Python Script If:
- ✅ You want quick, simple execution
- ✅ You prefer command-line tools
- ✅ You need one-time data loading
- ✅ You want minimal setup

### Use NiFi Process Group If:
- ✅ You want visual workflow management
- ✅ You need scheduled/repeated runs
- ✅ You want to modify the process easily
- ✅ You prefer GUI-based monitoring
- ✅ You need workflow orchestration

**Both approaches provide identical functionality with transaction handling and comprehensive statistics tracking!** 🎯 

# Simple January 2025 Data Loader Usage Guide

This guide explains how to use the enhanced Python script, NiFi process group, and **web dashboard** to load January 2025 market data.

## 📊 Web Dashboard

### **Start the Dashboard**
```bash
# Install dashboard dependencies
pip install flask flask-cors

# Start the dashboard server
python dashboard_app.py
```

The dashboard will be available at **http://localhost:12345**

### **Dashboard Features**

**🎯 Real-time Monitoring:**
- Live progress updates every 5 seconds
- System status (Running/Stopped/Shutdown Requested)
- Exchange-specific progress bars
- Total files, completed files, and records loaded

**📈 Visual Analytics:**
- Interactive charts showing daily progress
- Performance metrics by exchange
- Error tracking and summaries
- Weekly statistics and trends

**🎮 Control Panel:**
- **Shutdown Button**: Gracefully stop the running script
- **Resume Button**: Remove shutdown signals to allow continuation
- Real-time feedback on control actions

**📋 Activity Monitoring:**
- Recent file processing activity
- Status of each file (completed/failed/running)
- Processing times and record counts
- Error details and troubleshooting

**🔍 Database Insights:**
- Table statistics and record counts
- Performance metrics (avg processing time, records per file)
- System health indicators

### **Dashboard Screenshots**

**Main Overview:**
- System status with visual indicators
- Exchange progress with animated bars
- Key metrics at a glance

**Progress Chart:**
- Daily files processed by exchange
- Trend analysis over time
- Interactive chart with zoom/pan

**Activity Feed:**
- Real-time file processing updates
- Color-coded status indicators
- Processing time and record counts

## 🔄 Quick Start

**Option 1: Python Script + Dashboard (Recommended)**
```bash
# Terminal 1: Start the dashboard
python dashboard_app.py

# Terminal 2: Start the data loading
python load_january_simple.py --idempotent

# Open browser: http://localhost:12345
```

**Option 2: Python Script Only**
```bash
# First run
python load_january_simple.py --idempotent

# Resume interrupted run
python load_january_simple.py --idempotent
```

**Option 3: NiFi Process Group**
```bash
# Import the NiFi template
1. Open NiFi web UI (http://localhost:8080/nifi)
2. Upload template: Simple-January-2025-Loader.json
3. Drag template to canvas
4. Start the process group

# Monitor via dashboard: http://localhost:12345
```

## 🛑 How to Stop/Interrupt the Script

### 1. **Using the Web Dashboard (Easiest)**

**Access the Dashboard:**
```bash
# If not already running
python dashboard_app.py

# Open in browser
http://localhost:12345
```

**Stop the Script:**
1. Click the **"Shutdown"** button in the Control Panel
2. Watch the status change to "Shutdown Requested"
3. Script will complete current transaction and stop gracefully

**Resume the Script:**
1. Click the **"Resume"** button to remove shutdown signal
2. Restart the script: `python load_january_simple.py --idempotent`

### 2. Direct Command Line Execution

**Method 1: Ctrl+C (Standard)**
```bash
# While script is running, press Ctrl+C
# The script will complete current transaction before stopping
```

**Method 2: File-based Shutdown**
```bash
# In another terminal window:
python load_january_simple.py --create-shutdown-file

# The running script will detect this file and stop gracefully
```

### 3. Running Through NiFi Processor

When the script runs through NiFi, you can't use Ctrl+C. Use these methods:

**Method 1: Dashboard Control (Recommended)**
```bash
# Open dashboard and click "Shutdown"
http://localhost:12345
```

**Method 2: NiFi Processor Controls**
```bash
1. Go to NiFi web UI (http://localhost:8080/nifi)
2. Right-click on the ExecuteProcess processor
3. Select "Stop" or "Terminate"
   - Stop: Allows current execution to complete
   - Terminate: Forcefully kills the process
```

**Method 3: File-based Shutdown**
```bash
# From command line or another NiFi processor:
python load_january_simple.py --create-shutdown-file
```

### 4. NiFi + Dashboard Integration

**Monitoring Setup:**
1. Start NiFi process group
2. Start dashboard: `python dashboard_app.py`
3. Monitor progress at http://localhost:12345
4. Control via dashboard buttons

**Complete Control Flow:**
```
NiFi ExecuteProcess → Python Script → Database Updates → Dashboard Display
         ↑                                                       ↓
Dashboard Control ← File-based Shutdown ← Dashboard Shutdown Button
```

## 📊 Dashboard Monitoring

### **Real-time Status Indicators**

**System Status Colors:**
- 🟢 **Green**: Script running normally
- 🟡 **Yellow**: Shutdown requested (completing current task)
- 🔴 **Red**: Script stopped/idle
- 🟦 **Blue**: Processing status updates

**Progress Indicators:**
- **Animated bars**: Currently processing files
- **Static bars**: Completed processing
- **Red highlights**: Failed processing

### **Key Metrics Dashboard**

**Overview Cards:**
- **Total Files**: Sum across all exchanges
- **Completed**: Successfully processed files
- **Total Records**: Cumulative records loaded
- **System Status**: Current processing state

**Exchange Progress:**
- Individual progress bars per exchange
- Real-time file counts and percentages
- Error indicators for failed files

**Activity Feed:**
- Last 50 file processing events
- Status, timing, and record counts
- Error messages for failed loads

### **Performance Charts**

**Daily Progress Chart:**
- Files processed per day by exchange
- Trend analysis over time
- Interactive zoom and pan

**Error Analysis:**
- Error counts by exchange
- Error distribution over time
- Common error patterns

## 📋 Comprehensive Monitoring Commands

### **Database Queries (via Dashboard or Direct)**

**Check Progress:**
```sql
-- View current progress
SELECT * FROM bronze.load_progress 
WHERE status = 'started' 
ORDER BY start_time DESC;

-- View daily statistics
SELECT * FROM gold.daily_load_stats 
ORDER BY stats_date, exchange;

-- View weekly statistics
SELECT * FROM gold.weekly_load_stats 
ORDER BY week_ending DESC, exchange;
```

**Check via Dashboard API:**
```bash
# Get overview data
curl http://localhost:12345/api/overview

# Get progress details
curl http://localhost:12345/api/progress_detail

# Get error information
curl http://localhost:12345/api/errors

# Get performance statistics
curl http://localhost:12345/api/statistics
```

### **File-based Status Checks**

```bash
# Check if shutdown file exists
python load_january_simple.py --check-shutdown-file

# Output:
# Exit code 0: No shutdown file (script continues)
# Exit code 1: Shutdown file exists (script will stop)
```

## 🔧 Advanced Usage

### Dashboard + Script Integration

**Scenario 1: Development/Testing**
```bash
# Terminal 1: Start dashboard
python dashboard_app.py

# Terminal 2: Run script with verbose logging
python load_january_simple.py --idempotent --verbose

# Monitor via dashboard: http://localhost:12345
```

**Scenario 2: Production NiFi**
```bash
# Start dashboard
python dashboard_app.py &

# Configure NiFi ExecuteProcess:
# Command: python
# Arguments: load_january_simple.py --idempotent
# Working Dir: /path/to/project

# Monitor and control via dashboard
```

### Command Line Options

```bash
# Basic usage with dashboard monitoring
python load_january_simple.py --idempotent

# Custom exchanges and date range
python load_january_simple.py --idempotent \
  --exchanges LSE CME \
  --start-date 2025-01-01 \
  --end-date 2025-01-15

# Verbose logging with dashboard
python load_january_simple.py --idempotent --verbose

# Control commands
python load_january_simple.py --create-shutdown-file    # Create shutdown signal
python load_january_simple.py --remove-shutdown-file    # Remove shutdown signal
python load_january_simple.py --check-shutdown-file     # Check shutdown status
```

### NiFi Process Group Configuration

The NiFi template provides these key features:

1. **Atomic Transactions**: Each file load is wrapped in a transaction
2. **Progress Tracking**: Real-time progress updates in database
3. **Error Handling**: Failed loads are logged and can be retried
4. **Statistics Generation**: Daily and weekly statistics are maintained
5. **Dashboard Integration**: All data visible in web dashboard
6. **Graceful Shutdown**: Can be stopped cleanly via dashboard

## 🚨 Error Handling

### **Dashboard Error Monitoring**

**Error Summary Panel:**
- Shows error counts by exchange
- Highlights recent failures
- Provides error distribution analysis

**Error Details:**
- Recent failed file attempts
- Error messages and timestamps
- Retry suggestions

### **Common Issues and Solutions**

1. **"Data already exists" errors:**
   ```bash
   # Solution: Use --idempotent flag
   python load_january_simple.py --idempotent
   ```

2. **Script interrupted unexpectedly:**
   ```bash
   # Solution: Resume with --idempotent
   python load_january_simple.py --idempotent
   # Monitor progress via dashboard
   ```

3. **Dashboard not accessible:**
   ```bash
   # Check if dashboard is running
   curl http://localhost:12345/api/overview
   
   # Restart dashboard
   python dashboard_app.py
   ```

4. **NiFi process stuck:**
   ```bash
   # Use dashboard to send shutdown signal
   # Or create shutdown file directly
   python load_january_simple.py --create-shutdown-file
   ```

## 🎯 Best Practices

### **For Development/Testing:**
- Use dashboard for real-time monitoring
- Run Python script directly with `--idempotent` flag
- Monitor with `--verbose` for detailed logging
- Use dashboard shutdown controls for graceful interruption

### **For Production:**
- Use NiFi process group with dashboard monitoring
- Set up proper error handling and alerting
- Use database queries for automated monitoring
- Schedule regular statistics generation
- Monitor dashboard for system health

### **For Long-running Loads:**
- Always use `--idempotent` flag
- Monitor disk space via dashboard
- Use dashboard controls for graceful stops
- Check progress regularly via web interface
- Set up automated alerts for failures

## 📈 Performance Monitoring

### **Via Dashboard:**
1. **Real-time Metrics**: Processing speed, records per second
2. **Trend Analysis**: Daily/weekly progress charts
3. **Performance Comparison**: Exchange-specific metrics
4. **Resource Usage**: Database size, processing times

### **Via Command Line:**
```bash
# Monitor database size
du -h multi_exchange_data_lake.duckdb

# Check system resources
htop

# Monitor disk space
df -h

# Check dashboard health
curl -s http://localhost:12345/api/overview | jq .
```

## 🔗 Integration Examples

### **Full Stack Setup:**
```bash
# 1. Start dashboard
python dashboard_app.py &

# 2. Start NiFi (if using NiFi option)
/path/to/nifi/bin/nifi.sh start

# 3. Start data loading
python load_january_simple.py --idempotent &

# 4. Monitor everything
open http://localhost:12345        # Dashboard
open http://localhost:8080/nifi   # NiFi (if using)
```

### **Monitoring-only Setup:**
```bash
# If script is already running elsewhere
python dashboard_app.py

# Dashboard will show progress from any running script
open http://localhost:12345
```

The enhanced system provides **complete visibility and control** over the data loading process with the web dashboard being the central monitoring and control hub! 🚀 