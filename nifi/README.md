# 📊 January 2025 Market Data Loading System - NiFi Integration

A comprehensive system for loading January 2025 market data across multiple exchanges (LSE, CME, NYQ) with **single-date processing** and **parallel execution** capabilities.

> **Note**: This directory contains the NiFi-specific components. The main data loading script (`load_january_simple.py`) is in the parent directory and now processes ONE date at a time.

## 🎯 **System Overview**

This system provides three integrated approaches for loading market data:

1. **📈 Python Script**: Single-date execution allowing parallel instances (in parent directory)
2. **🔧 NiFi Process Group**: Visual workflow for single-date processing with parallel capabilities (templates in this directory)
3. **📊 Web Dashboard**: Real-time monitoring and control interface (in this directory)

**Key Features:**
- ✅ **Single-Date Processing**: Process one date at a time for better control and parallelization
- ✅ **Parallel Execution**: Run multiple instances simultaneously for different dates
- ✅ **Graceful Shutdown**: Multiple interrupt methods that complete current transactions
- ✅ **Idempotent Processing**: Resume interrupted loads exactly where you left off
- ✅ **Real-time Monitoring**: Web dashboard with live progress and statistics
- ✅ **Transaction Safety**: Atomic database operations prevent data corruption
- ✅ **Multi-Exchange Support**: LSE, CME, NYQ with individual progress tracking
- ✅ **Comprehensive Statistics**: Daily, weekly, and performance analytics
- ✅ **Error Tracking**: Detailed error logging and analysis
- ✅ **NiFi Integration**: Professional workflow management capabilities

## 🚀 **Quick Start**

### **Option 1: Single Date Processing (Python)**
```bash
# 1. From the nifi directory, start the monitoring dashboard
cd nifi
./start_dashboard.sh

# 2. In another terminal, process a single date
cd ..
python load_january_simple.py --date 2025-01-15 --idempotent

# 3. Monitor via web browser
open http://localhost:12345
```

### **Option 2: Parallel Date Processing (Multiple Python Instances)**
```bash
# 1. Start dashboard for monitoring
cd nifi
./start_dashboard.sh

# 2. In another terminal, run multiple dates in parallel
cd ..
python load_january_simple.py --date 2025-01-01 --idempotent &
python load_january_simple.py --date 2025-01-02 --idempotent &
python load_january_simple.py --date 2025-01-03 --idempotent &
python load_january_simple.py --date 2025-01-04 --idempotent &
wait  # Wait for all background processes to complete

# 3. Monitor via dashboard: http://localhost:12345
```

### **Option 3: NiFi Parallel Process Groups**
```bash
# 1. Start dashboard for monitoring
cd nifi
./start_dashboard.sh

# 2. Set up NiFi with multiple process group instances (see detailed setup below)
# 3. Each process group handles one date, run multiple simultaneously
# 4. Monitor via dashboard
```

## 📋 **Table of Contents**

- [Installation & Setup](#installation--setup)
- [Python Script Usage](#python-script-usage)
- [NiFi Process Group Setup](#nifi-process-group-setup)
- [Web Dashboard](#web-dashboard)
- [Shutdown & Control Methods](#shutdown--control-methods)
- [Monitoring & Statistics](#monitoring--statistics)
- [File Structure](#file-structure)
- [Troubleshooting](#troubleshooting)
- [API Reference](#api-reference)

## 🛠️ **Installation & Setup**

### **Prerequisites**
- Python 3.8+
- NiFi 2.4+ (optional, for NiFi integration)
- AWS credentials configured for S3 access
- DuckDB (installed via pip)

### **1. Clone and Setup Python Environment**
```bash
# Install dependencies (from parent directory)
cd ..
pip install -r requirements.txt

# Create necessary directories
mkdir -p logs
```

### **2. AWS S3 Configuration**
Ensure your AWS credentials are configured to access the `vendor-data-s3` bucket:
```bash
# Configure AWS credentials (one of these methods)
aws configure
# OR set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
```

### **3. Database Initialization**
The system automatically creates DuckDB schemas and tables on first run:
- `bronze.load_progress` - File processing tracking
- `gold.daily_load_stats` - Daily statistics
- `gold.weekly_load_stats` - Weekly rolling statistics

## 📈 **Python Script Usage**

### **Basic Commands** (from parent directory)
```bash
# Navigate to parent directory first
cd ..

# Process a specific date (recommended)
python load_january_simple.py --date 2025-01-15 --idempotent

# Process specific exchanges for a date
python load_january_simple.py --date 2025-01-15 --idempotent --exchanges LSE CME

# Process with verbose logging
python load_january_simple.py --date 2025-01-15 --idempotent --verbose

# Parallel processing (multiple dates simultaneously)
python load_january_simple.py --date 2025-01-01 --idempotent &
python load_january_simple.py --date 2025-01-02 --idempotent &
python load_january_simple.py --date 2025-01-03 --idempotent &
wait  # Wait for all to complete
```

### **Available Options**
```bash
Options:
  --date YYYY-MM-DD          Specific date to process (default: today)
  --idempotent, --resume     Skip existing data (allows resuming)
  --exchanges LSE CME NYQ    Specific exchanges to process (default: all)
  --verbose, -v              Enable verbose logging
  
Control Options:
  --create-shutdown-file     Create shutdown signal
  --remove-shutdown-file     Remove shutdown signal  
  --check-shutdown-file      Check shutdown status
  
Parallel Processing Examples:
  # Process full month in parallel (31 instances)
  for date in {01..31}; do
    python load_january_simple.py --date 2025-01-$date --idempotent &
  done
  wait
```

### **Data Source Structure**
The system loads data from S3 bucket `vendor-data-s3` with this structure:
```
s3://vendor-data-s3/LSEG/TRTH/{exchange}/ingestion/{date}/data/merged/
└── {exchange}-{date}-NORMALIZEDMP-Data-1-of-1.csv.gz
```

**Supported exchanges:** LSE, CME, NYQ  
**Date range:** January 1-31, 2025

## 🔧 **NiFi Process Group Setup**

### **1. NiFi Installation**
```bash
# Download NiFi 2.4+ (if not already installed)
# Extract and start NiFi
cd /path/to/nifi
./bin/nifi.sh start

# Access NiFi UI
open http://localhost:8080/nifi
```

### **2. Import Process Group Template**
1. **Open NiFi Web UI**: http://localhost:8080/nifi
2. **Upload Template**:
   - Click the "Upload Template" button (📄 icon) in the toolbar
   - Select `Simple-January-2025-Loader.json` (in this nifi directory)
   - Click "Upload"

3. **Add Template to Canvas for Each Date**:
   - Drag the "Template" icon from the toolbar to the canvas
   - Select "Single-Date-Market-Data-Loader" from the dropdown
   - Click "Add"
   - **Repeat for multiple dates** (create multiple instances for parallel processing)

### **3. Configure Process Group**

#### **Controller Services Setup**
1. **Right-click the process group** → "Configure"
2. **Controller Services tab** → Configure the following:

**AWS Credentials Service:**
```
Service: AWSCredentialsProviderControllerService
Access Key: [Your AWS Access Key]
Secret Key: [Your AWS Secret Key]
```

**S3 Client Service:**
```
Service: AWSCredentialsProviderControllerService  
Region: us-east-1 (or your bucket region)
```

**CSV Reader:**
```
Service: CSVReader
Schema Access Strategy: Use 'Schema Text' Property
Schema Text: [Auto-detected based on first file]
```

#### **Processor Configuration**

**Process Group Variables (for each instance):**
```
TARGET_DATE: 2025-01-15        # Set different date for each instance
EXCHANGES: LSE CME NYQ         # Space-separated exchange list
IDEMPOTENT: --idempotent       # Idempotent mode flag
```

**ExecuteProcess (Main Script):**
```
Command: python
Command Arguments: ${python_script} --date ${target_date} ${idempotent_mode} --exchanges ${exchanges} --verbose
Working Directory: /path/to/your/project
Environment Variables: 
  - AWS_ACCESS_KEY_ID: [your_key]
  - AWS_SECRET_ACCESS_KEY: [your_secret]
```

**Multiple Instance Setup:**
```
Instance 1: TARGET_DATE = 2025-01-01
Instance 2: TARGET_DATE = 2025-01-02  
Instance 3: TARGET_DATE = 2025-01-03
... (up to 31 instances for full month)
```

### **4. Start the Process Group**
1. **Enable Controller Services**: 
   - Controller Services tab → Enable all services
2. **Start Processors**:
   - Right-click process group → "Start"
   - All processors should transition to running state

### **5. Monitor via Dashboard**
```bash
# Start monitoring dashboard (from nifi directory)
./start_dashboard.sh

# Access dashboard
open http://localhost:12345
```

## 📊 **Web Dashboard**

### **Starting the Dashboard**
```bash
# From the nifi directory
./start_dashboard.sh

# Access via browser
open http://localhost:12345
```

### **Dashboard Features**

#### **🎯 Real-time Monitoring**
- **System Status**: Running/Stopped/Shutdown Requested indicators
- **Progress Bars**: Visual progress for each exchange
- **Live Metrics**: Files processed, records loaded, completion percentages
- **Auto-refresh**: Updates every 5 seconds

#### **📈 Visual Analytics**
- **Interactive Charts**: Daily progress by exchange with zoom/pan
- **Performance Metrics**: Processing time, records per file
- **Error Analysis**: Error counts and detailed messages
- **Trend Analysis**: Weekly rolling statistics

#### **🎮 Control Panel**
- **Shutdown Button**: Gracefully stop running processes
- **Resume Button**: Remove shutdown signals
- **Status Feedback**: Real-time operation status

#### **📊 Activity Monitoring**
- **Recent Activity**: Last 50 file processing events
- **Status Indicators**: Color-coded success/failure/running states
- **Processing Details**: Timestamps, record counts, error messages

### **Dashboard API**
```bash
# Programmatic access via REST API
curl http://localhost:12345/api/overview          # System overview
curl http://localhost:12345/api/progress_detail   # Detailed progress
curl http://localhost:12345/api/errors           # Error tracking
curl http://localhost:12345/api/statistics       # Performance stats
```

## 🛑 **Shutdown & Control Methods**

### **1. Web Dashboard Control (Recommended)**
- **Access**: http://localhost:12345
- **Shutdown**: Click "Shutdown" button in control panel
- **Resume**: Click "Resume" button to remove shutdown signals
- **Status**: Real-time visual feedback

### **2. Command Line Control** (from parent directory)
```bash
# Create shutdown signal
cd ..
python load_january_simple.py --create-shutdown-file

# Remove shutdown signal
python load_january_simple.py --remove-shutdown-file

# Check shutdown status
python load_january_simple.py --check-shutdown-file
```

### **3. NiFi Processor Control**
- **Access**: http://localhost:8080/nifi
- **Stop**: Right-click ExecuteProcess → "Stop" (graceful)
- **Terminate**: Right-click ExecuteProcess → "Terminate" (force)

### **4. Signal-based Control (Direct execution only)**
```bash
# Graceful interrupt (from parent directory)
cd ..
python load_january_simple.py --idempotent
# Then press Ctrl+C - completes current transaction before stopping
```

### **Shutdown Behavior**
- ✅ **Current transaction completes** before stopping
- ✅ **Database consistency** maintained
- ✅ **Progress tracking** updated with final status
- ✅ **Resume capability** via `--idempotent` flag

## 📈 **Monitoring & Statistics**

### **Progress Tracking**
```sql
-- View current progress
SELECT * FROM bronze.load_progress ORDER BY start_time DESC;

-- Progress by exchange
SELECT exchange, 
       COUNT(*) as total_files,
       COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
       COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
FROM bronze.load_progress 
GROUP BY exchange;
```

### **Performance Statistics**
```sql
-- Daily statistics
SELECT * FROM gold.daily_load_stats ORDER BY stats_date, exchange;

-- Weekly rolling statistics  
SELECT * FROM gold.weekly_load_stats ORDER BY week_ending DESC, exchange;

-- Performance metrics
SELECT exchange,
       AVG(records_loaded) as avg_records_per_file,
       AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_processing_time
FROM bronze.load_progress 
WHERE status = 'completed'
GROUP BY exchange;
```

### **Error Analysis**
```sql
-- Recent errors
SELECT exchange, data_date, error_message, start_time
FROM bronze.load_progress 
WHERE status = 'failed' 
ORDER BY start_time DESC;

-- Error summary by exchange
SELECT exchange, COUNT(*) as error_count
FROM bronze.load_progress 
WHERE status = 'failed'
GROUP BY exchange;
```

## 📁 **File Structure**

```
duckdb/
├── 📈 Core Scripts (Parent Directory)
│   ├── load_january_simple.py          # Main data loading script
│   ├── multi_exchange_data_lake.duckdb  # Main database (auto-created)
│   └── logs/                            # Application logs (auto-created)
│
├── 🔧 NiFi Integration (This Directory) 
│   ├── dashboard_app.py                 # Web dashboard application
│   ├── start_dashboard.sh               # Dashboard startup script
│   ├── Simple-January-2025-Loader.json # NiFi process group template
│   ├── test_shutdown_functionality.py   # Shutdown feature tests
│   ├── README.md                        # This comprehensive guide
│   ├── DASHBOARD_README.md              # Dashboard-specific documentation
│   └── templates/
│       └── dashboard.html               # Dashboard UI template
│
├── ⚙️ Configuration (Parent Directory)
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py                  # System configuration
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── database.py                  # Database utilities
│   │   └── data_processing.py           # Data processing utilities
│   └── requirements.txt                 # Python dependencies
│
└── 📝 Additional Documentation (Parent Directory)
    ├── SIMPLE_USAGE_GUIDE.md            # Detailed usage guide
    └── fix_summary.md                   # Historical development notes
```

## 🚨 **Troubleshooting**

### **Common Issues**

#### **1. "Data already exists" errors**
```bash
# Solution: Use idempotent mode (from parent directory)
cd ..
python load_january_simple.py --idempotent
```

#### **2. Dashboard not accessible**
```bash
# Check if dashboard is running
curl http://localhost:12345/api/overview

# Check port availability
netstat -an | grep :5000

# Restart dashboard (from nifi directory)
cd nifi
./start_dashboard.sh
```

#### **3. AWS S3 connection issues**
```bash
# Test S3 connectivity (from parent directory)
cd ..
python test_s3_connection.py

# Verify credentials
aws s3 ls s3://vendor-data-s3/LSEG/TRTH/LSE/ingestion/2025-01-01/
```

#### **4. NiFi process stuck**
```bash
# Use dashboard shutdown (from nifi directory)
http://localhost:12345 → Click "Shutdown"

# Or create shutdown file (from parent directory)
cd ..
python load_january_simple.py --create-shutdown-file

# Check NiFi logs
tail -f /path/to/nifi/logs/nifi-app.log
```

#### **5. Database lock issues**
```bash
# Check for running processes
ps aux | grep load_january_simple

# Check database file location (parent directory)
ls -la ../multi_exchange_data_lake.duckdb
```

### **Performance Issues**

#### **Slow processing**
- Monitor system resources: `htop`
- Check disk space: `df -h`
- Review network connectivity to S3
- Consider processing fewer exchanges: `--exchanges LSE`

#### **Memory issues**
- Monitor via dashboard performance metrics
- Check database size: `du -h ../multi_exchange_data_lake.duckdb`
- Consider processing smaller date ranges

### **Debugging Commands**
```bash
# Check system status (from parent directory)
cd ..
python load_january_simple.py --check-shutdown-file

# Test database connectivity (from parent directory)
cd ..
python -c "from utils.database import DuckDBManager; db = DuckDBManager(); print('DB OK')"

# Check recent logs (from parent directory)
tail -50 ../logs/january_load_simple_*.log
```

## 📡 **API Reference**

### **Dashboard REST API**

#### **GET /api/overview**
Returns system overview with current status and high-level metrics.

```bash
curl http://localhost:12345/api/overview
```

**Response:**
```json
{
  "overview": [
    {
      "exchange": "LSE",
      "total_files": 31,
      "completed_files": 25,
      "failed_files": 1,
      "running_files": 0,
      "total_records": 2500000
    }
  ],
  "is_running": false,
  "shutdown_requested": false,
  "last_updated": "2025-01-15T10:30:00"
}
```

#### **GET /api/progress_detail**
Returns detailed progress information and chart data.

#### **GET /api/errors**
Returns error tracking and analysis data.

#### **GET /api/statistics**
Returns performance statistics and metrics.

#### **POST /control/shutdown**
Creates shutdown signal to gracefully stop the script.

#### **POST /control/resume**
Removes shutdown signal to allow script continuation.

## 🎯 **Best Practices**

### **Development & Testing**
1. **Always use `--idempotent` flag** for resumable operations
2. **Start dashboard first** for monitoring capabilities (from nifi directory)
3. **Use verbose logging** for detailed debugging: `--verbose`
4. **Test with smaller date ranges** before full month processing
5. **Monitor disk space** during large data loads

### **Production Deployment**
1. **Use NiFi process groups** for visual workflow management
2. **Set up automated monitoring** via dashboard API endpoints
3. **Configure proper error handling** and alerting
4. **Schedule regular statistics updates**
5. **Implement backup strategies** for the DuckDB database

### **Performance Optimization**
1. **Monitor processing metrics** via dashboard
2. **Process exchanges in parallel** when possible
3. **Use appropriate hardware** (SSD storage, adequate RAM)
4. **Optimize network connectivity** to S3
5. **Regular database maintenance** and statistics updates

## 🔮 **Advanced Usage**

### **Automated Monitoring**
```bash
# Set up automated status checking
*/5 * * * * curl -s http://localhost:12345/api/overview | jq .is_running

# Automated restart on failure
if ! curl -s http://localhost:12345/api/overview; then
    cd /path/to/your/project/nifi
    ./start_dashboard.sh &
fi
```

### **Custom Date Ranges** (from parent directory)
```bash
# Process specific months
cd ..
python load_january_simple.py --idempotent \
  --start-date 2025-01-15 \
  --end-date 2025-01-20

# Process specific exchanges
python load_january_simple.py --idempotent \
  --exchanges LSE CME
```

### **Integration with Other Systems**
```python
# Example: Integration with alerting systems
import requests

def check_loading_status():
    response = requests.get('http://localhost:12345/api/overview')
    data = response.json()
    
    if data['is_running']:
        print("✅ Data loading is active")
    else:
        print("⚠️ Data loading is not running")
        
    return data
```

## 📞 **Support & Documentation**

- **Dashboard Guide**: See `DASHBOARD_README.md` (in this directory)
- **Detailed Usage**: See `SIMPLE_USAGE_GUIDE.md` (in parent directory)
- **Test Functionality**: Run `python test_shutdown_functionality.py` (from this directory)
- **API Testing**: Use the provided curl examples above

## 🏆 **System Capabilities Summary**

✅ **Multi-Exchange Support**: LSE, CME, NYQ  
✅ **Date Range Flexibility**: Custom start/end dates  
✅ **Graceful Shutdown**: Multiple interrupt methods  
✅ **Resume Capability**: Idempotent processing  
✅ **Real-time Monitoring**: Web dashboard with live updates  
✅ **NiFi Integration**: Professional workflow management  
✅ **Transaction Safety**: Atomic database operations  
✅ **Error Tracking**: Comprehensive error analysis  
✅ **Performance Analytics**: Daily/weekly statistics  
✅ **API Access**: REST endpoints for automation  

The system provides enterprise-grade market data loading with complete monitoring and control capabilities! 🚀

---

**Ready to start loading January 2025 market data with full visibility and control!** 🎯

## 🔄 **Quick Command Reference**

### **Start Dashboard (from nifi directory)**
```bash
cd nifi
./start_dashboard.sh
open http://localhost:12345
```

### **Start Data Loading (from parent directory)**  
```bash
cd ..
python load_january_simple.py --idempotent
```

### **Control via Dashboard**
- **Shutdown**: http://localhost:12345 → Click "Shutdown" button
- **Resume**: http://localhost:12345 → Click "Resume" button

### **NiFi Template**
- File: `Simple-January-2025-Loader.json` (in this directory)
- Import via NiFi UI: http://localhost:8080/nifi
