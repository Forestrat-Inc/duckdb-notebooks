# 📊 January 2025 Market Data Loading Dashboard

A real-time web dashboard for monitoring and controlling the January 2025 market data loading process.

> **Note**: This dashboard is located in the `nifi/` directory. The main data loading script is in the parent directory.

## 🚀 **Quick Start**

```bash
# From the nifi directory
./start_dashboard.sh

# Access dashboard
open http://localhost:12345
```

## 📋 **Prerequisites**

- Python 3.8+
- Flask and related dependencies (installed via `pip install -r ../requirements.txt`)
- DuckDB database created by the loading script (`../multi_exchange_data_lake.duckdb`)

## 🛠️ **Installation**

### **1. Install Dependencies**
```bash
# From parent directory
cd ..
pip install -r requirements.txt
```

### **2. Start the Dashboard**
```bash
# From nifi directory
cd nifi
./start_dashboard.sh
```

## 📊 **Dashboard Features**

### **System Overview**
- **Current Status**: Running/Stopped/Shutdown Requested
- **Progress Summary**: Files processed, loaded, skipped, failed
- **Records Summary**: Total records loaded across all exchanges
- **Completion Status**: Overall progress percentage

### **Interactive Charts**
- **Daily Progress by Exchange**: Visual representation of loading progress
- **Real-time Updates**: Charts update every 5 seconds
- **Zoom/Pan Support**: Interactive exploration of data trends

### **Control Panel**
- **Shutdown Button**: Gracefully stop the running data loading script
- **Resume Button**: Remove shutdown signals to allow continuation
- **Real-time Feedback**: Visual confirmation of control actions

### **Activity Monitoring**
- **Recent Activity**: Last 50 file processing events
- **Status Indicators**: Color-coded success/failure/running states
- **Processing Details**: Timestamps, record counts, error messages

### **Error Tracking**
- **Error Summary**: Count of errors by exchange
- **Error Details**: Specific error messages and timestamps
- **Error Analysis**: Patterns and trends in failures

### **Performance Metrics**
- **Processing Time**: Average time per file/record
- **Throughput**: Records per second
- **Daily Statistics**: Files and records processed per day
- **Weekly Trends**: Rolling 7-day averages

## 🎮 **Using the Dashboard**

### **1. Starting the Dashboard**
```bash
# From nifi directory
cd nifi
./start_dashboard.sh

# Or manually start Flask app
python dashboard_app.py
```

### **2. Accessing the Dashboard**
- **URL**: http://localhost:12345
- **Auto-refresh**: Dashboard updates every 5 seconds
- **Compatible**: Works with all modern web browsers

### **3. Control Operations**

#### **Start Loading (from parent directory)**
```bash
# In another terminal window
cd ..
python load_january_simple.py --idempotent
```

#### **Stop Loading (via Dashboard)**
1. Click the "Shutdown" button
2. System will complete current transaction before stopping
3. Dashboard will show "Shutdown Requested" status

#### **Resume Loading (via Dashboard)**
1. Click the "Resume" button to remove shutdown signals
2. Manually restart the loading script from parent directory:
   ```bash
   cd ..
   python load_january_simple.py --idempotent
   ```

## 🔧 **Configuration**

### **Database Connection**
The dashboard automatically connects to:
- **Database**: `../multi_exchange_data_lake.duckdb` (parent directory)
- **Connection**: Read-only access for monitoring
- **Auto-creation**: Tables are created if they don't exist

### **Shutdown Control**
- **Shutdown File**: `../shutdown_load_january.flag` (parent directory)
- **Creation**: Dashboard creates this file to signal shutdown
- **Removal**: Dashboard removes this file to allow continuation

### **Port Configuration**
- **Default Port**: 5000
- **Change Port**: Modify `app.run(port=5000)` in `dashboard_app.py`
- **Host Access**: Currently localhost only

## 📊 **API Endpoints**

### **GET /api/overview**
Returns system overview and current status.

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

### **GET /api/progress_detail**
Returns detailed progress information for charts.

### **GET /api/errors**
Returns error tracking and analysis data.

### **GET /api/statistics**
Returns performance statistics and metrics.

### **POST /control/shutdown**
Creates shutdown signal to gracefully stop the script.

### **POST /control/resume**
Removes shutdown signal to allow script continuation.

## 🚨 **Troubleshooting**

### **Dashboard Won't Start**
```bash
# Check if port is already in use
netstat -an | grep :5000

# Check Flask installation
pip list | grep -i flask

# Check database file exists (from parent directory)
ls -la ../multi_exchange_data_lake.duckdb
```

### **No Data Showing**
- Ensure the loading script has been run at least once
- Check database path is correct (`../multi_exchange_data_lake.duckdb`)
- Verify tables exist by running the loading script

### **Control Buttons Not Working**
- Check file permissions for shutdown file creation
- Verify the loading script is running in the parent directory
- Check browser console for JavaScript errors

### **API Not Responding**
```bash
# Test API connectivity
curl -v http://localhost:12345/api/overview

# Check Flask debug logs
# Look for error messages in the terminal where dashboard is running
```

## 🔄 **Dashboard Lifecycle**

### **1. Preparation**
- Database must exist (`../multi_exchange_data_lake.duckdb`)
- Loading script should have run at least once
- All Python dependencies installed

### **2. Startup**
```bash
# From nifi directory
cd nifi
./start_dashboard.sh
```

### **3. Monitoring**
- Real-time progress updates
- Visual charts and statistics
- Error tracking and analysis

### **4. Control**
- Shutdown via dashboard button
- Resume via dashboard button
- Status feedback and confirmation

### **5. Shutdown**
- Ctrl+C to stop Flask app
- Or close terminal window
- Dashboard stops immediately (doesn't affect loading script)

## 📈 **Performance Considerations**

### **Database Queries**
- Dashboard uses read-only queries
- Queries are optimized for real-time updates
- No impact on loading script performance

### **Update Frequency**
- Dashboard auto-refreshes every 5 seconds
- Can be modified in `dashboard.html` template
- Balance between real-time updates and performance

### **Memory Usage**
- Minimal memory footprint
- No data caching (always fresh from database)
- Suitable for long-running monitoring

## 🎯 **Best Practices**

### **Development**
1. Start dashboard before loading script for immediate monitoring
2. Use verbose logging for detailed debugging
3. Test with smaller date ranges first
4. Monitor browser console for JavaScript errors

### **Production**
1. Configure proper error handling and logging
2. Set up automated restart mechanisms
3. Monitor dashboard uptime and availability
4. Use reverse proxy for external access if needed

### **Monitoring**
1. Keep dashboard running throughout the loading process
2. Use API endpoints for automated monitoring
3. Set up alerts based on error rates or completion status
4. Review performance metrics regularly

## 📞 **Support**

- **Main Documentation**: See `README.md` in this directory
- **Loading Script**: See `../load_january_simple.py` in parent directory
- **Test Functionality**: Run `python test_shutdown_functionality.py` from this directory
- **API Testing**: Use the provided curl examples above

## 🏆 **Dashboard Capabilities**

✅ **Real-time Monitoring**: Live progress and statistics  
✅ **Visual Analytics**: Interactive charts and graphs  
✅ **Control Interface**: Shutdown and resume operations  
✅ **Error Tracking**: Comprehensive error analysis  
✅ **Performance Metrics**: Processing time and throughput  
✅ **REST API**: Programmatic access to all data  
✅ **Responsive Design**: Works on desktop and mobile  
✅ **Auto-refresh**: Always shows current status  

**Ready to monitor your January 2025 market data loading with full visibility and control!** 🚀

---

## 🔄 **Quick Command Reference**

### **Start Dashboard**
```bash
cd nifi
./start_dashboard.sh
```

### **Start Data Loading**  
```bash
cd ..
python load_january_simple.py --idempotent
```

### **Access Dashboard**
```bash
open http://localhost:12345
```

### **Control via Dashboard**
- **Shutdown**: Click "Shutdown" button
- **Resume**: Click "Resume" button

### **API Access**
```bash
curl http://localhost:12345/api/overview
``` 