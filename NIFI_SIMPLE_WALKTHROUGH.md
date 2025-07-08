# 🚀 **DEAD SIMPLE NiFi Walkthrough**
*Run `load_january_simple.py` through NiFi in 5 minutes*

---

## **Step 1: Start NiFi** ⚡

```bash
# Navigate to your NiFi installation
cd /path/to/nifi-2.4.0
./bin/nifi.sh start

# Wait 2-3 minutes for startup, then access:
open http://localhost:8080/nifi
```

---

## **Step 2: Import the Template** 📁

1. **In NiFi UI**: Click the **Upload Template** button (📄 icon)
2. **Select file**: `nifi/Simple-January-2025-Loader.json`
3. **Click**: "Upload"
4. **Drag**: Template icon from toolbar to canvas
5. **Select**: "Single-Date-Market-Data-Loader"
6. **Click**: "Add"

---

## **Step 3: Configure the Process Group** ⚙️

### **Set Parameters**
1. **Right-click** the process group → **Configure**
2. **Parameters tab** → **Add Parameters**:
   ```
   TARGET_DATE = 2025-01-15
   EXCHANGES = LSE CME NYQ
   IDEMPOTENT = --idempotent
   WORKING_DIR = /Users/kaushal/Documents/Forestrat/duckdb
   ```

### **Set Environment Variables**
1. **Double-click** "Execute Python Data Loading" processor
2. **Properties tab** → **Environment Variables**:
   ```
   AWS_ACCESS_KEY_ID=your_access_key_here
   AWS_SECRET_ACCESS_KEY=your_secret_key_here
   ```
3. **Working Directory**: `/Users/kaushal/Documents/Forestrat/duckdb`

---

## **Step 4: Start the Process** ▶️

1. **Right-click** the process group → **Start**
2. **Watch progress** in the NiFi UI
3. **Monitor logs** (see log locations below)

---

## **📍 LOG LOCATIONS** 📝

### **1. Python Script Logs** (Your data loading logs)
```bash
# Location:
/Users/kaushal/Documents/Forestrat/duckdb/logs/january_load_simple_*.log

# View latest:
tail -f /Users/kaushal/Documents/Forestrat/duckdb/logs/january_load_simple_$(date +%Y%m%d)_*.log

# View all recent logs:
ls -la /Users/kaushal/Documents/Forestrat/duckdb/logs/
```

### **2. NiFi Application Logs** (NiFi system logs)
```bash
# Location:
/path/to/nifi-2.4.0/logs/nifi-app.log

# View latest:
tail -f /path/to/nifi-2.4.0/logs/nifi-app.log

# Search for errors:
grep "ERROR" /path/to/nifi-2.4.0/logs/nifi-app.log
```

### **3. NiFi Processor Logs** (In the NiFi UI)
1. **Right-click** any processor → **View Data Provenance**
2. **Check** the "Logs" tab in processor details
3. **View** FlowFile attributes for detailed information

### **4. Dashboard Logs** (If using the web dashboard)
```bash
# Location:
/Users/kaushal/Documents/Forestrat/duckdb/nifi/dashboard.log

# View:
tail -f /Users/kaushal/Documents/Forestrat/duckdb/nifi/dashboard.log
```

---

## **📊 MONITORING** 👀

### **Option 1: NiFi UI** (http://localhost:8080/nifi)
- **Visual flow**: See data moving through processors
- **Queue counts**: Monitor backpressure
- **Processor stats**: Success/failure rates

### **Option 2: Web Dashboard** (Recommended)
```bash
# Start dashboard (from nifi directory):
cd /Users/kaushal/Documents/Forestrat/duckdb/nifi
./start_dashboard.sh

# Access dashboard:
open http://localhost:12345
```

### **Option 3: Command Line Log Monitoring**
```bash
# Monitor Python script logs in real-time:
tail -f /Users/kaushal/Documents/Forestrat/duckdb/logs/january_load_simple_*.log

# Monitor NiFi logs:
tail -f /path/to/nifi-2.4.0/logs/nifi-app.log | grep "ExecuteProcess"
```

---

## **🛑 STOPPING THE PROCESS** 

### **Method 1: NiFi UI** (Immediate)
1. **Right-click** process group → **Stop**
2. **Wait** for processors to finish current tasks

### **Method 2: Graceful Shutdown** (Recommended)
```bash
# Create shutdown signal:
cd /Users/kaushal/Documents/Forestrat/duckdb
python load_january_simple.py --create-shutdown-file

# The script will finish current transaction and stop
```

### **Method 3: Dashboard** (Easiest)
1. **Open**: http://localhost:12345
2. **Click**: "Shutdown" button
3. **Wait** for graceful completion

---

## **📋 WHAT TO EXPECT** 

### **In NiFi UI:**
- **Generate Trigger**: Creates a single FlowFile to start processing
- **Set Date Attributes**: Adds date and exchange parameters
- **Execute Python Loading**: Runs your script with the specified parameters
- **Success/Failure Routes**: Shows whether the script succeeded

### **In Python Script Logs:**
```
2025-01-20 10:15:23 - INFO - Loading LSE data for date: 2025-01-15
2025-01-20 10:15:25 - INFO - ✅ Successfully loaded 45,231 records for LSE 2025-01-15 in 2.34s
2025-01-20 10:15:27 - INFO - Loading CME data for date: 2025-01-15
2025-01-20 10:15:30 - INFO - ✅ Successfully loaded 67,891 records for CME 2025-01-15 in 3.12s
```

### **Processing Time:**
- **Single Date**: ~5-15 minutes (depends on data size)
- **Three Exchanges**: LSE, CME, NYQ processed sequentially

---

## **🚨 TROUBLESHOOTING** 

### **"Script not found" error:**
- **Check**: Working Directory is set to `/Users/kaushal/Documents/Forestrat/duckdb`
- **Verify**: Script exists at `../load_january_simple.py`

### **"AWS credentials" error:**
- **Set**: Environment variables in ExecuteProcess processor
- **Test**: `aws s3 ls s3://vendor-data-s3/` from command line

### **"Permission denied" error:**
- **Check**: File permissions on the script
- **Run**: `chmod +x load_january_simple.py`

### **Processor won't start:**
- **Check**: NiFi logs for detailed error messages
- **Verify**: All required parameters are set
- **Ensure**: Python is in the system PATH

---

## **📁 LOG FILE EXAMPLES**

### **Python Script Log** (`logs/january_load_simple_20250120_101523.log`):
```
2025-01-20 10:15:23,456 - INFO - Starting load for date: 2025-01-15
2025-01-20 10:15:23,457 - INFO - Processing exchanges: ['LSE', 'CME', 'NYQ']
2025-01-20 10:15:23,458 - INFO - Idempotent mode: enabled
2025-01-20 10:15:24,123 - INFO - ✅ LSE: 45,231 records loaded in 2.34s
2025-01-20 10:15:27,456 - INFO - ✅ CME: 67,891 records loaded in 3.12s
2025-01-20 10:15:31,789 - INFO - ✅ NYQ: 52,147 records loaded in 4.23s
2025-01-20 10:15:31,790 - INFO - 🎉 All exchanges completed successfully
```

### **NiFi Application Log** (`nifi-app.log`):
```
2025-01-20 10:15:23,456 INFO [Timer-Driven Process Thread-1] o.a.n.p.standard.ExecuteProcess Execute Python Data Loading[id=abc123] Starting process: python load_january_simple.py --date 2025-01-15 --idempotent --exchanges LSE CME NYQ --verbose
2025-01-20 10:15:31,790 INFO [Timer-Driven Process Thread-1] o.a.n.p.standard.ExecuteProcess Execute Python Data Loading[id=abc123] Process completed with exit code: 0
```

---

## **🎯 SUMMARY**

**What NiFi Does:**
1. **Triggers** the Python script execution
2. **Passes parameters** (date, exchanges, flags)
3. **Monitors** execution status
4. **Routes** success/failure appropriately
5. **Logs** everything for troubleshooting

**Where Logs Are:**
- **Python Script Logs**: `duckdb/logs/january_load_simple_*.log`
- **NiFi System Logs**: `nifi/logs/nifi-app.log` 
- **Dashboard Logs**: `duckdb/nifi/dashboard.log`
- **Database Progress**: Tables in DuckDB for real-time monitoring

**That's it!** 🎉 Your script now runs through NiFi with full logging and monitoring. 