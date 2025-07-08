# 🛠️ **MANUAL NiFi Setup - ExecuteProcess Only**
*Skip the template - set up manually in 3 minutes*

---

## **Step 1: Start NiFi** ⚡

```bash
# Navigate to your NiFi installation
cd /path/to/nifi-2.4.0
./bin/nifi.sh start

# Wait 2-3 minutes, then access:
open http://localhost:8080/nifi
```

---

## **Step 2: Configure AWS Credentials in Shell Script** 🔧

**Edit your `run_with_venv.sh` script to include AWS credentials:**

1. **Open the script**:
   ```bash
   nano /Users/kaushal/Documents/Forestrat/duckdb/run_with_venv.sh
   ```

2. **Add your AWS credentials** (replace with your actual keys):
   ```bash
   # Set AWS credentials
   export AWS_ACCESS_KEY_ID="AKIA123YOURACCESSKEY"
   export AWS_SECRET_ACCESS_KEY="abc123YourSecretKeyHere"
   ```

3. **Make the script executable**:
   ```bash
   chmod +x /Users/kaushal/Documents/Forestrat/duckdb/run_with_venv.sh
   ```

## **Step 3: Add ExecuteProcess Processor** 🔧

1. **Drag** "Processor" from the toolbar to the canvas
2. **Search** for "ExecuteProcess" 
3. **Select** "ExecuteProcess" and click **Add**

---

## **Step 4: Configure ExecuteProcess** ⚙️

### **Double-click** the ExecuteProcess processor to configure:

#### **🏷️ SETTINGS Tab:**
- **Name**: `Load January Data`
- **Penalty Duration**: `30 sec`
- **Yield Duration**: `1 sec`

#### **📋 PROPERTIES Tab:**
```
Command: ./run_with_venv.sh

Command Arguments: 
--date ${TARGET_DATE} --idempotent --exchanges ${EXCHANGES} --verbose

Working Directory: 
/Users/kaushal/Documents/Forestrat/duckdb

Batch Duration:
0

Redirect Error Stream:
true
```

**✅ AWS Credentials are now embedded in your shell script!** No need to configure them in NiFi - much simpler!

---

## **📝 PARAMETERIZATION SETUP** (Variables via NiFi API)

### **Step 1: Create Parameter Context** (Recommended)

1. **In NiFi UI**: Click **hamburger menu** (☰) → **Parameter Contexts**
2. **Click "+"** to create new context
3. **Name**: `market-data-params`
4. **Add Parameters**:
   ```
   TARGET_DATE = 2025-01-15
   EXCHANGES = LSE CME NYQ
   IDEMPOTENT_FLAG = --idempotent
   VERBOSE_FLAG = --verbose
   WORKING_DIR = /Users/kaushal/Documents/Forestrat/duckdb
   ```
5. **Click "Apply"**

### **Step 2: Apply Parameter Context to Process Group**

1. **Right-click** on canvas (empty space) → **Configure**
2. **General tab** → **Parameter Context**: Select `market-data-params`
3. **Click "Apply"**

### **Step 3: Alternative - Process Group Variables** (Simpler)

If you prefer variables over parameter context:

1. **Right-click** on canvas → **Variables**
2. **Add Variables**:
   ```
   TARGET_DATE = 2025-01-15
   EXCHANGES = LSE CME NYQ
   IDEMPOTENT_FLAG = --idempotent
   VERBOSE_FLAG = --verbose
   ```
3. **Click "Apply"**

### **🔧 UPDATED PROCESSOR CONFIGURATION**

**Command Arguments** (using variables):
```bash
--date ${TARGET_DATE} ${IDEMPOTENT_FLAG} --exchanges ${EXCHANGES} ${VERBOSE_FLAG}
```

**Working Directory** (can also be parameterized):
```bash
${WORKING_DIR}
```

---

## **🌐 NIFI API ACCESS** (View/Modify Parameters)

### **📋 View Parameters via API**

#### **Get Parameter Contexts:**
```bash
# List all parameter contexts:
curl -X GET http://localhost:8080/nifi-api/parameter-contexts

# Get specific parameter context:
curl -X GET http://localhost:8080/nifi-api/parameter-contexts/{context-id}

# Get market-data-params context (find ID first):
curl -X GET http://localhost:8080/nifi-api/parameter-contexts | jq '.parameterContexts[] | select(.component.name=="market-data-params")'
```

#### **Get Process Group Variables:**
```bash
# Get process group info (includes variables):
curl -X GET http://localhost:8080/nifi-api/process-groups/{process-group-id}

# Get root process group:
curl -X GET http://localhost:8080/nifi-api/process-groups/root
```

### **🔧 Update Parameters via API**

#### **Update Parameter Context:**
```bash
# Get current context (for revision):
CONTEXT_ID=$(curl -s http://localhost:8080/nifi-api/parameter-contexts | jq -r '.parameterContexts[] | select(.component.name=="market-data-params") | .id')

# Update TARGET_DATE parameter:
curl -X PUT http://localhost:8080/nifi-api/parameter-contexts/$CONTEXT_ID \
  -H "Content-Type: application/json" \
  -d '{
    "revision": {"version": 0},
    "component": {
      "id": "'$CONTEXT_ID'",
      "name": "market-data-params",
      "parameters": [
        {
          "parameter": {
            "name": "TARGET_DATE",
            "value": "2025-01-20",
            "sensitive": false
          }
        }
      ]
    }
  }'
```

#### **Update Process Group Variables:**
```bash
# Get process group ID:
PG_ID=$(curl -s http://localhost:8080/nifi-api/process-groups/root | jq -r '.id')

# Update variable:
curl -X PUT http://localhost:8080/nifi-api/process-groups/$PG_ID/variable-registry \
  -H "Content-Type: application/json" \
  -d '{
    "processGroupRevision": {"version": 0},
    "variableRegistry": {
      "variables": [
        {
          "variable": {
            "name": "TARGET_DATE",
            "value": "2025-01-20"
          }
        }
      ]
    }
  }'
```

### **📊 Monitor Processor via API**

#### **Get Processor Status:**
```bash
# Find your ExecuteProcess processor:
curl -X GET http://localhost:8080/nifi-api/process-groups/root/processors

# Get specific processor status:
PROCESSOR_ID=$(curl -s http://localhost:8080/nifi-api/process-groups/root/processors | jq -r '.processors[] | select(.component.name=="Load January Data") | .id')

curl -X GET http://localhost:8080/nifi-api/processors/$PROCESSOR_ID
```

#### **Start/Stop Processor via API:**
```bash
# Stop processor:
curl -X PUT http://localhost:8080/nifi-api/processors/$PROCESSOR_ID \
  -H "Content-Type: application/json" \
  -d '{
    "revision": {"version": 0},
    "component": {
      "id": "'$PROCESSOR_ID'",
      "state": "STOPPED"
    }
  }'

# Start processor:
curl -X PUT http://localhost:8080/nifi-api/processors/$PROCESSOR_ID \
  -H "Content-Type: application/json" \
  -d '{
    "revision": {"version": 0},
    "component": {
      "id": "'$PROCESSOR_ID'",
      "state": "RUNNING"
    }
  }'
```

---

## **🚀 EXAMPLE WORKFLOWS**

### **Scenario 1: Run Different Date via API**
```bash
# 1. Stop processor
curl -X PUT http://localhost:8080/nifi-api/processors/$PROCESSOR_ID \
  -d '{"revision":{"version":0},"component":{"id":"'$PROCESSOR_ID'","state":"STOPPED"}}'

# 2. Update TARGET_DATE parameter
curl -X PUT http://localhost:8080/nifi-api/parameter-contexts/$CONTEXT_ID \
  -d '{"revision":{"version":0},"component":{"parameters":[{"parameter":{"name":"TARGET_DATE","value":"2025-01-25"}}]}}'

# 3. Start processor
curl -X PUT http://localhost:8080/nifi-api/processors/$PROCESSOR_ID \
  -d '{"revision":{"version":0},"component":{"id":"'$PROCESSOR_ID'","state":"RUNNING"}}'
```

### **Scenario 2: Run Only Specific Exchanges**
```bash
# Update EXCHANGES parameter to run only LSE and CME:
curl -X PUT http://localhost:8080/nifi-api/parameter-contexts/$CONTEXT_ID \
  -d '{"revision":{"version":0},"component":{"parameters":[{"parameter":{"name":"EXCHANGES","value":"LSE CME"}}]}}'
```

### **🔍 Quick API Reference Script**

Create a helper script `nifi-api-helper.sh`:
```bash
#!/bin/bash
NIFI_URL="http://localhost:8080/nifi-api"

# Get IDs
get_context_id() {
  curl -s $NIFI_URL/parameter-contexts | jq -r '.parameterContexts[] | select(.component.name=="market-data-params") | .id'
}

get_processor_id() {
  curl -s $NIFI_URL/process-groups/root/processors | jq -r '.processors[] | select(.component.name=="Load January Data") | .id'
}

# Update date
update_date() {
  CONTEXT_ID=$(get_context_id)
  curl -X PUT $NIFI_URL/parameter-contexts/$CONTEXT_ID \
    -H "Content-Type: application/json" \
    -d '{"revision":{"version":0},"component":{"parameters":[{"parameter":{"name":"TARGET_DATE","value":"'$1'"}}]}}'
}

# Usage: ./nifi-api-helper.sh update_date 2025-01-20
```

#### **⏰ SCHEDULING Tab:**
```
Scheduling Strategy: Timer driven
Run Duration: 0 milliseconds  
Run Schedule: 0 sec
Concurrent Tasks: 1
```

#### **🚫 RELATIONSHIPS Tab:**
- **Check** ✅ `success` (auto-terminate)
- **Check** ✅ `failure` (auto-terminate)

---

## **Step 5: Start the Processor** ▶️

1. **Right-click** the ExecuteProcess processor
2. **Click** "Start"
3. **Watch** the processor turn green and start running

---

## **📍 LOG LOCATIONS** 📝

### **Your Python Script Logs** (Primary logs to watch):
```bash
# Location:
/Users/kaushal/Documents/Forestrat/duckdb/logs/january_load_simple_*.log

# Watch in real-time:
tail -f /Users/kaushal/Documents/Forestrat/duckdb/logs/january_load_simple_*.log

# List all recent logs:
ls -la /Users/kaushal/Documents/Forestrat/duckdb/logs/
```

### **NiFi Processor Logs**:
```bash
# NiFi system logs:
tail -f /path/to/nifi-2.4.0/logs/nifi-app.log | grep "ExecuteProcess"

# In NiFi UI:
Right-click processor → View Data Provenance → Check "Details" for each FlowFile
```

---

## **📊 MONITORING** 👀

### **In NiFi UI:**
- **Green processor** = Running
- **Numbers on processor** = Success/failure counts
- **Right-click** → "View Status History" for charts

### **Command Line:**
```bash
# Watch your script logs:
tail -f /Users/kaushal/Documents/Forestrat/duckdb/logs/january_load_simple_*.log

# Check if script is running:
ps aux | grep load_january_simple
```

### **Optional Dashboard:**
```bash
# Start the web dashboard:
cd /Users/kaushal/Documents/Forestrat/duckdb/nifi
./start_dashboard.sh

# Access: http://localhost:12345
```

---

## **🛑 STOPPING THE PROCESS** 

### **Method 1: NiFi UI** (Immediate)
1. **Right-click** processor → **Stop**
2. **Wait** for current execution to finish

### **Method 2: Graceful Shutdown** (Recommended)
```bash
# Create shutdown signal:
cd /Users/kaushal/Documents/Forestrat/duckdb
python load_january_simple.py --create-shutdown-file

# The script will finish current file and stop
```

---

## **🔄 CHANGING PARAMETERS** 

### **Method 1: Via NiFi UI** (Manual)

**For Parameter Context:**
1. **Click hamburger menu** (☰) → **Parameter Contexts**
2. **Click** "market-data-params" → **Edit**
3. **Update values**:
   ```
   TARGET_DATE = 2025-01-20        # Change date
   EXCHANGES = LSE CME            # Change exchanges
   ```
4. **Click "Apply"**
5. **Restart processor** if running

**For Process Group Variables:**
1. **Right-click** canvas → **Variables**
2. **Update variable values**
3. **Click "Apply"**

### **Method 2: Via NiFi API** (Programmatic)

**Quick parameter update:**
```bash
# Update date via API:
CONTEXT_ID=$(curl -s http://localhost:8080/nifi-api/parameter-contexts | jq -r '.parameterContexts[] | select(.component.name=="market-data-params") | .id')

curl -X PUT http://localhost:8080/nifi-api/parameter-contexts/$CONTEXT_ID \
  -H "Content-Type: application/json" \
  -d '{"revision":{"version":0},"component":{"parameters":[{"parameter":{"name":"TARGET_DATE","value":"2025-01-25"}}]}}'
```

### **Method 3: Automated Script**
```bash
# Use the helper script:
./nifi-api-helper.sh update_date 2025-01-25
```

---

## **📋 WHAT YOU'LL SEE** 

### **In NiFi UI:**
- **Processor starts** (turns green)
- **FlowFile created** (you'll see "1" in the processor)
- **Success/Failure** routes based on script exit code
- **Duration** shows how long the script ran

### **In Your Python Logs:**
```
2025-01-20 10:15:23 - INFO - Starting load for date: 2025-01-15
2025-01-20 10:15:23 - INFO - Processing exchanges: ['LSE', 'CME', 'NYQ']
2025-01-20 10:15:24 - INFO - ✅ LSE: 45,231 records loaded in 2.34s
2025-01-20 10:15:27 - INFO - ✅ CME: 67,891 records loaded in 3.12s
2025-01-20 10:15:31 - INFO - ✅ NYQ: 52,147 records loaded in 4.23s
2025-01-20 10:15:31 - INFO - 🎉 All exchanges completed successfully
```

### **In NiFi Logs:**
```
2025-01-20 10:15:23 INFO [Timer-Driven Process Thread-1] ExecuteProcess[id=abc123] Starting process: ./run_with_venv.sh --date 2025-01-15 --idempotent --exchanges LSE CME NYQ --verbose
2025-01-20 10:15:31 INFO [Timer-Driven Process Thread-1] ExecuteProcess[id=abc123] Process completed successfully with exit code: 0
```

---

## **🚨 TROUBLESHOOTING** 

### **Processor shows "Invalid":**
- **Check**: Working Directory path is correct
- **Verify**: Python is in system PATH
- **Ensure**: Script file exists

### **"Command not found" error:**
```bash
# Test Python path:
which python

# If not found, use full path:
Command: /usr/bin/python3
# or
Command: /usr/local/bin/python
```

### **"Permission denied":**
```bash
# Make shell script executable:
chmod +x /Users/kaushal/Documents/Forestrat/duckdb/run_with_venv.sh

# Alternative: Use bash directly with venv activation:
Command: bash
Command Arguments: -c "source venv/bin/activate && python load_january_simple.py --date 2025-01-15 --idempotent --exchanges LSE CME NYQ --verbose"
```

### **AWS credential errors:**
**Step 1: Check your shell script has credentials**
```bash
# Verify credentials are in your shell script:
grep "AWS_ACCESS_KEY_ID" /Users/kaushal/Documents/Forestrat/duckdb/run_with_venv.sh
grep "AWS_SECRET_ACCESS_KEY" /Users/kaushal/Documents/Forestrat/duckdb/run_with_venv.sh
```

**Step 2: Test credentials work**
```bash
# Test the shell script manually:
cd /Users/kaushal/Documents/Forestrat/duckdb
./run_with_venv.sh --date 2025-01-15 --idempotent --exchanges LSE --verbose
```

**Step 3: Test AWS access directly**
```bash
# Set credentials in your current shell:
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
aws s3 ls s3://vendor-data-s3/LSEG/TRTH/LSE/ingestion/2025-01-01/
```

### **Script fails but no logs:**
```bash
# Check script manually first:
cd /Users/kaushal/Documents/Forestrat/duckdb
python load_january_simple.py --date 2025-01-15 --idempotent --verbose

# Check NiFi user permissions:
ls -la /Users/kaushal/Documents/Forestrat/duckdb/
```

---

## **💡 PRO TIPS** 

### **For Multiple Dates:**
1. **Create multiple processors** (one per date)
2. **Name them**: "Load 2025-01-01", "Load 2025-01-02", etc.
3. **Run in parallel** or sequence as needed

### **For Scheduling:**
- **Timer driven**: Runs once immediately
- **Cron driven**: Set schedule like `0 0 2 * * ?` (daily at 2 AM)

### **For Monitoring:**
- **Use dashboard**: Much easier than checking logs manually
- **Set up alerts**: Use NiFi's monitoring to alert on failures

---

## **🎯 SUMMARY**

**That's it!** You now have:
✅ **Simple setup** - Just one ExecuteProcess processor  
✅ **Virtual environment** - Uses your existing `venv` with all dependencies  
✅ **Same logs** - Your script logs go to `logs/january_load_simple_*.log`  
✅ **Visual monitoring** - See status in NiFi UI  
✅ **Easy control** - Start/stop via NiFi or shutdown file  
✅ **Flexible parameters** - Change dates/exchanges easily  

**Your script runs exactly the same as command line (with venv), just triggered by NiFi!** 🚀 