# 📊 January 2025 Market Data Loading System

A comprehensive system for loading January 2025 market data from S3 across multiple exchanges (LSE, CME, NYQ) with real-time monitoring and NiFi workflow integration.

## 🎯 **Quick Start**

### **Option 1: Single Date Processing (Recommended)**
```bash
# 1. Start monitoring dashboard
cd nifi
./start_dashboard.sh

# 2. In another terminal, process a specific date
python load_january_simple.py --date 2025-01-15 --idempotent

# 3. Monitor via web browser
open http://localhost:12345
```

### **Option 2: Parallel Date Processing**
```bash
# Process multiple dates in parallel
python load_january_simple.py --date 2025-01-01 --idempotent &
python load_january_simple.py --date 2025-01-02 --idempotent &
python load_january_simple.py --date 2025-01-03 --idempotent &
python load_january_simple.py --date 2025-01-04 --idempotent &
wait  # Wait for all to complete

# Process full month in parallel
for date in {01..31}; do
  python load_january_simple.py --date 2025-01-$date --idempotent &
done
wait
```

## 📁 **Project Structure**

```
duckdb/
├── 📈 Core Data Loading Scripts
│   ├── load_january_simple.py          # Main data loading script
│   ├── multi_exchange_data_lake.duckdb  # Database (auto-created)
│   └── logs/                            # Application logs
│
├── 🔧 NiFi Integration & Dashboard
│   ├── nifi/
│   │   ├── dashboard_app.py             # Web monitoring dashboard
│   │   ├── Simple-January-2025-Loader.json  # NiFi process group
│   │   ├── start_dashboard.sh           # Dashboard startup script
│   │   ├── README.md                    # Comprehensive documentation
│   │   └── DASHBOARD_README.md          # Dashboard-specific guide
│   │
│   └── Other Files...
│
├── ⚙️ Configuration & Utilities
│   ├── config/                          # System configuration
│   ├── utils/                           # Database and processing utilities
│   └── requirements.txt                 # Python dependencies
│
└── 📊 Jupyter Notebooks
    ├── 01_setup_and_configuration.ipynb
    ├── 02_data_discovery.ipynb
    └── ... (other analysis notebooks)
```

## 🚀 **Features**

✅ **Single-Date Processing**: Process one date at a time for better control and parallelization  
✅ **Parallel Execution**: Run multiple instances simultaneously for different dates  
✅ **Graceful Shutdown**: Multiple interrupt methods (Ctrl+C, file-based, dashboard)  
✅ **Idempotent Processing**: Resume interrupted loads exactly where you left off  
✅ **Real-time Monitoring**: Web dashboard with live progress tracking  
✅ **Multi-Exchange Support**: LSE, CME, NYQ with individual progress tracking  
✅ **NiFi Integration**: Professional workflow management templates  
✅ **Transaction Safety**: Atomic database operations prevent data corruption  
✅ **Comprehensive Statistics**: Daily, weekly, and performance analytics  

## 📖 **Documentation**

- **📊 Complete System Guide**: [`nifi/README.md`](nifi/README.md) - Comprehensive documentation
- **🎮 Dashboard Guide**: [`nifi/DASHBOARD_README.md`](nifi/DASHBOARD_README.md) - Web dashboard documentation
- **🔧 Script Options**: `python load_january_simple.py --help` - Command line options

## 🛠️ **Installation**

```bash
# Install dependencies
pip install -r requirements.txt

# Create necessary directories
mkdir -p logs

# Configure AWS credentials for S3 access
aws configure
```

## 🎮 **Usage Examples**

### **Basic Data Loading**
```bash
# Load specific date (idempotent - safe to restart)
python load_january_simple.py --date 2025-01-15 --idempotent

# Load specific exchanges for a date
python load_january_simple.py --date 2025-01-15 --idempotent --exchanges LSE CME

# Load with verbose logging
python load_january_simple.py --date 2025-01-15 --idempotent --verbose
```

### **With Real-time Monitoring**
```bash
# Terminal 1: Start dashboard
cd nifi
./start_dashboard.sh

# Terminal 2: Process single date with monitoring
python load_january_simple.py --date 2025-01-15 --idempotent --verbose

# Terminal 3: Process multiple dates in parallel
python load_january_simple.py --date 2025-01-01 --idempotent &
python load_january_simple.py --date 2025-01-02 --idempotent &
python load_january_simple.py --date 2025-01-03 --idempotent &

# Monitor: http://localhost:12345
```

### **Shutdown Control**
```bash
# Graceful shutdown via command (affects all running instances)
python load_january_simple.py --create-shutdown-file

# Remove shutdown signal
python load_january_simple.py --remove-shutdown-file

# Check shutdown status
python load_january_simple.py --check-shutdown-file

# Or use dashboard: http://localhost:12345
```

## 🔄 **Data Flow**

1. **Source**: S3 bucket `vendor-data-s3` with market data files
2. **Processing**: Python script with transaction-safe loading
3. **Storage**: DuckDB database with bronze/silver/gold schemas
4. **Monitoring**: Real-time web dashboard
5. **Workflow**: Optional NiFi process group integration

## 🚨 **Quick Troubleshooting**

- **"Data already exists"**: Use `--idempotent` flag
- **Dashboard not loading**: Check `cd nifi && ./start_dashboard.sh`
- **S3 connection issues**: Verify `aws configure` and credentials
- **Script stuck**: Use dashboard shutdown or `--create-shutdown-file`

## 📊 **System Monitoring**

### **Database Queries**
```sql
-- Check progress
SELECT * FROM bronze.load_progress ORDER BY start_time DESC;

-- Daily statistics
SELECT * FROM gold.daily_load_stats ORDER BY stats_date, exchange;

-- Performance metrics
SELECT exchange, AVG(records_loaded) as avg_records
FROM bronze.load_progress WHERE status = 'completed' GROUP BY exchange;
```

### **Web Dashboard**
- **URL**: http://localhost:12345
- **Features**: Real-time progress, charts, control buttons, error tracking
- **API**: REST endpoints for programmatic access

## 📞 **Support**

- **📋 Full Documentation**: See [`nifi/README.md`](nifi/README.md)
- **🎮 Dashboard Help**: See [`nifi/DASHBOARD_README.md`](nifi/DASHBOARD_README.md)
- **🧪 Test Features**: `cd nifi && python test_shutdown_functionality.py`
- **📊 Command Help**: `python load_january_simple.py --help`

**Ready to load January 2025 market data with comprehensive monitoring and control!** 🚀
