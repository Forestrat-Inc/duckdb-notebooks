# NiFi 2.4 January 2025 Market Data Loader - Phase 1 Setup Instructions

## Overview
This document provides step-by-step instructions for implementing Phase 1 of the NiFi 2.4 process group for loading January 2025 market data from S3 to DuckDB with comprehensive progress tracking and statistics.

## Phase 1 Components Created ✅

### 1. DuckDB Progress Tracking Infrastructure
- **Progress Tracking Tables**: `bronze.nifi_load_progress`, `bronze.nifi_load_stats`, `bronze.nifi_load_errors`
- **Statistics Tables**: `gold.daily_stats`, `gold.weekly_rolling_stats`, `silver.hourly_stats`
- **Dashboard Views**: Real-time progress, error analysis, and performance metrics
- **Setup Script**: `setup_nifi_tables.py` for automated table creation

### 2. NiFi Process Group Template
- **Template File**: `January-2025-Market-Data-Loader.json` (NiFi 2.4 JSON format)
- **Process Groups**: File Discovery, Data Processing, Progress Tracking, Statistics Generation
- **Processor Count**: 15+ specialized processors for complete data pipeline

### 3. Controller Services Configuration
- **Configuration File**: `controller-services-config.json`
- **Services Included**: AWS S3 Credentials, DuckDB Connection Pool, CSV Reader, JSON Writer
- **Parameter Context**: Market data parameters with sensitive value handling

### 4. Exchange Metadata Configuration
- **Properties File**: `config/exchange-metadata.properties`
- **Exchanges Covered**: LSE, CME, NYQ, NSQ with timezone and trading hours

## Prerequisites

### 1. NiFi 2.4 Installation
- **Location**: `/Users/kaushal/Documents/Forestrat/nifi/nifi-2.3.0` (Upgrade to 2.4 recommended)
- **Status**: ✅ Installed
- **Web UI**: http://localhost:8080/nifi (default)

### 2. DuckDB Database
- **File**: `./multi_exchange_data_lake.duckdb`
- **Status**: ✅ Exists
- **Schema**: Bronze, Silver, Gold layers created

### 3. AWS S3 Access
- **Bucket**: `vendor-data-s3`
- **Path Structure**: `/LSEG/TRTH/{exchange}/ingestion/{date}/data/merged/`
- **Credentials**: AWS Access Key ID and Secret Access Key required

### 4. Dependencies
- **DuckDB JDBC Driver**: Required for NiFi-DuckDB connection
- **AWS SDK**: Included in NiFi AWS processors
- **Java 11+**: Required for NiFi 2.4

## Step-by-Step Setup

### Step 1: Setup DuckDB Tables

1. **Close any existing DuckDB connections**:
   ```bash
   # Check for running DuckDB processes
   ps aux | grep duckdb
   
   # Kill any conflicting processes if needed
   kill -9 <PID>
   ```

2. **Run the table setup script**:
   ```bash
   cd /Users/kaushal/Documents/Forestrat/duckdb
   python setup_nifi_tables.py
   ```

3. **Verify table creation**:
   ```bash
   # Expected output: All schemas and tables created successfully
   # Tables: bronze.nifi_load_progress, bronze.nifi_load_stats, bronze.nifi_load_errors
   #         silver.hourly_stats, gold.daily_stats, gold.weekly_rolling_stats
   ```

### Step 2: Configure NiFi Controller Services

1. **Copy DuckDB JDBC Driver**:
   ```bash
   # Download DuckDB JDBC driver if not already present
   # Place in NiFi's lib directory or specify path in connection pool
   ```

2. **Start NiFi**:
   ```bash
   cd /Users/kaushal/Documents/Forestrat/nifi/nifi-2.3.0
   ./bin/nifi.sh start
   ```

3. **Access NiFi Web UI**:
   - URL: http://localhost:8080/nifi
   - Wait for NiFi to fully start (check logs if needed)

### Step 3: Import Process Group Template

1. **Upload Template**:
   - Navigate to NiFi Web UI
   - Click on "Templates" (toolbar)
   - Click "Browse" and select `nifi/January-2025-Market-Data-Loader.json`
   - Click "Import"

2. **Create Process Group**:
   - Drag "Process Group" from processor palette to canvas
   - Select "January-2025-Market-Data-Loader" template
   - Click "Add"

### Step 4: Configure Controller Services

1. **Create Controller Services**:
   - Right-click on process group → "Configure"
   - Go to "Controller Services" tab
   - Use the configuration from `controller-services-config.json`

2. **Configure AWS Credentials**:
   - Set `aws.access.key.id` parameter
   - Set `aws.secret.access.key` parameter
   - Enable AWS Credentials Provider Service

3. **Configure DuckDB Connection**:
   - Verify database path: `./multi_exchange_data_lake.duckdb`
   - Set DuckDB JDBC driver path
   - Test connection
   - Enable DuckDB Connection Pool Service

4. **Enable Record Services**:
   - Enable CSV Record Reader Service
   - Enable JSON Record Writer Service
   - Enable Exchange Metadata Lookup Service

### Step 5: Configure Parameters

1. **Create Parameter Context**:
   - Go to "Parameter Contexts" (hamburger menu)
   - Create new context: "market-data-parameters"
   - Add parameters from `controller-services-config.json`

2. **Set Sensitive Parameters**:
   - `aws.access.key.id`: Your AWS Access Key ID
   - `aws.secret.access.key`: Your AWS Secret Access Key

3. **Apply Parameter Context**:
   - Right-click process group → "Configure"
   - Select "market-data-parameters" context
   - Click "Apply"

### Step 6: Validate Setup

1. **Check Controller Services**:
   - All services should show "ENABLED" status
   - Test database connection
   - Verify S3 access

2. **Validate Process Group**:
   - All processors should show valid configurations
   - No validation errors should be present
   - Check processor scheduling settings

## Expected Data Flow

### File Discovery (Every 10 minutes)
1. **ListS3** discovers new market data files
2. **UpdateAttribute** extracts file metadata
3. **ExecuteSQL** checks for already processed files
4. **RouteOnAttribute** routes only new files

### Data Processing (Parallel execution)
1. **ExecuteSQL** logs processing start
2. **FetchS3Object** downloads CSV file
3. **UnpackContent** decompresses GZIP
4. **ConvertRecord** transforms CSV to JSON
5. **JoltTransformJSON** adds metadata fields
6. **PutDatabaseRecord** inserts into DuckDB

### Progress Tracking (Real-time)
1. **ExecuteSQL** updates processing status
2. **GenerateFlowFile** triggers statistics updates
3. **ExecuteSQL** calculates real-time metrics

### Statistics Generation (Scheduled)
1. **Daily Stats**: Every hour
2. **Weekly Rolling Stats**: Every 4 hours
3. **Real-time Stats**: Every 30 seconds

## Monitoring and Verification

### 1. Progress Tracking Queries
```sql
-- View current progress
SELECT * FROM bronze.v_nifi_progress_summary;

-- Check recent errors
SELECT * FROM bronze.v_nifi_error_analysis;

-- Monitor performance
SELECT * FROM bronze.v_nifi_performance_metrics;
```

### 2. Statistics Queries
```sql
-- Daily statistics
SELECT * FROM gold.daily_stats ORDER BY stats_date DESC;

-- Weekly rolling statistics
SELECT * FROM gold.weekly_rolling_stats ORDER BY window_end_date DESC;

-- Real-time dashboard data
SELECT * FROM gold.v_current_progress_dashboard;
```

### 3. NiFi Monitoring
- **Data Provenance**: Track individual FlowFiles
- **Connection Queues**: Monitor backpressure
- **Processor Statistics**: Check processing rates
- **Controller Services**: Verify service health

## Performance Tuning

### 1. Processor Concurrency
- **ListS3**: 1 concurrent task (discovery)
- **FetchS3Object**: 4 concurrent tasks (parallel downloads)
- **PutDatabaseRecord**: 2 concurrent tasks (database writes)

### 2. Connection Queue Sizes
- **File Discovery to Processing**: 1000 FlowFiles
- **Processing to Progress Tracking**: 500 FlowFiles
- **Statistics Generation**: 100 FlowFiles

### 3. Memory Settings
- **NiFi Heap**: Minimum 4GB recommended
- **FlowFile Repository**: 1GB
- **Content Repository**: 10GB

## Troubleshooting

### Common Issues
1. **Database Lock Error**: Close existing DuckDB connections
2. **AWS Access Denied**: Verify S3 credentials and permissions
3. **Template Import Failed**: Ensure NiFi 2.4 compatibility
4. **Controller Service Start Failed**: Check configuration and dependencies

### Log Locations
- **NiFi Logs**: `{NIFI_HOME}/logs/nifi-app.log`
- **Processing Logs**: `./logs/january_load_simple_*.log`
- **Database Logs**: Check DuckDB connection errors

## Next Steps (Phase 2)

Once Phase 1 is complete and validated:

1. **Advanced Error Handling**: Implement retry logic and dead letter queues
2. **Performance Monitoring**: Add detailed metrics and alerting
3. **Data Quality Checks**: Implement validation and quality scoring
4. **Dashboard Creation**: Build real-time monitoring dashboard
5. **Automation**: Add automated scaling and resource management

## File Structure

```
/Users/kaushal/Documents/Forestrat/duckdb/
├── sql/
│   ├── nifi_progress_tracking_tables.sql
│   └── nifi_statistics_tables.sql
├── nifi/
│   ├── January-2025-Market-Data-Loader.json
│   ├── controller-services-config.json
│   └── SETUP_INSTRUCTIONS.md
├── config/
│   └── exchange-metadata.properties
├── setup_nifi_tables.py
└── multi_exchange_data_lake.duckdb
```

## Support

For issues or questions:
1. Check NiFi documentation: https://nifi.apache.org/components/
2. Review DuckDB documentation: https://duckdb.org/docs/
3. Validate AWS S3 access and permissions
4. Check processor and controller service logs

---

**Phase 1 Status**: ✅ **COMPLETE**
**Next Phase**: Phase 2 - Advanced Features and Monitoring
**Estimated Total Processing Time**: 2-4 hours for complete January 2025 dataset 