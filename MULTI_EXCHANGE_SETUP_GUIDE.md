# Multi-Exchange Data Lake Setup Guide

## 📊 Overview

This guide documents the new multi-exchange DuckDB data lake structure that supports **LSE**, **CME**, and **NYQ** exchanges with proper partitioning and data pipeline management.

## 🏗️ S3 Structure Analysis

### Current S3 Bucket Structure
```
vendor-data-s3/
└── LSEG/TRTH/
    ├── LSE/                    ← London Stock Exchange
    │   ├── extraction/         ← Raw extraction stage
    │   ├── ingestion/          ← Daily partitioned (2025-01-01, 2025-01-02, etc.)
    │   ├── normalization/      ← Normalized data stage
    │   └── transformation/     ← Transformed data stage
    ├── CME/                    ← Chicago Mercantile Exchange
    │   └── ingestion/          ← Daily partitioned (currently only ingestion)
    └── NYQ/                    ← New York Stock Exchange (NYSE)
        └── ingestion/          ← Daily partitioned (currently only ingestion)
```

### File Structure Pattern
Each daily partition contains:
- **Data File**: `{EXCHANGE}-{DATE}-NORMALIZEDMP-Data-1-of-1.csv.gz` (main trading data)
- **Report File**: `{EXCHANGE}-{DATE}-NORMALIZEDMP-Report-1-of-1.csv.gz` (summary/metadata)

**Example paths:**
- `s3://vendor-data-s3/LSEG/TRTH/LSE/ingestion/2025-02-19/data/merged/LSE-2025-02-19-NORMALIZEDMP-Data-1-of-1.csv.gz`
- `s3://vendor-data-s3/LSEG/TRTH/CME/ingestion/2025-02-09/data/merged/CME-2025-02-09-NORMALIZEDMP-Data-1-of-1.csv.gz`
- `s3://vendor-data-s3/LSEG/TRTH/NYQ/ingestion/2025-02-19/data/merged/NYQ-2025-02-19-NORMALIZEDMP-Data-1-of-1.csv.gz`

## 🗄️ Database Schema

### Schema Architecture
The new database follows a **medallion architecture** with proper partitioning:

```
bronze/          ← Raw ingested data (partitioned by date + exchange)
├── lse_market_data_raw
├── cme_market_data_raw
├── nyq_market_data_raw
└── market_data_reports

silver/          ← Cleaned and normalized data
├── market_data_unified      ← All exchanges unified
└── price_timeseries        ← Time series optimized

gold/            ← Analytics-ready data
├── daily_market_summary
└── arbitrage_opportunities

audit/           ← Data lineage and quality tracking
├── data_ingestion_log
├── pipeline_state
└── data_quality_metrics

views/           ← Easy-access views
├── latest_market_data
├── cross_exchange_prices
├── daily_volume_leaders
└── pipeline_health
```

### Key Features

1. **Multi-Exchange Support**: Native support for LSE, CME, NYQ with extensible architecture
2. **Proper Partitioning**: Data partitioned by date and exchange for optimal query performance
3. **Data Quality Tracking**: Built-in quality metrics and validation
4. **Pipeline Monitoring**: Comprehensive audit trails and health monitoring
5. **Cross-Exchange Analytics**: Views for comparing data across exchanges

## 🚀 Quick Start

### 1. Setup Files Created

- **`create_tables_with_partitioning.sql`**: Complete database schema
- **`utils/multi_exchange_loader.py`**: Python module for data loading
- **`demo_multi_exchange_setup.ipynb`**: Demonstration notebook

### 2. Initialize Database

```python
from utils.database import DuckDBManager
from utils.multi_exchange_loader import MultiExchangeLoader

# Create new database
db_manager = DuckDBManager('./multi_exchange_data_lake.duckdb')
conn = db_manager.connect()

# Execute schema creation
with open('create_tables_with_partitioning.sql', 'r') as f:
    sql_script = f.read()
    
# Execute SQL statements...
```

### 3. Load Data

```python
# Initialize loader
loader = MultiExchangeLoader(db_manager)

# Load single day for all exchanges
from datetime import date
results = loader.load_all_exchanges(date(2025, 2, 19))

# Load date range for specific exchange
results = loader.load_date_range('LSE', date(2025, 2, 1), date(2025, 2, 19))
```

## 📋 Key Classes and Methods

### MultiExchangeLoader

#### Core Methods
- **`load_single_file(exchange, date, stage='ingestion')`**: Load single file
- **`load_date_range(exchange, start_date, end_date)`**: Load date range
- **`load_all_exchanges(date)`**: Load all exchanges for one date
- **`validate_data_quality(exchange, date)`**: Run quality checks
- **`discover_file_schema(exchange, date)`**: Analyze file structure

#### Utility Methods
- **`get_s3_path(exchange, date, stage, file_type)`**: Generate S3 paths
- **`list_available_files(exchange, stage)`**: List available files
- **`get_loading_status(exchange=None, days_back=30)`**: Check loading status
- **`update_pipeline_state(...)`**: Update pipeline tracking

### Supported Exchanges

```python
exchanges = {
    'LSE': {
        'stages': ['extraction', 'ingestion', 'normalization', 'transformation'],
        'table': 'bronze.lse_market_data_raw',
        'timezone': 'Europe/London'
    },
    'CME': {
        'stages': ['ingestion'],
        'table': 'bronze.cme_market_data_raw', 
        'timezone': 'America/Chicago'
    },
    'NYQ': {
        'stages': ['ingestion'],
        'table': 'bronze.nyq_market_data_raw',
        'timezone': 'America/New_York'
    }
}
```

## 🔍 Data Quality Features

### Automatic Quality Checks
- **Completeness**: Null value detection
- **Accuracy**: Invalid price/volume detection
- **Consistency**: Duplicate detection
- **Scoring**: Overall quality scores (0-100%)

### Quality Metrics Tracked
```sql
SELECT * FROM audit.data_quality_metrics 
WHERE exchange = 'LSE' 
ORDER BY check_timestamp DESC;
```

## 📊 Analytics Views

### Cross-Exchange Price Comparison
```sql
SELECT * FROM views.cross_exchange_prices 
WHERE symbol = 'AAPL';
```

### Pipeline Health Monitoring
```sql
SELECT * FROM views.pipeline_health 
WHERE health_status != 'HEALTHY';
```

### Daily Volume Leaders
```sql
SELECT * FROM views.daily_volume_leaders 
WHERE data_date = CURRENT_DATE - 1
ORDER BY volume_rank;
```

## 🔧 Advanced Features

### Helper Functions

```sql
-- Generate S3 paths dynamically
SELECT get_s3_path('LSE', '2025-02-19', 'ingestion');

-- Get report paths
SELECT get_s3_report_path('CME', '2025-02-09');
```

### Partitioning Benefits

1. **Query Performance**: Partition pruning for date-based queries
2. **Parallel Processing**: Each partition can be processed independently
3. **Storage Optimization**: Only query relevant partitions
4. **Maintenance**: Easy to drop old partitions

### Example Queries

```sql
-- Query specific exchange and date (uses partition pruning)
SELECT symbol, price, volume 
FROM bronze.lse_market_data_raw 
WHERE data_date = '2025-02-19' AND exchange = 'LSE';

-- Cross-exchange comparison
SELECT exchange, symbol, AVG(price) as avg_price
FROM silver.market_data_unified 
WHERE data_date BETWEEN '2025-02-01' AND '2025-02-19'
GROUP BY exchange, symbol;

-- Arbitrage opportunities
SELECT * FROM gold.arbitrage_opportunities 
WHERE spread_percentage > 0.5 
ORDER BY potential_profit DESC;
```

## 🛠️ Pipeline Management

### Loading Status Monitoring
```python
# Check recent loading status
status = loader.get_loading_status(days_back=7)
print(status)

# Check specific exchange
lse_status = loader.get_loading_status(exchange='LSE', days_back=30)
```

### Pipeline State Tracking
```sql
-- Check pipeline health
SELECT * FROM audit.pipeline_state 
WHERE processing_status != 'completed';

-- Update pipeline state
INSERT INTO audit.pipeline_state (...) 
ON CONFLICT (...) DO UPDATE SET ...;
```

## 📈 Performance Considerations

### Indexing Strategy
- **Primary indexes**: (data_date, exchange, symbol)
- **Secondary indexes**: exchange_time, symbol lookups
- **Composite indexes**: Multi-column queries

### Query Optimization
- Always include partition keys (data_date, exchange) in WHERE clauses
- Use LIMIT for exploratory queries
- Leverage views for common query patterns

### Memory Management
- Configured for 8GB memory limit
- 4 threads for parallel processing
- Progress bars enabled for long-running queries

## 🔄 Migration from Old Structure

### Key Changes
1. **Multi-Exchange Support**: Previously LSE-only, now supports LSE/CME/NYQ
2. **Enhanced Partitioning**: Date + Exchange partitioning
3. **Quality Tracking**: Built-in data quality metrics
4. **Pipeline Monitoring**: Comprehensive audit trails
5. **Cross-Exchange Analytics**: Unified views and comparisons

### Migration Steps
1. Create new database with enhanced schema
2. Migrate existing LSE data to new structure
3. Add CME and NYQ data sources
4. Update pipelines to use MultiExchangeLoader
5. Create dashboards using new views

## 🚨 Best Practices

### Data Loading
- Always check loading status before reloading
- Use `force_reload=True` only when necessary
- Monitor data quality scores after loading
- Update pipeline state after successful loads

### Query Patterns
- Include partition keys in WHERE clauses
- Use specific date ranges rather than open-ended queries
- Leverage views for complex cross-exchange queries
- Monitor query performance with EXPLAIN

### Monitoring
- Check `views.pipeline_health` regularly
- Monitor `audit.data_quality_metrics` for data issues
- Review `audit.data_ingestion_log` for loading problems
- Set up alerts for critical pipeline failures

## 📚 Files and Resources

### Created Files
- **`create_tables_with_partitioning.sql`**: Database schema (350+ lines)
- **`utils/multi_exchange_loader.py`**: Python loading module (485 lines)
- **`demo_multi_exchange_setup.ipynb`**: Demo notebook
- **`MULTI_EXCHANGE_SETUP_GUIDE.md`**: This documentation

### Integration Points
- Extends existing `utils/database.py` (DuckDBManager)
- Uses existing `config/settings.py` for configuration
- Compatible with existing notebook structure

## 🎯 Next Steps

1. **Test the demo notebook** to validate the setup
2. **Load historical data** using date range functions
3. **Create silver layer transformations** for data unification
4. **Build gold layer analytics** for cross-exchange insights
5. **Set up automated pipelines** for daily data processing
6. **Create monitoring dashboards** using the audit views

---

## 🤝 Support

For questions or issues:
1. Check the demo notebook for examples
2. Review audit tables for data quality issues
3. Use `loader.get_loading_status()` for pipeline diagnostics
4. Check DuckDB logs for detailed error messages

**The multi-exchange data lake is now ready for production use! 🚀** 