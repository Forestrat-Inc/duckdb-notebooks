# DuckDB Data Lake with S3 Integration - Setup Instructions

## Overview

This project implements a complete data lake solution using DuckDB with pandas integration for processing LSEG market data from S3. The architecture follows the medallion pattern (Bronze → Silver → Gold) for data processing and analytics.

## Project Structure

```
/Users/kaushal/Documents/Forestrat/duckdb/
├── config/
│   ├── __init__.py
│   └── settings.py              # Configuration management
├── utils/
│   ├── __init__.py
│   ├── database.py              # DuckDB and S3 management
│   └── data_processing.py       # Data processing utilities
├── notebooks/
│   ├── 01_setup_and_configuration.ipynb
│   ├── 02_data_discovery.ipynb
│   ├── 03_bronze_layer.ipynb
│   ├── 04_silver_layer.ipynb
│   └── 05_gold_layer.ipynb
├── requirements.txt             # Python dependencies
├── .env.example                # Environment variables template
└── README.md                   # This file
```

## Quick Start

### 1. Prerequisites

- Python 3.9 or higher
- AWS credentials configured (for S3 access)
- Jupyter Lab/Notebook
- Access to the S3 bucket `vendor-data-s3`

### 2. Installation

```bash
# Clone or navigate to the project directory
cd /Users/kaushal/Documents/Forestrat/duckdb

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your AWS credentials and configuration
```

### 3. Configure Environment Variables

Edit the `.env` file with your settings:

```bash
# AWS Credentials (choose one method)
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1

# Or use AWS profile
# AWS_PROFILE=your_profile_name

# S3 Configuration
S3_BUCKET=vendor-data-s3
S3_ENDPOINT_URL=https://s3.amazonaws.com

# DuckDB Configuration
DUCKDB_DATABASE_PATH=./data_lake.duckdb
DUCKDB_MEMORY_LIMIT=8GB
DUCKDB_THREADS=4
```

### 4. Run the Notebooks

Execute the notebooks in order:

1. **Setup and Configuration** (`01_setup_and_configuration.ipynb`)
   - Initializes DuckDB connection
   - Tests S3 connectivity
   - Creates database schema
   - Sets up performance optimizations

2. **Data Discovery** (`02_data_discovery.ipynb`)
   - Explores S3 data structure
   - Analyzes schema and data quality
   - Generates data dictionary
   - Provides recommendations

3. **Bronze Layer** (`03_bronze_layer.ipynb`)
   - Creates raw data ingestion layer
   - Implements data lineage tracking
   - Sets up monitoring and quality checks
   - Exports to Parquet format

4. **Silver Layer** (`04_silver_layer.ipynb`)
   - Implements data cleaning and validation
   - Adds data enrichment and technical indicators
   - Creates quality monitoring
   - Optimizes for analytics

5. **Gold Layer** (`05_gold_layer.ipynb`)
   - Creates business-ready aggregations
   - Implements market analytics
   - Builds reporting dashboards
   - Optimizes for query performance

## Architecture Overview

### Bronze Layer (Raw Data)
- **Purpose**: Store raw data from S3 with minimal transformation
- **Tables**: `bronze.lse_market_data_raw`, `bronze.ingestion_metadata`
- **Features**: Data lineage, ingestion tracking, error handling

### Silver Layer (Cleaned Data)
- **Purpose**: Clean, validate, and enrich data for analytics
- **Tables**: `silver.lse_market_data_clean`, `silver.lse_market_data_enriched`
- **Features**: Data quality scoring, validation rules, technical indicators

### Gold Layer (Business Ready)
- **Purpose**: Aggregated, business-ready data for reporting
- **Tables**: `gold.daily_market_stats`, `gold.symbol_performance`, `gold.market_dashboard`
- **Features**: KPIs, market analytics, performance metrics

## Key Features

### 🔧 **Technical Capabilities**
- **DuckDB Integration**: High-performance analytical database
- **S3 Native Support**: Direct querying of S3 data
- **Pandas Integration**: Advanced data processing capabilities
- **Parquet Optimization**: Columnar storage for better performance
- **Incremental Processing**: Efficient data pipeline execution

### 📊 **Analytics Features**
- **Market Statistics**: Daily OHLC, volume, volatility metrics
- **Performance Analysis**: Symbol ranking and categorization
- **Trading Patterns**: Intraday and session analysis
- **Technical Indicators**: SMA, EMA, RSI, MACD, Bollinger Bands
- **Quality Monitoring**: Data quality scoring and alerts

### 🏗️ **Data Engineering**
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Data Lineage**: Complete audit trail from source to analytics
- **Quality Framework**: Comprehensive data validation and monitoring
- **Performance Optimization**: Indexes, materialized views, query tuning

## Configuration Options

### DuckDB Settings
```python
# Memory and performance
DUCKDB_MEMORY_LIMIT = "8GB"      # Adjust based on available RAM
DUCKDB_THREADS = 4               # Adjust based on CPU cores

# Processing
BATCH_SIZE = 100000              # Records per batch
MAX_MEMORY_USAGE = "4GB"         # Max memory for operations
```

### S3 Configuration
```python
# S3 paths
BASE_S3_PATH = "s3://vendor-data-s3/LSEG/TRTH/LSE"
INGESTION_PATH = f"{BASE_S3_PATH}/ingestion"
TRANSFORMATION_PATH = f"{BASE_S3_PATH}/transformation"
```

## Usage Examples

### Basic Data Query
```python
from utils import db_manager

# Query latest market data
latest_data = db_manager.execute_query("""
    SELECT symbol, trade_date, close_price, total_volume
    FROM gold.daily_market_stats
    WHERE trade_date = (SELECT MAX(trade_date) FROM gold.daily_market_stats)
    ORDER BY total_volume DESC
    LIMIT 10
""")
print(latest_data)
```

### Market Analysis
```python
# Get top performers
top_performers = db_manager.execute_query("""
    SELECT symbol, period_return_pct, volatility, volume_rank
    FROM gold.top_performers
    LIMIT 20
""")

# Market dashboard
dashboard = db_manager.execute_query("""
    SELECT * FROM gold.market_dashboard
    ORDER BY report_date DESC
    LIMIT 5
""")
```

### Data Quality Check
```python
# Check data quality
quality_report = db_manager.execute_query("""
    SELECT 
        trade_date,
        avg_quality_score,
        high_quality_trades,
        total_records
    FROM silver.quality_monitoring
    ORDER BY trade_date DESC
    LIMIT 10
""")
```

## Troubleshooting

### Common Issues

#### 1. S3 Connection Failed
- **Check AWS credentials**: Ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are correct
- **Verify S3 permissions**: Ensure read access to the `vendor-data-s3` bucket
- **Test connectivity**: Run the S3 test in the setup notebook

#### 2. Memory Issues
- **Reduce memory limits**: Lower `DUCKDB_MEMORY_LIMIT` and `MAX_MEMORY_USAGE`
- **Process smaller batches**: Reduce `BATCH_SIZE`
- **Close unused connections**: Call `db_manager.close()` when done

#### 3. Performance Issues
- **Check indexes**: Ensure proper indexes are created
- **Optimize queries**: Use the performance monitoring views
- **Update statistics**: Run `ANALYZE` on large tables

#### 4. Data Quality Issues
- **Check validation rules**: Review `silver.validation_rules`
- **Monitor quality scores**: Use `silver.quality_monitoring`
- **Review transformation logs**: Check `silver.transformation_log`

### Getting Help

1. **Check the logs**: Look in `./logs/datalake.log` for detailed error messages
2. **Review configuration**: Ensure all environment variables are set correctly
3. **Test components**: Run individual notebook cells to isolate issues
4. **Monitor resources**: Check available memory and disk space

## Performance Tuning

### DuckDB Optimization
```sql
-- Recommended settings for large datasets
SET memory_limit='16GB';
SET threads=8;
SET enable_progress_bar=true;
SET enable_object_cache=true;
```

### Query Optimization
- Use appropriate indexes on frequently queried columns
- Partition large tables by date
- Use materialized views for complex aggregations
- Leverage DuckDB's columnar storage with Parquet

### S3 Optimization
- Use Parquet format for better compression and performance
- Implement proper partitioning strategy
- Consider data locality and region settings

## Monitoring and Maintenance

### Data Quality Monitoring
```sql
-- Daily data quality check
SELECT * FROM silver.quality_monitoring 
WHERE trade_date >= CURRENT_DATE - 7;

-- Performance metrics
SELECT * FROM bronze.performance_metrics
WHERE processing_date >= CURRENT_DATE - 7;
```

### System Health
```sql
-- Database size and performance
SELECT 
    table_schema,
    table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## Advanced Usage

### Custom Technical Indicators
```python
from utils import data_processor

# Add custom indicators to market data
market_data = db_manager.execute_query("SELECT * FROM silver.lse_market_data_enriched")
enhanced_data = data_processor.add_technical_indicators(market_data, price_col='price')
```

### Batch Processing
```python
# Process multiple dates
for date in date_range:
    result = db_manager.execute_query(f"SELECT silver.clean_market_data('{date}')")
    print(f"Processed {date}: {result}")
```

### Export for BI Tools
```python
# Export to various formats
db_manager.execute_sql("""
    COPY (SELECT * FROM gold.market_dashboard) 
    TO 'market_dashboard.csv' (HEADER, DELIMITER ',')
""")
```

## Security Considerations

- **Credentials**: Never commit AWS credentials to version control
- **Access Control**: Use IAM roles with minimal required permissions
- **Encryption**: Enable S3 server-side encryption
- **Network**: Consider VPC endpoints for S3 access
- **Monitoring**: Log all data access and modifications

## Contributing

To extend the data lake:

1. **Add new data sources**: Create new Bronze layer tables
2. **Enhance processing**: Add validation rules and transformations
3. **Expand analytics**: Create new Gold layer aggregations
4. **Improve performance**: Add indexes and optimize queries
5. **Add monitoring**: Create new quality checks and alerts

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the notebook outputs for error details
3. Examine the log files for detailed error messages
4. Test individual components to isolate problems

---

**Note**: This data lake is designed for financial market data analysis. Ensure compliance with data usage agreements and regulations when processing financial data.
