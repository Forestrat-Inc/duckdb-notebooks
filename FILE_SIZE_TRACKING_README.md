# File Size Tracking Enhancement

This document describes the new file size tracking functionality added to the `bronze.load_progress` table and related statistics tables.

## Overview

The enhancement adds file size information to track the volume of data being processed, providing better insights into:
- File size distributions across exchanges
- Data volume trends over time
- Performance correlations between file size and processing time
- Storage and bandwidth monitoring

## Changes Made

### 1. Database Schema Updates

#### `bronze.load_progress` Table
- **New Column**: `file_size_bytes BIGINT` - Stores the size of the source file in bytes
- **Index**: Added for improved query performance on file size filters

#### `gold.daily_load_stats` Table
- **New Column**: `total_file_size_bytes BIGINT` - Sum of all file sizes processed in a day
- **New Column**: `avg_file_size_bytes DECIMAL(20,2)` - Average file size for the day

#### `gold.weekly_load_stats` Table
- **New Column**: `avg_daily_file_size_bytes DECIMAL(20,2)` - Average daily file size for the week
- **New Column**: `total_file_size_bytes BIGINT` - Total file size for the week

### 2. Code Changes

#### `load_january_simple.py`
- Imports `S3Manager` for file metadata retrieval
- Initializes S3Manager in the loader class
- Added `get_file_size_bytes()` method to retrieve file size from S3
- Updated progress tracking to include file size information
- Enhanced logging to display file sizes during processing

#### `utils/supabase_manager.py`
- Updated `insert_progress_record()` to accept `file_size_bytes` parameter
- Modified statistics aggregation queries to include file size calculations
- Updated table creation methods for new schema

## Migration for Existing Installations

If you have existing data, run the migration script to add the new columns:

```sql
-- Run this script on both DuckDB and Supabase
.execute add_file_size_column.sql
```

### For DuckDB:
```bash
duckdb your_database.duckdb < add_file_size_column.sql
```

### For Supabase:
```bash
psql "your_supabase_connection_string" -f add_file_size_column.sql
```

## Usage Examples

### 1. View File Sizes in Progress Records

```sql
SELECT 
    exchange, 
    data_date, 
    file_path,
    file_size_bytes,
    ROUND(file_size_bytes / 1024.0 / 1024.0, 2) as file_size_mb,
    records_loaded,
    status
FROM bronze.load_progress 
WHERE file_size_bytes IS NOT NULL
ORDER BY file_size_bytes DESC
LIMIT 10;
```

### 2. Daily File Size Statistics

```sql
SELECT 
    stats_date, 
    exchange,
    total_files,
    total_records,
    total_file_size_bytes,
    ROUND(total_file_size_bytes / 1024.0 / 1024.0, 2) as total_size_mb,
    ROUND(avg_file_size_bytes / 1024.0 / 1024.0, 2) as avg_file_size_mb
FROM gold.daily_load_stats
WHERE total_file_size_bytes > 0
ORDER BY stats_date DESC;
```

### 3. Find Largest Files by Exchange

```sql
SELECT 
    exchange,
    COUNT(*) as file_count,
    AVG(file_size_bytes) as avg_size_bytes,
    MIN(file_size_bytes) as min_size_bytes,
    MAX(file_size_bytes) as max_size_bytes,
    ROUND(AVG(file_size_bytes) / 1024.0 / 1024.0, 2) as avg_size_mb
FROM bronze.load_progress 
WHERE file_size_bytes IS NOT NULL
GROUP BY exchange
ORDER BY avg_size_bytes DESC;
```

### 4. Processing Performance vs File Size

```sql
SELECT 
    exchange,
    CASE 
        WHEN file_size_bytes < 50*1024*1024 THEN 'Small (<50MB)'
        WHEN file_size_bytes < 200*1024*1024 THEN 'Medium (50-200MB)'
        ELSE 'Large (>200MB)'
    END as size_category,
    COUNT(*) as file_count,
    AVG(records_loaded) as avg_records,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_processing_seconds
FROM bronze.load_progress 
WHERE file_size_bytes IS NOT NULL 
AND status = 'completed'
AND end_time IS NOT NULL
GROUP BY exchange, size_category
ORDER BY exchange, size_category;
```

## Monitoring and Alerts

### Key Metrics to Monitor

1. **File Size Trends**: Track if file sizes are growing over time
2. **Processing Efficiency**: Monitor processing time vs file size ratios
3. **Storage Usage**: Estimate storage requirements based on daily file volumes
4. **Network Bandwidth**: Plan bandwidth needs based on file size patterns

### Sample Alert Queries

#### Detect Unusually Large Files
```sql
SELECT * FROM bronze.load_progress 
WHERE file_size_bytes > (
    SELECT AVG(file_size_bytes) + 3 * STDDEV(file_size_bytes) 
    FROM bronze.load_progress 
    WHERE file_size_bytes IS NOT NULL
);
```

#### Track Daily Data Volume Growth
```sql
SELECT 
    stats_date,
    SUM(total_file_size_bytes) as daily_total_bytes,
    LAG(SUM(total_file_size_bytes)) OVER (ORDER BY stats_date) as prev_day_bytes,
    ROUND(
        (SUM(total_file_size_bytes) - LAG(SUM(total_file_size_bytes)) OVER (ORDER BY stats_date)) 
        * 100.0 / LAG(SUM(total_file_size_bytes)) OVER (ORDER BY stats_date), 2
    ) as growth_percentage
FROM gold.daily_load_stats 
WHERE total_file_size_bytes > 0
GROUP BY stats_date
ORDER BY stats_date DESC
LIMIT 30;
```

## Technical Details

### File Size Retrieval
- Uses the existing `S3Manager` class to get file metadata via `get_file_info()`
- File size is retrieved before processing begins
- If file size cannot be determined, the value is stored as NULL
- Processing continues normally even if file size retrieval fails

### Error Handling
- File size retrieval failures are logged as warnings but don't stop processing
- NULL values are handled gracefully in all aggregation queries
- Existing functionality remains unaffected if S3Manager is unavailable

### Performance Impact
- Minimal: One additional S3 HEAD request per file
- S3 HEAD requests are fast and don't transfer file content
- File size retrieval happens before the main data processing

## Future Enhancements

Possible future improvements:
1. Add compression ratio tracking (original vs compressed size)
2. Network transfer time monitoring
3. File size-based processing optimization
4. Automatic archival policies based on file age and size
5. Cost estimation based on storage and processing volumes

## Troubleshooting

### Common Issues

1. **File sizes showing as NULL**
   - Check S3 credentials and permissions
   - Verify S3Manager initialization in logs
   - Ensure S3 paths are correctly formatted

2. **Migration script fails**
   - Check if columns already exist
   - Verify database permissions
   - Run verification queries separately

3. **Statistics not updating**
   - Check that the new columns exist in stats tables
   - Verify that the aggregation methods are being called
   - Review Supabase connection logs

### Debug Commands

```sql
-- Check if file_size_bytes column exists
SELECT column_name FROM information_schema.columns 
WHERE table_name = 'load_progress' AND column_name = 'file_size_bytes';

-- Check recent records with file sizes
SELECT TOP 5 * FROM bronze.load_progress 
WHERE file_size_bytes IS NOT NULL 
ORDER BY created_at DESC;

-- Verify S3Manager functionality (run in Python)
from utils.database import S3Manager
s3_manager = S3Manager()
file_info = s3_manager.get_file_info('LSEG/TRTH/LSE/ingestion/2025-01-15/data/merged/LSE-2025-01-15-NORMALIZEDMP-Data-1-of-1.csv.gz')
print(file_info)
``` 