# DuckDB Performance Optimization Guide

## Current Setup Performance Analysis
- **Storage**: Hybrid (S3 source → Local optimized storage)
- **Database Size**: 1.1GB (highly compressed columnar format)
- **Memory**: 8GB limit configured
- **Threads**: 4 parallel threads

## Performance Optimizations Already Active

### 1. Columnar Storage Benefits
```sql
-- Fast aggregations (only reads needed columns)
SELECT AVG(price), COUNT(*) FROM bronze.bond_data;  -- Milliseconds!

-- Efficient filtering (skips irrelevant data blocks)
SELECT * FROM bronze.bond_data WHERE date = '2024-01-15';
```

### 2. S3 Integration Optimizations
- **Streaming reads**: No need to download entire files
- **Predicate pushdown**: Filters applied during S3 read
- **Parallel downloads**: Multiple files read simultaneously

### 3. Query Optimization Examples
```sql
-- DuckDB automatically optimizes:
-- ❌ Slow: Read all data then filter
-- ✅ Fast: Filter during read using metadata

-- Partitioning by date (if implemented)
CREATE TABLE bronze.bond_data_partitioned AS
SELECT *, date_trunc('month', trade_date) as partition_key
FROM bronze.bond_data;
```

## Additional Performance Tuning

### 1. Increase Parallelism (if needed)
```python
# In config/settings.py - increase if you have more CPU cores
DUCKDB_THREADS = 8  # Match your CPU core count
```

### 2. Enable Query Profiling
```sql
-- Check query performance
EXPLAIN ANALYZE SELECT * FROM bronze.bond_data WHERE price > 100;
```

### 3. Create Indexes on Frequently Queried Columns
```sql
-- Not needed for most analytics, but helpful for point lookups
CREATE INDEX idx_bond_date ON bronze.bond_data(trade_date);
```

### 4. Partition Large Tables by Date
```sql
-- For very large datasets, consider partitioning
CREATE TABLE bronze.bond_data_partitioned (
    trade_date DATE,
    price DECIMAL,
    -- other columns
) PARTITION BY RANGE (trade_date);
```

## Performance Monitoring

### 1. Check Query Performance
```sql
-- Enable profiling
SET enable_profiling = 1;
SELECT COUNT(*) FROM bronze.bond_data;
```

### 2. Monitor Memory Usage
```sql
-- Check memory consumption
SELECT * FROM duckdb_memory();
```

### 3. Storage Efficiency
```sql
-- Check compression ratios
SELECT 
    table_name,
    estimated_size,
    column_count
FROM duckdb_tables();
```

## Why DuckDB is Fast for Your Use Case

1. **No Network Latency**: Local processing of cached data
2. **Vectorized Operations**: Processes thousands of rows per CPU instruction
3. **Columnar Format**: Only reads columns you actually use
4. **Advanced Compression**: 5-10x space savings with fast decompression
5. **Parallel Everything**: Uses all CPU cores automatically
6. **Smart Caching**: Frequently accessed data stays in memory

## Benchmarking Your Performance

```python
import time
from utils.database import DuckDBManager

db = DuckDBManager()

# Test aggregation performance
start = time.time()
result = db.execute_query("SELECT COUNT(*), AVG(price) FROM bronze.bond_data")
print(f"Aggregation took: {time.time() - start:.3f} seconds")

# Test filtering performance  
start = time.time()
result = db.execute_query("SELECT * FROM bronze.bond_data WHERE price > 100 LIMIT 1000")
print(f"Filtering took: {time.time() - start:.3f} seconds")
```

Your current setup is already well-optimized for analytical workloads! 