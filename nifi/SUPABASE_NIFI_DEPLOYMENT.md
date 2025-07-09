# Supabase Integration - NiFi Deployment Guide

This guide explains how to deploy the Supabase-integrated `load_january_simple.py` script in NiFi.

## Overview

The `load_january_simple.py` script now automatically writes statistics to both:
- **DuckDB** (local file)
- **Supabase** (cloud PostgreSQL)

When you run this script from NiFi, **no additional configuration is needed** - the Supabase integration is built-in.

## Prerequisites

### 1. Python Dependencies
Install these packages in your NiFi Python environment:

```bash
# In your NiFi Python environment
pip install psycopg2-binary>=2.9.0
pip install pandas>=1.5.0
pip install duckdb>=0.8.0
pip install boto3>=1.26.0
pip install s3fs>=2023.1.0
```

### 2. Network Access
Ensure NiFi can reach Supabase:
- **Host**: `your-project.supabase.co` (example)
- **Port**: `6543`
- **Protocol**: PostgreSQL over SSL

Test connectivity from your NiFi server:
```bash
telnet your-project.supabase.co 5432
```

## NiFi Processor Configuration

### ExecuteScript Processor Setup

1. **Processor Type**: `ExecuteScript`
2. **Script Engine**: `python`
3. **Script File**: `/path/to/load_january_simple.py`

#### Properties:
```
Script Engine: python
Script File: /opt/nifi/scripts/load_january_simple.py
Script Arguments: --date ${date} --idempotent
Module Directory: /opt/nifi/scripts
```

#### Environment Variables (Optional - for security):
```
SUPABASE_HOST=your-project.supabase.co
SUPABASE_PORT=6543
SUPABASE_DATABASE=postgres
SUPABASE_USER=postgres.your-user-id
SUPABASE_PASSWORD=your-database-password
```

### Sample NiFi Flow

```
GenerateFlowFile → UpdateAttribute → ExecuteScript → LogAttribute
                      (set date)      (run script)    (log results)
```

#### UpdateAttribute Processor:
- **date**: `${now():format('yyyy-MM-dd')}`
- **exchange**: `LSE` (or dynamic based on your needs)

#### ExecuteScript Processor:
- **Script**: `load_january_simple.py`
- **Arguments**: `--date ${date} --exchanges ${exchange} --idempotent`

## Monitoring & Logging

### 1. Script Logs
The script creates detailed logs at:
```
/opt/nifi/scripts/logs/january_load_simple_YYYYMMDD_HHMMSS.log
```

### 2. Supabase Connection Status
The script logs Supabase connectivity:
- ✅ `Supabase connection established for statistics tracking`
- ⚠️ `Failed to connect to Supabase - statistics will only be tracked locally`

### 3. Statistics Verification
Check if data is being written to Supabase:

**In Supabase SQL Editor:**
```sql
-- Check recent progress
SELECT * FROM bronze.load_progress 
ORDER BY created_at DESC 
LIMIT 10;

-- Check daily stats
SELECT * FROM gold.daily_load_stats 
ORDER BY stats_date DESC, exchange;

-- Check weekly stats  
SELECT * FROM gold.weekly_load_stats 
ORDER BY week_ending DESC, exchange;
```

## Error Handling

### Graceful Degradation
If Supabase is unavailable:
- ✅ Script continues running
- ✅ Data still written to local DuckDB
- ⚠️ Warning logged about Supabase unavailability
- 📊 Statistics available locally for later sync

### Common Issues

1. **`ModuleNotFoundError: No module named 'psycopg2'`**
   - Solution: Install `psycopg2-binary` in NiFi Python environment

2. **Connection timeout to Supabase**
   - Check network connectivity
   - Verify firewall rules allow outbound port 6543

3. **Authentication failed**
   - Verify Supabase credentials
   - Check if user has necessary permissions

## Performance Considerations

### 1. Connection Pooling
The script creates one connection per run. For high-frequency NiFi flows:
- Consider connection pooling
- Monitor Supabase connection limits

### 2. Batch Processing
For multiple dates/exchanges:
```bash
# Single date, all exchanges
python load_january_simple.py --date 2025-01-15 --idempotent

# Specific exchanges only
python load_january_simple.py --date 2025-01-15 --exchanges LSE CME --idempotent
```

## Testing in NiFi

### 1. Test Connectivity
Create a simple test processor:
```python
from utils.supabase_manager import SupabaseManager

# Test connection
try:
    manager = SupabaseManager()
    result = manager.execute_query("SELECT NOW() as current_time")
    print(f"✅ Supabase connected: {result.iloc[0]['current_time']}")
    manager.disconnect()
except Exception as e:
    print(f"❌ Supabase connection failed: {e}")
```

### 2. Verify Statistics
After running the script, check both databases:

**DuckDB (local):**
```sql
SELECT COUNT(*) FROM bronze.load_progress;
SELECT COUNT(*) FROM gold.daily_load_stats;
```

**Supabase (cloud):**
```sql
SELECT COUNT(*) FROM bronze.load_progress;
SELECT COUNT(*) FROM gold.daily_load_stats;
```

## Security Best Practices

### 1. Environment Variables
Instead of hardcoded credentials, use environment variables:
```bash
export SUPABASE_PASSWORD="your_password"
```

### 2. NiFi Parameter Contexts
Store sensitive values in NiFi Parameter Contexts:
- `supabase.host`
- `supabase.user`
- `supabase.password`

### 3. Network Security
- Use VPN/private networks when possible
- Monitor connection logs
- Rotate credentials regularly

## Troubleshooting Commands

```bash
# Test Supabase connection
python nifi/supabase_config.py

# Test full script functionality
python test_supabase_connection.py

# Check NiFi Python environment
python -c "import psycopg2; print('✅ psycopg2 available')"

# Test network connectivity
telnet your-project.supabase.co 5432
```

## Summary

✅ **No changes needed** - `load_january_simple.py` already includes Supabase integration  
✅ **Automatic dual-write** - Statistics go to both DuckDB and Supabase  
✅ **Fault tolerant** - Continues working if Supabase is down  
✅ **NiFi ready** - Just ensure dependencies and network access  

The integration is seamless - your existing NiFi flows will automatically start writing to Supabase! 🚀 