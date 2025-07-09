-- Fix DECIMAL precision for avg_records_per_file in DuckDB
-- This script can be run manually if needed

-- For DuckDB: Drop and recreate the table with correct precision
DROP TABLE IF EXISTS gold.daily_load_stats_backup;
CREATE TABLE gold.daily_load_stats_backup AS SELECT * FROM gold.daily_load_stats;

DROP TABLE IF EXISTS gold.daily_load_stats;
CREATE TABLE gold.daily_load_stats (
    id INTEGER PRIMARY KEY,
    stats_date DATE NOT NULL,
    exchange VARCHAR NOT NULL,
    total_files INTEGER DEFAULT 0,
    successful_files INTEGER DEFAULT 0,
    failed_files INTEGER DEFAULT 0,
    total_records BIGINT DEFAULT 0,
    avg_records_per_file DECIMAL(15,2),  -- Increased from DECIMAL(10,2)
    total_processing_time_seconds DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(stats_date, exchange)
);

-- Restore data from backup
INSERT INTO gold.daily_load_stats SELECT * FROM gold.daily_load_stats_backup;
DROP TABLE gold.daily_load_stats_backup;

-- For Supabase: Use ALTER TABLE (PostgreSQL supports this)
-- ALTER TABLE gold.daily_load_stats ALTER COLUMN avg_records_per_file TYPE DECIMAL(15,2); 