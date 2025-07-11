-- Migration script to add file_size_bytes column to bronze.load_progress table
-- and update stats tables to include file size aggregations
-- Run this script against both DuckDB and Supabase databases

-- =====================================
-- 1. Update bronze.load_progress table
-- =====================================

-- Add file_size_bytes column to bronze.load_progress table
-- This is compatible with both DuckDB and PostgreSQL/Supabase
ALTER TABLE bronze.load_progress ADD COLUMN file_size_bytes BIGINT;

-- Optional: Add a comment to document the column (PostgreSQL/Supabase only)
-- COMMENT ON COLUMN bronze.load_progress.file_size_bytes IS 'Size of the source file in bytes';

-- Create an index for better performance when filtering by file size
CREATE INDEX IF NOT EXISTS idx_load_progress_file_size ON bronze.load_progress(file_size_bytes);

-- =====================================
-- 2. Update gold.daily_load_stats table
-- =====================================

-- Add file size columns to daily stats table
ALTER TABLE gold.daily_load_stats ADD COLUMN total_file_size_bytes BIGINT DEFAULT 0;
ALTER TABLE gold.daily_load_stats ADD COLUMN avg_file_size_bytes DECIMAL(20,2);

-- =====================================
-- 3. Update gold.weekly_load_stats table
-- =====================================

-- Add file size columns to weekly stats table
ALTER TABLE gold.weekly_load_stats ADD COLUMN avg_daily_file_size_bytes DECIMAL(20,2);
ALTER TABLE gold.weekly_load_stats ADD COLUMN total_file_size_bytes BIGINT DEFAULT 0;

-- =====================================
-- 4. Verification Queries
-- =====================================

-- Verify the columns were added successfully to bronze.load_progress
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'bronze' 
AND table_name = 'load_progress' 
AND column_name = 'file_size_bytes';

-- Verify the columns were added successfully to gold.daily_load_stats
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'gold' 
AND table_name = 'daily_load_stats' 
AND column_name IN ('total_file_size_bytes', 'avg_file_size_bytes');

-- Verify the columns were added successfully to gold.weekly_load_stats
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'gold' 
AND table_name = 'weekly_load_stats' 
AND column_name IN ('avg_daily_file_size_bytes', 'total_file_size_bytes');

-- =====================================
-- 5. Sample Queries for File Size Data
-- =====================================

-- Example: Get progress records with file sizes
-- SELECT exchange, data_date, 
--        file_size_bytes, 
--        ROUND(file_size_bytes / 1024.0 / 1024.0, 2) as file_size_mb,
--        records_loaded,
--        status
-- FROM bronze.load_progress 
-- WHERE file_size_bytes IS NOT NULL
-- ORDER BY file_size_bytes DESC
-- LIMIT 10;

-- Example: Get daily stats with file size aggregations
-- SELECT stats_date, exchange,
--        total_files,
--        total_records,
--        total_file_size_bytes,
--        ROUND(total_file_size_bytes / 1024.0 / 1024.0, 2) as total_size_mb,
--        ROUND(avg_file_size_bytes / 1024.0 / 1024.0, 2) as avg_file_size_mb
-- FROM gold.daily_load_stats
-- WHERE total_file_size_bytes > 0
-- ORDER BY stats_date DESC; 