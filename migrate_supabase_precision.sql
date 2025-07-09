-- Supabase Migration Script: Update DECIMAL precision for trillion-scale records
-- Run this script directly in Supabase SQL Editor or via psql

-- Update daily_load_stats table to handle trillion-scale records
ALTER TABLE gold.daily_load_stats 
ALTER COLUMN avg_records_per_file TYPE DECIMAL(20,2);

-- Update weekly_load_stats table to handle trillion-scale records  
ALTER TABLE gold.weekly_load_stats 
ALTER COLUMN avg_daily_records TYPE DECIMAL(20,2);

-- Verify the changes
SELECT column_name, data_type, numeric_precision, numeric_scale 
FROM information_schema.columns 
WHERE table_name IN ('daily_load_stats', 'weekly_load_stats') 
AND column_name LIKE '%records%'
ORDER BY table_name, column_name; 