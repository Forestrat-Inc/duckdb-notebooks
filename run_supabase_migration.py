#!/usr/bin/env python3
"""
Supabase Migration Script: Update DECIMAL precision for trillion-scale records

This script automatically migrates the Supabase database schema to handle
CME's trillion-scale record counts by updating DECIMAL precision from (15,2) to (20,2).
"""

import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from utils.supabase_manager import SupabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_migration():
    """Run the Supabase schema migration"""
    try:
        logger.info("🔄 Starting Supabase schema migration for trillion-scale record support...")
        
        # Connect to Supabase
        manager = SupabaseManager()
        logger.info("✅ Connected to Supabase")
        
        # Check current schema
        logger.info("🔍 Checking current schema...")
        current_schema = manager.execute_query("""
            SELECT table_name, column_name, data_type, numeric_precision, numeric_scale 
            FROM information_schema.columns 
            WHERE table_name IN ('daily_load_stats', 'weekly_load_stats') 
            AND column_name LIKE '%records%'
            ORDER BY table_name, column_name
        """)
        
        if not current_schema.empty:
            logger.info("📋 Current schema:")
            for _, row in current_schema.iterrows():
                logger.info(f"  {row['table_name']}.{row['column_name']}: {row['data_type']}({row['numeric_precision']},{row['numeric_scale']})")
        
        # Run migration
        logger.info("🔧 Updating daily_load_stats.avg_records_per_file to DECIMAL(20,2)...")
        success1 = manager.execute_sql("""
            ALTER TABLE gold.daily_load_stats 
            ALTER COLUMN avg_records_per_file TYPE DECIMAL(20,2)
        """)
        
        if success1:
            logger.info("✅ daily_load_stats migration successful")
        else:
            logger.error("❌ daily_load_stats migration failed")
        
        logger.info("🔧 Updating weekly_load_stats.avg_daily_records to DECIMAL(20,2)...")
        success2 = manager.execute_sql("""
            ALTER TABLE gold.weekly_load_stats 
            ALTER COLUMN avg_daily_records TYPE DECIMAL(20,2)
        """)
        
        if success2:
            logger.info("✅ weekly_load_stats migration successful")
        else:
            logger.error("❌ weekly_load_stats migration failed")
        
        # Verify changes
        logger.info("🔍 Verifying migration...")
        updated_schema = manager.execute_query("""
            SELECT table_name, column_name, data_type, numeric_precision, numeric_scale 
            FROM information_schema.columns 
            WHERE table_name IN ('daily_load_stats', 'weekly_load_stats') 
            AND column_name LIKE '%records%'
            ORDER BY table_name, column_name
        """)
        
        if not updated_schema.empty:
            logger.info("📋 Updated schema:")
            for _, row in updated_schema.iterrows():
                logger.info(f"  {row['table_name']}.{row['column_name']}: {row['data_type']}({row['numeric_precision']},{row['numeric_scale']})")
        
        # Test with large number
        logger.info("🧪 Testing with CME-scale number (2+ billion)...")
        test_success = manager.execute_sql("""
            INSERT INTO gold.daily_load_stats 
            (stats_date, exchange, total_files, successful_files, failed_files, total_records, avg_records_per_file)
            VALUES 
            ('2025-01-01', 'TEST_MIGRATION', 1, 1, 0, 2086203701, 2086203701.00)
            ON CONFLICT (stats_date, exchange) DO UPDATE SET
            avg_records_per_file = EXCLUDED.avg_records_per_file
        """)
        
        if test_success:
            logger.info("✅ Test insert with 2+ billion records successful!")
            
            # Clean up test record
            manager.execute_sql("DELETE FROM gold.daily_load_stats WHERE exchange = 'TEST_MIGRATION'")
            logger.info("🧹 Test record cleaned up")
        else:
            logger.error("❌ Test insert failed")
        
        manager.disconnect()
        
        if success1 and success2:
            logger.info("🎉 Migration completed successfully! Database now supports trillion-scale records.")
            return True
        else:
            logger.error("❌ Migration failed - some operations were unsuccessful")
            return False
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        return False

def main():
    """Main function"""
    logger.info("="*70)
    logger.info("SUPABASE SCHEMA MIGRATION - TRILLION-SCALE RECORD SUPPORT")
    logger.info("="*70)
    
    success = run_migration()
    
    if success:
        logger.info("🚀 Your database is now ready to handle CME's trillion-scale records!")
        sys.exit(0)
    else:
        logger.error("💥 Migration failed - please check logs and try again")
        sys.exit(1)

if __name__ == "__main__":
    main() 