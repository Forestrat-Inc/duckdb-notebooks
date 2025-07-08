"""
Test Supabase Connection and Statistics Tables

This script tests the connection to Supabase and verifies that all
statistics tables are created properly.
"""

import logging
import sys
from pathlib import Path
from datetime import date, datetime

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

def test_supabase_connection():
    """Test basic Supabase connectivity"""
    logger.info("🧪 Testing Supabase connection...")
    
    try:
        # Initialize Supabase manager
        supabase_manager = SupabaseManager()
        
        # Test basic connectivity
        current_time = supabase_manager.execute_query("SELECT NOW() as current_time")
        logger.info(f"✅ Connection successful! Current time: {current_time.iloc[0]['current_time']}")
        
        # Test table creation
        logger.info("🏗️ Testing table creation...")
        
        # Check if schemas exist
        schemas_query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name IN ('bronze', 'silver', 'gold')
        ORDER BY schema_name
        """
        schemas = supabase_manager.execute_query(schemas_query)
        logger.info(f"📁 Schemas found: {list(schemas['schema_name'])}")
        
        # Check if tables exist
        tables_query = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('bronze', 'silver', 'gold')
        ORDER BY table_schema, table_name
        """
        tables = supabase_manager.execute_query(tables_query)
        
        if not tables.empty:
            logger.info("📊 Tables found:")
            for _, row in tables.iterrows():
                logger.info(f"   {row['table_schema']}.{row['table_name']}")
        else:
            logger.warning("⚠️ No tables found in bronze/silver/gold schemas")
        
        # Test insert and update operations
        logger.info("🧪 Testing CRUD operations...")
        
        # Test progress record
        progress_id = supabase_manager.insert_progress_record(
            exchange='TEST',
            data_date=date.today(),
            file_path='s3://test-bucket/test-file.csv'
        )
        
        if progress_id:
            logger.info(f"✅ Progress record created with ID: {progress_id}")
            
            # Test update completion
            supabase_manager.update_progress_completed(progress_id, 1000)
            logger.info("✅ Progress record updated as completed")
            
            # Test daily stats upsert
            supabase_manager.upsert_daily_stats('TEST', date.today())
            logger.info("✅ Daily statistics upserted")
            
            # Test weekly stats upsert
            supabase_manager.upsert_weekly_stats('TEST', date.today())
            logger.info("✅ Weekly statistics upserted")
            
        # Test data retrieval
        logger.info("📊 Testing data retrieval...")
        
        progress_summary = supabase_manager.get_progress_summary()
        logger.info(f"📈 Progress records found: {len(progress_summary)}")
        
        daily_stats = supabase_manager.get_daily_stats()
        logger.info(f"📈 Daily stats records found: {len(daily_stats)}")
        
        weekly_stats = supabase_manager.get_weekly_stats()
        logger.info(f"📈 Weekly stats records found: {len(weekly_stats)}")
        
        # Cleanup test data
        if progress_id:
            cleanup_sql = "DELETE FROM bronze.load_progress WHERE exchange = 'TEST'"
            supabase_manager.execute_sql(cleanup_sql)
            
            cleanup_daily = "DELETE FROM gold.daily_load_stats WHERE exchange = 'TEST'"
            supabase_manager.execute_sql(cleanup_daily)
            
            cleanup_weekly = "DELETE FROM gold.weekly_load_stats WHERE exchange = 'TEST'"
            supabase_manager.execute_sql(cleanup_weekly)
            
            logger.info("🧹 Test data cleaned up")
        
        # Disconnect
        supabase_manager.disconnect()
        logger.info("✅ All tests passed! Supabase is ready for statistics tracking")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False

def test_existing_data():
    """Check if there's any existing data in Supabase"""
    logger.info("🔍 Checking for existing data...")
    
    try:
        supabase_manager = SupabaseManager()
        
        # Check progress data
        progress_count = supabase_manager.execute_query(
            "SELECT COUNT(*) as count FROM bronze.load_progress"
        )
        logger.info(f"📊 Progress records: {progress_count.iloc[0]['count']}")
        
        # Check daily stats
        daily_count = supabase_manager.execute_query(
            "SELECT COUNT(*) as count FROM gold.daily_load_stats"
        )
        logger.info(f"📊 Daily stats records: {daily_count.iloc[0]['count']}")
        
        # Check weekly stats
        weekly_count = supabase_manager.execute_query(
            "SELECT COUNT(*) as count FROM gold.weekly_load_stats"
        )
        logger.info(f"📊 Weekly stats records: {weekly_count.iloc[0]['count']}")
        
        # Show recent records if any exist
        if progress_count.iloc[0]['count'] > 0:
            recent_progress = supabase_manager.execute_query(
                "SELECT exchange, data_date, status, records_loaded FROM bronze.load_progress ORDER BY created_at DESC LIMIT 5"
            )
            logger.info("📈 Recent progress records:")
            for _, row in recent_progress.iterrows():
                logger.info(f"   {row['exchange']} {row['data_date']} - {row['status']} ({row['records_loaded']} records)")
        
        supabase_manager.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Error checking existing data: {e}")

if __name__ == "__main__":
    logger.info("🚀 Starting Supabase test suite...")
    
    success = test_supabase_connection()
    
    if success:
        test_existing_data()
        logger.info("🎉 Supabase testing completed successfully!")
    else:
        logger.error("❌ Supabase testing failed!")
        sys.exit(1) 