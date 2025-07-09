#!/usr/bin/env python3
"""
Test script to verify Supabase connection with new port 6543
"""

import os
import sys
from pathlib import Path
import logging

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

def test_new_port():
    """Test Supabase connection with new port 6543"""
    try:
        logger.info("Testing Supabase connection with port 6543...")
        
        # Create manager (should use port 6543 by default now)
        manager = SupabaseManager()
        logger.info(f"Using port: {manager.port}")
        logger.info(f"Using host: {manager.host}")
        
        # Test connection
        if manager.connection and not manager.connection.closed:
            logger.info("✅ Supabase connection successful with new port!")
            
            # Test a simple operation
            result = manager.execute_query("SELECT NOW() as current_time, version() as pg_version")
            if not result.empty:
                logger.info(f"✅ Database query successful:")
                logger.info(f"   Time: {result.iloc[0]['current_time']}")
                logger.info(f"   Version: {result.iloc[0]['pg_version']}")
            else:
                logger.error("❌ Database query failed")
            
            # Test progress record insert/update (the operation that was failing)
            logger.info("Testing progress record operations...")
            progress_id = manager.insert_progress_record("TEST", "2025-01-01", "test_path")
            if progress_id:
                logger.info(f"✅ Progress record inserted with ID: {progress_id}")
                
                # Test update (this was the failing operation)
                success = manager.update_progress_completed(progress_id, 1000)
                if success:
                    logger.info("✅ Progress record update successful!")
                else:
                    logger.error("❌ Progress record update failed")
                
                # Clean up test record
                manager.execute_sql("DELETE FROM bronze.load_progress WHERE id = %s", (progress_id,))
                logger.info("✅ Test record cleaned up")
            else:
                logger.error("❌ Progress record insert failed")
            
        else:
            logger.error("❌ Supabase connection failed")
        
        manager.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Port test failed: {e}")

if __name__ == "__main__":
    test_new_port() 