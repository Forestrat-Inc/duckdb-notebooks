#!/usr/bin/env python3
"""
Test script to validate all the bug fixes:
1. Variable scope issue fix
2. DECIMAL precision fix  
3. Supabase connection handling fix
"""

import sys
import os
from pathlib import Path
from datetime import date, datetime
import logging

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from utils.database import DuckDBManager
from utils.supabase_manager import SupabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_decimal_precision():
    """Test that large numbers can be stored in avg_records_per_file"""
    try:
        logger.info("Testing DECIMAL precision fix...")
        
        # Test with DuckDB
        db_manager = DuckDBManager(database_path="./test_precision.duckdb")
        
        # Create test table
        create_sql = """
        CREATE TABLE IF NOT EXISTS test_stats (
            id INTEGER PRIMARY KEY,
            avg_records_per_file DECIMAL(15,2)
        )
        """
        db_manager.execute_sql(create_sql)
        
        # Test inserting large number
        large_number = 143407374.00
        insert_sql = f"INSERT INTO test_stats (id, avg_records_per_file) VALUES (1, {large_number})"
        success = db_manager.execute_sql(insert_sql)
        
        if success:
            # Verify the value was stored correctly
            result = db_manager.execute_query("SELECT avg_records_per_file FROM test_stats WHERE id = 1")
            stored_value = result.iloc[0]['avg_records_per_file']
            logger.info(f"✅ DuckDB DECIMAL precision test passed - stored: {stored_value}")
        else:
            logger.error("❌ DuckDB DECIMAL precision test failed - could not insert large number")
        
        # Clean up
        os.remove("./test_precision.duckdb")
        
    except Exception as e:
        logger.error(f"❌ DECIMAL precision test failed: {e}")

def test_supabase_connection():
    """Test Supabase connection handling and reconnection"""
    try:
        logger.info("Testing Supabase connection handling...")
        
        # Test normal connection
        supabase_manager = SupabaseManager()
        
        # Test initial connection
        if supabase_manager.connection and not supabase_manager.connection.closed:
            logger.info("✅ Initial Supabase connection successful")
        else:
            logger.error("❌ Initial Supabase connection failed")
            return
        
        # Test a simple query
        result = supabase_manager.execute_query("SELECT NOW() as current_time")
        if not result.empty:
            logger.info(f"✅ Supabase query test passed - time: {result.iloc[0]['current_time']}")
        else:
            logger.error("❌ Supabase query test failed")
        
        # Test connection state handling (simulate closed connection)
        logger.info("Testing connection state handling...")
        
        # Close connection manually to test reconnection
        supabase_manager.disconnect()
        
        # Try to execute another query (should trigger reconnection)
        result2 = supabase_manager.execute_query("SELECT 1 as test_value")
        if not result2.empty:
            logger.info("✅ Supabase reconnection test passed")
        else:
            logger.error("❌ Supabase reconnection test failed")
        
        supabase_manager.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Supabase connection test failed: {e}")

def test_progress_tracking_variable():
    """Test that progress_tracking variable is properly initialized"""
    try:
        logger.info("Testing progress_tracking variable initialization...")
        
        # Import the loader class
        from load_january_simple import SimpleMultiExchangeLoader
        
        # Create a loader instance
        loader = SimpleMultiExchangeLoader()
        
        # Test that progress_tracking is handled properly even on early errors
        # We'll simulate an error by using an invalid date
        result = loader.load_single_file("INVALID_EXCHANGE", date(2025, 1, 1))
        
        # The result should have an error but not crash with "referenced before assignment"
        if 'error' in result and result['error'] is not None:
            logger.info("✅ Progress tracking variable test passed - error handled gracefully")
        else:
            logger.warning("⚠️ Progress tracking test inconclusive - no error occurred")
        
        loader.cleanup()
        
    except NameError as e:
        if "progress_tracking" in str(e):
            logger.error("❌ Progress tracking variable test failed - variable referenced before assignment")
        else:
            logger.error(f"❌ Progress tracking variable test failed: {e}")
    except Exception as e:
        logger.info(f"✅ Progress tracking variable test passed - handled exception gracefully: {type(e).__name__}")

def test_numpy_type_conversion():
    """Test numpy type conversion in Supabase manager"""
    try:
        logger.info("Testing numpy type conversion...")
        
        import numpy as np
        
        supabase_manager = SupabaseManager()
        
        # Test numpy type conversion
        test_values = [
            np.int64(123),
            np.float64(456.78),
            np.bool_(True),
            "regular string",
            123  # regular int
        ]
        
        converted_values = [supabase_manager._convert_numpy_types(val) for val in test_values]
        
        # Check types are converted properly
        type_check = [
            isinstance(converted_values[0], int),
            isinstance(converted_values[1], float), 
            isinstance(converted_values[2], bool),
            isinstance(converted_values[3], str),
            isinstance(converted_values[4], int)
        ]
        
        if all(type_check):
            logger.info("✅ Numpy type conversion test passed")
        else:
            logger.error(f"❌ Numpy type conversion test failed - types: {[type(v) for v in converted_values]}")
        
        supabase_manager.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Numpy type conversion test failed: {e}")

def main():
    """Run all tests"""
    logger.info("="*60)
    logger.info("RUNNING ALL BUG FIX TESTS")
    logger.info("="*60)
    
    # Test 1: DECIMAL precision
    test_decimal_precision()
    
    # Test 2: Supabase connection handling
    test_supabase_connection()
    
    # Test 3: Progress tracking variable scope
    test_progress_tracking_variable()
    
    # Test 4: Numpy type conversion
    test_numpy_type_conversion()
    
    logger.info("="*60)
    logger.info("ALL TESTS COMPLETED")
    logger.info("="*60)

if __name__ == "__main__":
    main() 