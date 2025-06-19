#!/usr/bin/env python3
"""
Test script to verify S3 connectivity and provide widget-free visualization
"""

import sys
import os
import logging
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils.database import DuckDBManager
from config.settings import config
import matplotlib.pyplot as plt
import pandas as pd

def test_s3_and_visualize():
    """Test S3 connection and create simple visualizations"""
    
    # Setup logging
    config.setup_logging()
    logger = logging.getLogger(__name__)
    
    print("🔗 Testing S3 connectivity...")
    
    # Initialize database manager
    db_manager = DuckDBManager()
    
    # Test S3 connection
    try:
        if db_manager.test_s3_connection():
            print("✅ S3 connection successful!")
            
            # Try to get some basic data statistics
            print("\n📊 Getting basic data statistics...")
            
            basic_stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT filename) as unique_files
            FROM read_csv('{config.INGESTION_PATH}/*/*.csv.gz', 
                         AUTO_DETECT=true, 
                         FILENAME=true,
                         SAMPLE_SIZE=1000)
            """
            
            stats = db_manager.execute_query(basic_stats_query)
            print(f"📈 Found {stats['total_records'].iloc[0]:,} records across {stats['unique_files'].iloc[0]} files")
            
            # Get file counts
            file_counts_query = f"""
            SELECT 
                filename,
                COUNT(*) as record_count
            FROM read_csv('{config.INGESTION_PATH}/*/*.csv.gz', 
                         AUTO_DETECT=true, 
                         FILENAME=true,
                         SAMPLE_SIZE=1000)
            GROUP BY filename
            ORDER BY record_count DESC
            LIMIT 10
            """
            
            file_counts = db_manager.execute_query(file_counts_query)
            
            # Create a simple matplotlib plot (no widgets needed)
            if not file_counts.empty:
                plt.figure(figsize=(12, 6))
                plt.bar(range(len(file_counts)), file_counts['record_count'])
                plt.xticks(range(len(file_counts)), 
                          [f.split('/')[-1][:20] + '...' for f in file_counts['filename']], 
                          rotation=45, ha='right')
                plt.ylabel('Record Count')
                plt.title('Top 10 Files by Record Count')
                plt.tight_layout()
                plt.savefig('s3_test_results.png', dpi=150, bbox_inches='tight')
                plt.show()
                print("📊 Visualization saved as 's3_test_results.png'")
            
            return True
            
        else:
            print("❌ S3 connection failed")
            return False
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
        print(f"❌ Test failed: {e}")
        return False
    
    finally:
        db_manager.close()

if __name__ == "__main__":
    print("🚀 Starting S3 connectivity test...")
    success = test_s3_and_visualize()
    
    if success:
        print("\n✅ All tests passed!")
        print("\n💡 To fix notebook widget issues:")
        print("1. Restart your Jupyter kernel")
        print("2. Add this to the top of your notebook cells:")
        print("   import matplotlib.pyplot as plt")
        print("   plt.ioff()  # Turn off interactive mode")
        print("3. Use plt.show() instead of fig.show() for static plots")
    else:
        print("\n❌ Tests failed - check your AWS configuration") 