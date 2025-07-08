#!/usr/bin/env python3
"""
Setup script for NiFi Progress Tracking Tables in DuckDB
This script creates all necessary tables and views for NiFi progress tracking and statistics.
"""

import os
import sys
import time
from pathlib import Path
import duckdb

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

def setup_nifi_tables():
    """Setup all NiFi tracking tables in DuckDB"""
    database_path = "./multi_exchange_data_lake.duckdb"
    
    print("🔧 Setting up NiFi Progress Tracking Tables...")
    print(f"📍 Database path: {database_path}")
    
    # Check if database file exists
    if not os.path.exists(database_path):
        print(f"❌ Database file not found: {database_path}")
        print("Please ensure the database file exists or create it first.")
        return False
    
    try:
        # Try to connect to the database
        print("🔌 Connecting to DuckDB...")
        conn = duckdb.connect(database_path)
        
        # Read the SQL files
        print("📖 Reading SQL files...")
        
        # Progress tracking tables
        progress_sql_path = "sql/nifi_progress_tracking_tables.sql"
        if os.path.exists(progress_sql_path):
            with open(progress_sql_path, 'r') as f:
                progress_sql = f.read()
        else:
            print(f"❌ SQL file not found: {progress_sql_path}")
            return False
        
        # Statistics tables
        stats_sql_path = "sql/nifi_statistics_tables.sql"
        if os.path.exists(stats_sql_path):
            with open(stats_sql_path, 'r') as f:
                stats_sql = f.read()
        else:
            print(f"❌ SQL file not found: {stats_sql_path}")
            return False
        
        # Execute the SQL commands
        print("🏗️  Creating progress tracking tables...")
        conn.execute(progress_sql)
        print("✅ Progress tracking tables created successfully!")
        
        print("🏗️  Creating statistics tables...")
        conn.execute(stats_sql)
        print("✅ Statistics tables created successfully!")
        
        # Verify tables were created
        print("\n🔍 Verifying table creation...")
        
        # Check schemas
        schemas_result = conn.execute("SHOW SCHEMAS").fetchall()
        print("Available schemas:")
        for schema in schemas_result:
            print(f"  - {schema[0]}")
        
        # Check bronze tables
        bronze_tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'bronze' 
            ORDER BY table_name
        """).fetchall()
        
        print("\n📊 Tables in bronze schema:")
        for table in bronze_tables:
            print(f"  - bronze.{table[0]}")
        
        # Check silver tables
        silver_tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'silver' 
            ORDER BY table_name
        """).fetchall()
        
        print("\n📊 Tables in silver schema:")
        for table in silver_tables:
            print(f"  - silver.{table[0]}")
        
        # Check gold tables
        gold_tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'gold' 
            ORDER BY table_name
        """).fetchall()
        
        print("\n📊 Tables in gold schema:")
        for table in gold_tables:
            print(f"  - gold.{table[0]}")
        
        # Test one of the tables
        print("\n🧪 Testing table functionality...")
        test_result = conn.execute("SELECT COUNT(*) FROM bronze.nifi_load_progress").fetchone()
        print(f"✅ bronze.nifi_load_progress table working - current record count: {test_result[0]}")
        
        conn.close()
        
        print("\n🎉 All NiFi tracking tables created successfully!")
        print("\n📋 Summary of created tables:")
        print("   Bronze Layer (Raw tracking data):")
        print("     - nifi_load_progress: Individual file processing status")
        print("     - nifi_load_stats: Real-time statistics")
        print("     - nifi_load_errors: Error tracking and resolution")
        print("   Silver Layer (Processed data):")
        print("     - hourly_stats: Hourly aggregations")
        print("   Gold Layer (Analytics-ready data):")
        print("     - daily_stats: Daily aggregated statistics")
        print("     - weekly_rolling_stats: 7-day rolling window analytics")
        print("\n🔍 Available views for dashboards:")
        print("   - bronze.v_nifi_progress_summary: Real-time progress overview")
        print("   - bronze.v_nifi_error_analysis: Error analysis and trends")
        print("   - bronze.v_nifi_performance_metrics: Performance metrics")
        print("   - gold.v_current_progress_dashboard: Dashboard data")
        print("   - gold.v_weekly_trends_dashboard: Weekly trends")
        print("   - gold.v_error_analysis_dashboard: Error analysis dashboard")
        
        return True
        
    except duckdb.IOException as e:
        if "Conflicting lock" in str(e):
            print("\n❌ Database Lock Error!")
            print("The DuckDB database is currently locked by another process.")
            print("\n🔧 To resolve this issue:")
            print("1. Close any DuckDB CLI sessions (check terminal windows)")
            print("2. Stop any running Python scripts that use this database")
            print("3. Check for any running processes:")
            print("   ps aux | grep duckdb")
            print("4. Kill any conflicting processes if needed:")
            print("   kill -9 <PID>")
            print("\n🔄 Then run this script again.")
            
            return False
        else:
            print(f"❌ Database connection error: {e}")
            return False
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def main():
    """Main function"""
    print("=" * 60)
    print("🚀 NiFi Progress Tracking Tables Setup")
    print("=" * 60)
    
    success = setup_nifi_tables()
    
    if success:
        print("\n✅ Setup completed successfully!")
        print("\n🎯 Next steps:")
        print("1. Configure NiFi controller services")
        print("2. Create NiFi process group template")
        print("3. Set up progress monitoring dashboard")
        
        # Update the TODO list
        print("\n📋 TODO Status Update:")
        print("✅ Step 1.1: Create DuckDB progress tracking tables - COMPLETED")
        print("⏳ Step 1.2: Create NiFi process group structure - PENDING")
        print("⏳ Step 1.3: Configure NiFi controller services - PENDING")
        
    else:
        print("\n❌ Setup failed. Please resolve the issues above and try again.")
        
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 