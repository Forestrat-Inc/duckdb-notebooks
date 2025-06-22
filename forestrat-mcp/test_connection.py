#!/usr/bin/env python3
"""
Test script for Forestrat MCP Server database connection
"""

import sys
import json
from pathlib import Path
from database import DuckDBConnection

def test_database_connection():
    """Test database connection and basic queries"""
    print("Testing Forestrat MCP Server Database Connection")
    print("="*50)
    
    try:
        # Initialize database connection
        db_path = "../multi_exchange_data_lake.duckdb"
        print(f"Connecting to database: {db_path}")
        
        db = DuckDBConnection(db_path)
        
        # Test basic connection
        print("✓ Database connection established")
        
        # Test connection
        if db.test_connection():
            print("✓ Connection test passed")
        else:
            print("✗ Connection test failed")
            return False
        
        # Get database stats
        print("\nDatabase Statistics:")
        stats = db.get_database_stats()
        print(f"Total tables: {stats.get('total_tables', 'N/A')}")
        print(f"Schemas: {', '.join(stats.get('schemas', []))}")
        
        # List all tables
        print("\nAvailable Tables:")
        tables_query = """
        SELECT 
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'main')
        ORDER BY table_schema, table_name
        """
        
        tables = db.execute_query(tables_query)
        for _, row in tables.iterrows():
            print(f"  {row['table_schema']}.{row['table_name']} ({row['table_type']})")
        
        # Test a few specific tables if they exist
        test_tables = [
            'bronze.lse_market_data',
            'bronze.cme_market_data', 
            'bronze.nyq_market_data',
            'silver.market_data_unified'
        ]
        
        print("\nTable Tests:")
        for table in test_tables:
            if db.table_exists(table):
                try:
                    count_query = f"SELECT COUNT(*) as count FROM {table}"
                    result = db.execute_query(count_query)
                    count = result.iloc[0]['count']
                    print(f"  ✓ {table}: {count:,} records")
                except Exception as e:
                    print(f"  ✗ {table}: Error - {e}")
            else:
                print(f"  - {table}: Table does not exist")
        
        print("\n✓ All tests completed successfully!")
        return True
        
    except FileNotFoundError as e:
        print(f"✗ Database file not found: {e}")
        return False
    except Exception as e:
        print(f"✗ Error during testing: {e}")
        return False
    finally:
        try:
            db.close()
            print("Database connection closed")
        except:
            pass

def test_mcp_tools():
    """Test MCP tool functionality (without actual MCP server)"""
    print("\nTesting MCP Tool Functions")
    print("="*30)
    
    try:
        from main import ForestratMCPServer
        
        # Initialize server (but don't run it)
        server = ForestratMCPServer()
        
        print("✓ MCP Server initialized")
        print("✓ Database connection in MCP server established")
        
        # Test some async functions (basic validation)
        import asyncio
        
        async def test_async_functions():
            try:
                # Test list datasets
                datasets = await server._list_datasets(include_stats=False)
                print(f"✓ list_datasets: Found {len(datasets.get('schemas', {}))} schemas")
                
                return True
            except Exception as e:
                print(f"✗ Async function test failed: {e}")
                return False
        
        # Run async test
        result = asyncio.run(test_async_functions())
        
        if result:
            print("✓ MCP tool functions working correctly")
        
        return result
        
    except Exception as e:
        print(f"✗ MCP tool test failed: {e}")
        return False

if __name__ == "__main__":
    print("Forestrat MCP Server Test Suite")
    print("="*40)
    
    # Test database connection
    db_test = test_database_connection()
    
    # Test MCP functionality if database works
    if db_test:
        tool_test = test_mcp_tools()
        
        if db_test and tool_test:
            print("\n🎉 All tests passed! The MCP server is ready to use.")
            sys.exit(0)
        else:
            print("\n❌ Some tests failed. Please check the errors above.")
            sys.exit(1)
    else:
        print("\n❌ Database connection failed. Please check your database setup.")
        sys.exit(1) 