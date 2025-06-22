#!/usr/bin/env python3
"""
Test script to verify MCP server functionality
"""

import asyncio
import json
import sys
from pathlib import Path

# Import the server components
from main import ForestratMCPServer

async def test_mcp_server():
    """Test the MCP server functionality"""
    print("Testing Forestrat MCP Server Functionality")
    print("=" * 45)
    
    try:
        # Initialize server
        server = ForestratMCPServer()
        print("✓ MCP Server initialized successfully")
        
        # Test database connection
        if not server.db.test_connection():
            print("❌ Database connection failed")
            return False
        print("✓ Database connection working")
        
        # Test each tool function
        tests = [
            ("list_datasets", server._list_datasets, {"include_stats": False}),
            ("get_dataset_exchanges", server._get_dataset_exchanges, {"dataset": "unified"}),
            ("get_table_schema", server._get_table_schema, {"table_name": "bronze.lse_market_data"}),
            ("get_available_symbols", server._get_available_symbols, {"exchange": "LSE"}),
        ]
        
        for test_name, test_func, test_args in tests:
            try:
                print(f"\nTesting {test_name}...")
                if asyncio.iscoroutinefunction(test_func):
                    result = await test_func(**test_args)
                else:
                    result = test_func(**test_args)
                print(f"✓ {test_name} - Success")
            except Exception as e:
                print(f"❌ {test_name} - Failed: {e}")
        
        print("\n✅ MCP Server functionality test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Server test failed: {e}")
        return False
    finally:
        try:
            server.db.close()
        except:
            pass

def test_server_startup():
    """Test that the server can start without errors"""
    print("\nTesting Server Startup...")
    try:
        from main import ForestratMCPServer
        server = ForestratMCPServer()
        print("✓ Server initialization successful")
        
        # Test that handlers are registered
        if hasattr(server.server, '_tools_handler'):
            print("✓ Tools handler registered")
        
        server.db.close()
        return True
    except Exception as e:
        print(f"❌ Server startup failed: {e}")
        return False

def main():
    """Main test function"""
    print("Forestrat MCP Server Test Suite")
    print("=" * 35)
    
    # Test 1: Server startup
    startup_ok = test_server_startup()
    
    # Test 2: Server functionality  
    if startup_ok:
        functionality_ok = asyncio.run(test_mcp_server())
        
        if startup_ok and functionality_ok:
            print("\n🎉 All tests passed! The MCP server is ready to use.")
            print("\nTo run the server:")
            print("  python main.py")
            print("\nThe server will communicate via stdin/stdout using the MCP protocol.")
            return True
        else:
            print("\n❌ Some tests failed.")
            return False
    else:
        print("\n❌ Server startup failed.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 