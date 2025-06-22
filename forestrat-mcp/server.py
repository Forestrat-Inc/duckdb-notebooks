#!/usr/bin/env python3
"""
Simple server runner for Forestrat MCP Server
"""

import sys
import json
import logging
from main import ForestratMCPServer

def run_server():
    """Run the MCP server"""
    try:
        server = ForestratMCPServer()
        
        # Test database connection
        if not server.db.test_connection():
            print(json.dumps({
                "error": "Failed to connect to database",
                "database_path": server.db.database_path
            }))
            sys.exit(1)
        
        # Print server info
        print(json.dumps({
            "status": "ready",
            "server": "forestrat-mcp",
            "database": str(server.db.database_path),
            "connection": "active"
        }))
        
        # Run the server
        import asyncio
        asyncio.run(server.run())
        
    except Exception as e:
        print(json.dumps({
            "error": str(e),
            "status": "failed"
        }))
        sys.exit(1)

if __name__ == "__main__":
    run_server() 