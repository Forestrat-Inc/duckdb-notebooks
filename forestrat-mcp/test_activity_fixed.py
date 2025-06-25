#!/usr/bin/env python3
"""
Test script for the activity analysis tools in main_fixed.py
"""

import asyncio
import sys
from pathlib import Path
import json

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from main_fixed import ForestratMCPServer

async def test_activity_tools():
    """Test the most active and least active symbols tools"""
    
    # Initialize server
    server = ForestratMCPServer()
    
    # Test date from our database
    test_date = "2025-02-19"
    test_exchange = "LSE"
    
    print(f"Testing activity analysis for {test_exchange} on {test_date}")
    print("=" * 60)
    
    # Test most active symbols by trade count
    print("\n1. Most Active Symbols (by trade count):")
    try:
        result = await server._get_most_active_symbols(
            date=test_date,
            exchange=test_exchange,
            metric="trade_count",
            limit=5
        )
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")
    
    # Test most active symbols by volume
    print("\n2. Most Active Symbols (by volume):")
    try:
        result = await server._get_most_active_symbols(
            date=test_date,
            exchange=test_exchange,
            metric="volume",
            limit=5
        )
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")
    
    # Test least active symbols by trade count
    print("\n3. Least Active Symbols (by trade count):")
    try:
        result = await server._get_least_active_symbols(
            date=test_date,
            exchange=test_exchange,
            metric="trade_count",
            limit=5
        )
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")
    
    # Test least active symbols by volume
    print("\n4. Least Active Symbols (by volume):")
    try:
        result = await server._get_least_active_symbols(
            date=test_date,
            exchange=test_exchange,
            metric="volume",
            limit=5
        )
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")

    # Test with different exchanges
    print(f"\n5. Most Active Symbols for CME:")
    try:
        result = await server._get_most_active_symbols(
            date=test_date,
            exchange="CME",
            metric="trade_count",
            limit=3
        )
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_activity_tools()) 