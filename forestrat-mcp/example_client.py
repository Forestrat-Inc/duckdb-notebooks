#!/usr/bin/env python3
"""
Example client for Forestrat MCP Server

This script demonstrates how to interact with the MCP server tools.
"""

import asyncio
import json
from main import ForestratMCPServer

async def demo_mcp_tools():
    """Demonstrate the MCP server tools"""
    print("Forestrat MCP Server Demo")
    print("=" * 30)
    
    try:
        # Initialize the server (without running the MCP protocol)
        server = ForestratMCPServer()
        print("✓ MCP Server initialized")
        
        # Demo 1: List all datasets
        print("\n1. Listing all datasets...")
        try:
            datasets = await server._list_datasets(include_stats=True)
            print(f"Found {len(datasets.get('schemas', {}))} schemas:")
            for schema_name, schema_info in datasets.get('schemas', {}).items():
                print(f"  {schema_name}: {schema_info['description']}")
                print(f"    Tables: {len(schema_info['tables'])}")
        except Exception as e:
            print(f"Error: {e}")
        
        # Demo 2: Get exchanges for LSE dataset (bronze table)
        print("\n2. Getting exchanges for 'lse' dataset...")
        try:
            exchanges = await server._get_dataset_exchanges("lse")
            if 'error' in exchanges:
                print(f"Note: {exchanges['note']}")
            else:
                print(f"Table: {exchanges['table']}")
                for exchange in exchanges.get('exchanges', []):
                    print(f"  {exchange['exchange']}: {exchange['record_count']:,} records")
                    print(f"    Date range: {exchange['earliest_date']} to {exchange['latest_date']}")
                    print(f"    Unique symbols: {exchange['unique_symbols']:,}")
        except Exception as e:
            print(f"Error: {e}")
        
        # Demo 3: Get table schema
        print("\n3. Getting schema for bronze.lse_market_data...")
        try:
            schema = await server._get_table_schema("bronze.lse_market_data")
            print(f"Table: {schema['table_name']}")
            print("Columns:")
            for col in schema['columns'][:5]:  # Show first 5 columns
                print(f"  {col['column_name']}: {col['column_type']}")
            if len(schema['columns']) > 5:
                print(f"  ... and {len(schema['columns']) - 5} more columns")
        except Exception as e:
            print(f"Error: {e}")
        
        # Demo 4: Get available symbols for LSE
        print("\n4. Getting available symbols for LSE...")
        try:
            symbols = await server._get_available_symbols("LSE")
            print(f"Exchange: {symbols['exchange']}")
            print(f"Total symbols: {symbols['symbol_count']}")
            print("Top 5 symbols by trade count:")
            for symbol in symbols['symbols'][:5]:
                print(f"  {symbol['symbol']}: {symbol['trade_count']:,} trades")
        except Exception as e:
            print(f"Error: {e}")
        
        # Demo 5: Execute a custom query
        print("\n5. Executing custom query...")
        try:
            query = """
            SELECT 
                exchange,
                COUNT(DISTINCT "#RIC") as unique_symbols,
                COUNT(*) as total_records
            FROM bronze.lse_market_data 
            GROUP BY exchange
            ORDER BY total_records DESC
            """
            result = await server._query_data(query, limit=10)
            print("Query results:")
            for row in result['data']:
                print(f"  {row['exchange']}: {row['unique_symbols']} symbols, {row['total_records']:,} records")
        except Exception as e:
            print(f"Error: {e}")
        
        # Demo 6: Get data for specific time range from bronze table
        print("\n6. Getting data for time range from LSE...")
        try:
            # Use the actual date where data exists (2025-02-19)
            data = await server._get_data_for_time_range(
                dataset="lse",  # Use 'lse' which maps to bronze.lse_market_data
                start_date="2025-02-19",
                end_date="2025-02-19",  # Single day where data exists
                limit=3
            )
            print(f"Retrieved {data['record_count']} records from {data['table']}")
            print(f"Date range: {data['start_date']} to {data['end_date']}")
            if data['data']:
                print("Sample records:")
                for i, record in enumerate(data['data'][:2], 1):
                    print(f"  Record {i}:")
                    # Show key fields that are most relevant
                    key_fields = ['#RIC', 'Price', 'Volume', 'exchange', 'data_date']
                    for field in key_fields:
                        if field in record:
                            value = record[field]
                            # Format numeric values nicely
                            if field == 'Price' and isinstance(value, (int, float)):
                                print(f"    {field}: ${value:.2f}")
                            elif field == 'Volume' and isinstance(value, (int, float)):
                                print(f"    {field}: {value:,}")
                            else:
                                print(f"    {field}: {value}")
            else:
                print("No data found for this date range")
        except Exception as e:
            print(f"Error: {e}")
        
        # Demo 7: Compare data across exchanges
        print("\n7. Comparing data across all exchanges...")
        try:
            exchanges = ['LSE', 'CME', 'NYQ']
            for exchange in exchanges:
                try:
                    symbols = await server._get_available_symbols(exchange)
                    if 'error' not in symbols:
                        print(f"  {exchange}: {symbols['symbol_count']:,} symbols")
                    else:
                        print(f"  {exchange}: No data available")
                except Exception as ex:
                    print(f"  {exchange}: Error - {ex}")
        except Exception as e:
            print(f"Error: {e}")
        
        # Demo 8: Advanced query - Top traded symbols
        print("\n8. Finding top traded symbols across LSE...")
        try:
            query = """
            SELECT 
                "#RIC" as symbol,
                COUNT(*) as trade_count,
                AVG(Price) as avg_price,
                SUM(Volume) as total_volume,
                MIN(data_date) as first_date,
                MAX(data_date) as last_date
            FROM bronze.lse_market_data 
            WHERE Price > 0 AND Volume > 0
            GROUP BY "#RIC"
            ORDER BY trade_count DESC
            LIMIT 5
            """
            result = await server._query_data(query, limit=5)
            print("Top 5 most traded symbols:")
            for row in result['data']:
                print(f"  {row['symbol']}: {row['trade_count']:,} trades, avg price: ${row['avg_price']:.2f}")
        except Exception as e:
            print(f"Error: {e}")
        
        print("\n✓ Demo completed successfully!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
    finally:
        # Clean up
        try:
            server.db.close()
        except:
            pass

def show_tool_schemas():
    """Show the JSON schemas for all available tools"""
    print("\nMCP Tool Schemas")
    print("=" * 20)
    
    tools = [
        {
            "name": "list_datasets",
            "description": "List all datasets with vendor information and exchanges",
            "parameters": {
                "include_stats": "boolean (optional) - Include record counts and date ranges"
            }
        },
        {
            "name": "get_dataset_exchanges", 
            "description": "Get all exchanges available for a specific dataset",
            "parameters": {
                "dataset": "string (required) - Dataset name (lse, cme, nyq, unified, etc.)"
            }
        },
        {
            "name": "get_data_for_time_range",
            "description": "Get data for a specific dataset and time range", 
            "parameters": {
                "dataset": "string (required) - Dataset name (lse, cme, nyq)",
                "start_date": "string (required) - Start date (YYYY-MM-DD)",
                "end_date": "string (required) - End date (YYYY-MM-DD)",
                "exchange": "string (optional) - Exchange filter",
                "limit": "integer (optional) - Max records (default: 1000)"
            }
        },
        {
            "name": "query_data",
            "description": "Execute SQL-like queries on the data",
            "parameters": {
                "query": "string (required) - SQL query to execute",
                "limit": "integer (optional) - Max records (default: 1000)"
            }
        },
        {
            "name": "get_table_schema",
            "description": "Get the schema/structure of a specific table",
            "parameters": {
                "table_name": "string (required) - Table name (bronze.lse_market_data, etc.)"
            }
        },
        {
            "name": "get_available_symbols",
            "description": "Get available symbols/instruments for a given exchange",
            "parameters": {
                "exchange": "string (required) - Exchange name (LSE, CME, NYQ)",
                "start_date": "string (optional) - Start date filter",
                "end_date": "string (optional) - End date filter"
            }
        }
    ]
    
    for tool in tools:
        print(f"\n{tool['name']}:")
        print(f"  Description: {tool['description']}")
        print("  Parameters:")
        for param, desc in tool['parameters'].items():
            print(f"    {param}: {desc}")

def show_available_datasets():
    """Show information about available datasets"""
    print("\nAvailable Datasets")
    print("=" * 18)
    print("The MCP server provides access to the following datasets:")
    print()
    print("Bronze Layer (Raw Data):")
    print("  • lse     → bronze.lse_market_data     (52K+ symbols, 117M+ records)")  
    print("  • cme     → bronze.cme_market_data     (143K+ records)")
    print("  • nyq     → bronze.nyq_market_data     (87M+ records)")
    print()
    print("Silver Layer (Unified Data):")
    print("  • unified → silver.market_data_unified (Not yet created)")
    print()
    print("Gold Layer (Aggregated Data):")
    print("  • daily_summary → gold.daily_market_summary (Not yet created)")
    print("  • arbitrage     → gold.arbitrage_opportunities (Not yet created)")
    print()
    print("Key Column Mappings:")
    print("  • Symbol/Instrument: '#RIC' (note the quotes due to # character)")
    print("  • Price: 'Price'")
    print("  • Volume: 'Volume'")
    print("  • Timestamp: 'Date-Time'")
    print("  • Date: 'data_date'")
    print("  • Exchange: 'exchange'")

async def main():
    """Main function"""
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--schemas":
            show_tool_schemas()
        elif sys.argv[1] == "--datasets":
            show_available_datasets()
        else:
            print("Usage: python example_client.py [--schemas|--datasets]")
            print("  --schemas  : Show MCP tool schemas")
            print("  --datasets : Show available datasets")
            print("  (no args) : Run demo")
    else:
        await demo_mcp_tools()

if __name__ == "__main__":
    asyncio.run(main()) 