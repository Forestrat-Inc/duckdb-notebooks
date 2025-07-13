#!/usr/bin/env python3
"""
Development Tools for Forestrat MCP Tools

This script allows you to test individual MCP tools locally without running 
the full MCP server. Useful for debugging and development.

Usage:
    python dev_tools.py <tool_name> [arguments]

Examples:
    python dev_tools.py list_datasets
    python dev_tools.py get_next_futures_symbols --product_type bitcoin --start_month_name February --start_year 2025 --num_futures 6
    python dev_tools.py query_data --query "SELECT * FROM bronze.lse_market_data LIMIT 5"
"""

import sys
import os
import argparse
import asyncio
import json
from pathlib import Path

# Add python-utils directory to Python path
PYTHON_UTILS_DIR = Path(__file__).parent.parent.parent / "python-utils"
if PYTHON_UTILS_DIR.exists():
    sys.path.insert(0, str(PYTHON_UTILS_DIR))
    print(f"✓ Added to Python path: {PYTHON_UTILS_DIR}")
else:
    print(f"✗ Could not find python-utils directory at: {PYTHON_UTILS_DIR}")
    sys.exit(1)

# Import required modules
try:
    from forestrat_utils.mcp_tools import ForestratTools
    from forestrat_utils.database import DuckDBConnection
    from forestrat_utils.config import Config
    print("✓ Successfully imported forestrat_utils modules")
except ImportError as e:
    print(f"✗ Failed to import forestrat_utils: {e}")
    sys.exit(1)

class DevToolsRunner:
    def __init__(self, database_path: str = None):
        """Initialize the development tools runner"""
        config = Config()
        if database_path:
            config.database_path = database_path
        
        try:
            self.db = DuckDBConnection(config.database_path)
            print(f"✓ Connected to database: {config.database_path}")
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            sys.exit(1)
        
        self.tools = ForestratTools(self.db)
        print("✓ Initialized ForestratTools")

    async def run_tool(self, tool_name: str, **kwargs):
        """Run a specific tool with given arguments"""
        try:
            if tool_name == "list_datasets":
                result = await self.tools.list_datasets(kwargs.get("include_stats", False))
            
            elif tool_name == "get_dataset_exchanges":
                if "dataset" not in kwargs:
                    raise ValueError("dataset argument is required")
                result = await self.tools.get_dataset_exchanges(kwargs["dataset"])
            
            elif tool_name == "get_data_for_time_range":
                required_args = ["dataset", "start_date", "end_date"]
                for arg in required_args:
                    if arg not in kwargs:
                        raise ValueError(f"{arg} argument is required")
                result = await self.tools.get_data_for_time_range(
                    kwargs["dataset"],
                    kwargs["start_date"],
                    kwargs["end_date"],
                    kwargs.get("exchange"),
                    kwargs.get("symbol"),
                    kwargs.get("limit", 1000)
                )
            
            elif tool_name == "query_data":
                if "query" not in kwargs:
                    raise ValueError("query argument is required")
                result = await self.tools.query_data(
                    kwargs["query"],
                    kwargs.get("limit", 1000)
                )
            
            elif tool_name == "get_table_schema":
                if "table_name" not in kwargs:
                    raise ValueError("table_name argument is required")
                result = await self.tools.get_table_schema(kwargs["table_name"])
            
            elif tool_name == "get_available_symbols":
                if "exchange" not in kwargs:
                    raise ValueError("exchange argument is required")
                result = await self.tools.get_available_symbols(
                    kwargs["exchange"],
                    kwargs.get("start_date"),
                    kwargs.get("end_date"),
                    kwargs.get("limit", 1000)
                )
            
            elif tool_name == "get_next_futures_symbols":
                required_args = ["product_type", "start_month_name", "start_year", "num_futures"]
                for arg in required_args:
                    if arg not in kwargs:
                        raise ValueError(f"{arg} argument is required")
                result = await self.tools.get_next_futures_symbols(
                    kwargs["product_type"],
                    kwargs["start_month_name"],
                    kwargs["start_year"],
                    kwargs["num_futures"]
                )
            
            elif tool_name == "get_symbols_by_category":
                if "category" not in kwargs:
                    raise ValueError("category argument is required")
                result = await self.tools.get_symbols_by_category(
                    kwargs["category"],
                    kwargs.get("limit", 1000)
                )
            
            else:
                raise ValueError(f"Unknown tool: {tool_name}")
            
            return result
            
        except Exception as e:
            import traceback
            error_msg = f"Error in {tool_name}: {str(e)}\n{traceback.format_exc()}"
            return {"error": error_msg}

    def list_available_tools(self):
        """List all available tools"""
        tools = [
            "list_datasets",
            "get_dataset_exchanges", 
            "get_data_for_time_range",
            "query_data",
            "get_table_schema",
            "get_available_symbols",
            "get_next_futures_symbols",
            "get_symbols_by_category"
        ]
        
        print("Available tools:")
        for tool in tools:
            print(f"  - {tool}")
        
        return tools

def main():
    parser = argparse.ArgumentParser(description="Development Tools for Forestrat MCP Tools")
    parser.add_argument("tool_name", nargs="?", help="Name of the tool to run (or 'list' to see available tools)")
    parser.add_argument("--database-path", "-d", help="Path to the DuckDB database file", default=None)
    
    # Common arguments
    parser.add_argument("--dataset", help="Dataset name")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--exchange", help="Exchange name")
    parser.add_argument("--symbol", help="Symbol name")
    parser.add_argument("--query", help="SQL query")
    parser.add_argument("--table-name", help="Table name")
    parser.add_argument("--category", help="Symbol category")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to return")
    parser.add_argument("--include-stats", action="store_true", help="Include statistics")
    
    # Futures-specific arguments
    parser.add_argument("--product-type", help="Product type (bitcoin, micro bitcoin, etc.)")
    parser.add_argument("--start-month-name", help="Starting month name")
    parser.add_argument("--start-year", type=int, help="Starting year")
    parser.add_argument("--num-futures", type=int, help="Number of futures symbols to generate")
    
    args = parser.parse_args()
    
    # Create runner
    runner = DevToolsRunner(args.database_path)
    
    # Handle list command
    if args.tool_name == "list" or args.tool_name is None:
        runner.list_available_tools()
        return
    
    # Prepare arguments
    kwargs = {}
    for key, value in vars(args).items():
        if value is not None and key not in ["tool_name", "database_path"]:
            # Convert dashes to underscores for argument names
            clean_key = key.replace("-", "_")
            kwargs[clean_key] = value
    
    # Run the tool
    try:
        result = asyncio.run(runner.run_tool(args.tool_name, **kwargs))
        
        # Pretty print the result
        if isinstance(result, dict) and "error" in result:
            print(f"❌ {result['error']}")
        else:
            print("✅ Result:")
            print(json.dumps(result, indent=2, default=str))
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 