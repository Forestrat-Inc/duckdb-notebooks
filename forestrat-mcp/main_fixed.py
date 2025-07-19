#!/usr/bin/env python3
"""
Forestrat MCP Server - Fixed Version with Streaming

Multi-exchange data lake MCP server with manual JSON-RPC implementation 
to avoid pydantic validation issues in the MCP library.

This server provides access to DuckDB data lake containing:
- LSE (London Stock Exchange) market data  
- CME (Chicago Mercantile Exchange) data
- NYQ (New York Stock Exchange) data

Features:
- Streaming responses for long-running operations
- Progress notifications during tool execution
- Compatible with MCP protocol via JSON-RPC over stdio
"""

import asyncio
import json
import logging
import sys
import os
import uuid
from typing import Any, Dict, List, Optional, AsyncGenerator
from datetime import datetime

from database import DuckDBConnection
from config import Config

config = Config()
TABLE_MAPPINGS = config.DATASET_MAPPING

# Configure logging
import os
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'forestrat_mcp_server.log')
log_handlers = [
    logging.StreamHandler(sys.stderr),  # Log to stderr to avoid interfering with stdout JSON-RPC
    logging.FileHandler(log_file_path, mode='a')  # Also log to file in current directory
]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=log_handlers
)
logger = logging.getLogger(__name__)

class StreamingProgress:
    """Class to handle streaming progress updates"""
    
    def __init__(self, server_instance, request_id: str, tool_name: str):
        self.server = server_instance
        self.request_id = request_id
        self.tool_name = tool_name
        self.step_count = 0
        
    async def update(self, message: str, progress_percent: Optional[float] = None, data: Optional[Dict] = None):
        """Send a progress update notification"""
        self.step_count += 1
        
        notification = {
            "jsonrpc": "2.0",
            "method": "notifications/progress",
            "params": {
                "progressToken": self.request_id,
                "value": {
                    "kind": "report",
                    "message": message,
                    "percentage": progress_percent,
                    "step": self.step_count,
                    "tool": self.tool_name,
                    "data": data or {}
                }
            }
        }
        
        await self.server._send_notification(notification)
        
    async def complete(self, message: str = "Operation completed"):
        """Send completion notification"""
        notification = {
            "jsonrpc": "2.0", 
            "method": "notifications/progress",
            "params": {
                "progressToken": self.request_id,
                "value": {
                    "kind": "end",
                    "message": message,
                    "percentage": 100,
                    "step": self.step_count + 1,
                    "tool": self.tool_name
                }
            }
        }
        
        await self.server._send_notification(notification)

class ForestratMCPServer:
    """Forestrat MCP Server using manual JSON-RPC implementation with streaming support"""
    
    def __init__(self, database_path: Optional[str] = None):
        import os
        if database_path is None:
            database_path = os.getenv("DATABASE_PATH", "../multi_exchange_data_lake.duckdb")
        
        self.db = DuckDBConnection(database_path)
        self.tools = ForestratTools(self.db)
        self.initialized = False
        self.streaming_enabled = True  # Enable streaming by default
        logger.info("Forestrat MCP Server with Streaming initialized")
    
    async def _send_notification(self, notification: Dict[str, Any]):
        """Send a notification (no response expected)"""
        try:
            notification_json = json.dumps(notification, ensure_ascii=True, separators=(',', ':'))
            print(notification_json, flush=True)
            logger.info(f"📡 Sent progress notification: {notification['params']['value']['message']}")
        except Exception as e:
            logger.error(f"❌ Error sending notification: {e}")
    
    def create_response(self, request_id: Optional[Any], result: Any) -> Dict[str, Any]:
        """Create a JSON-RPC response"""
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result
        }
    
    def create_error(self, request_id: Optional[Any], code: int, message: str) -> Dict[str, Any]:
        """Create a JSON-RPC error response"""
        return {
            "jsonrpc": "2.0", 
            "id": request_id,
            "error": {
                "code": code,
                "message": message
            }
        }
    
    def handle_initialize(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle initialize request"""
        logger.info("Handling initialize request")
        self.initialized = True
        
        # Check if client supports progress notifications
        client_capabilities = params.get("capabilities", {})
        progress_notifications = client_capabilities.get("experimental", {}).get("progressNotifications")
        self.streaming_enabled = isinstance(progress_notifications, dict) or progress_notifications is not None
        
        logger.info(f"Streaming enabled: {self.streaming_enabled}")
        
        return self.create_response(request_id, {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {},
                "prompts": {},
                "resources": {},
                "experimental": {
                    "progressNotifications": {}
                }
            },
            "serverInfo": {
                "name": "forestrat-mcp",
                "version": "1.0.0"
            }
        })
    
    def handle_list_tools(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/list request"""
        logger.info("Handling tools/list request")
        
        if not self.initialized:
            return self.create_error(request_id, -32002, "Server not initialized")
        
        tools = [
            {
                "name": "list_datasets",
                "description": "List all datasets with vendor information and exchanges",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "include_stats": {
                            "type": "boolean",
                            "description": "Include record counts and date ranges"
                        }
                    },
                    "additionalProperties": False
                }
            },
            {
                "name": "get_dataset_exchanges",
                "description": "Get all exchanges available for a specific dataset",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dataset": {
                            "type": "string",
                            "description": "Dataset name (e.g., 'market_data', 'bronze', 'silver', 'gold')"
                        }
                    },
                    "required": ["dataset"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_data_for_time_range",
                "description": "Get data for a specific dataset and time range",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "dataset": {
                            "type": "string",
                            "description": "Dataset name or table name"
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Start date (YYYY-MM-DD)"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date",
                            "description": "End date (YYYY-MM-DD)"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange filter (LSE, CME, NYQ)"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of records to return"
                        }
                    },
                    "required": ["dataset", "start_date", "end_date"],
                    "additionalProperties": False
                }
            },
            {
                "name": "query_data",
                "description": "Execute SQL-like queries on the data",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL query to execute"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of records to return"
                        }
                    },
                    "required": ["query"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_table_schema",
                "description": "Get the schema/structure of a specific table",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Table name (e.g., 'bronze.lse_market_data')"
                        }
                    },
                    "required": ["table_name"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_available_symbols",
                "description": "Get available symbols/instruments for a given exchange and date range",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "exchange": {
                            "type": "string",
                            "description": "Exchange name (LSE, CME, NYQ)"
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Start date (YYYY-MM-DD)"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date",
                            "description": "End date (YYYY-MM-DD)"
                        }
                    },
                    "required": ["exchange"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_most_active_symbols",
                "description": "Get the most active symbols for a specific date based on volume or trade count",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "date": {
                            "type": "string",
                            "format": "date",
                            "description": "Date to analyze (YYYY-MM-DD)"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange name (LSE, CME, NYQ)"
                        },
                        "metric": {
                            "type": "string",
                            "enum": ["volume", "trade_count"],
                            "description": "Metric to use for activity (volume or trade_count)"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of top symbols to return (default: 10)"
                        }
                    },
                    "required": ["date", "exchange"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_least_active_symbols",
                "description": "Get the least active symbols for a specific date based on volume or trade count",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "date": {
                            "type": "string",
                            "format": "date",
                            "description": "Date to analyze (YYYY-MM-DD)"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange name (LSE, CME, NYQ)"
                        },
                        "metric": {
                            "type": "string",
                            "enum": ["volume", "trade_count"],
                            "description": "Metric to use for activity (volume or trade_count)"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of bottom symbols to return (default: 10)"
                        }
                    },
                    "required": ["date", "exchange"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_symbols_by_category",
                "description": "Get predefined symbol lists by category (e.g., bitcoin_futures, ethereum_futures) for efficient queries without expensive LIKE operations",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "category": {
                            "type": "string",
                            "enum": ["bitcoin_futures", "ethereum_futures", "crypto_futures", "micro_bitcoin", "standard_bitcoin", "micro_ethereum", "standard_ethereum"],
                            "description": "Symbol category to retrieve"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange to filter by (optional)"
                        },
                        "include_stats": {
                            "type": "boolean",
                            "description": "Include trading statistics for the symbols (default: false)"
                        },
                        "date": {
                            "type": "string",
                            "format": "date",
                            "description": "Date for statistics (required if include_stats is true)"
                        }
                    },
                    "required": ["category"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_category_volume_data",
                "description": "Get volume and trading data for a specific symbol category (optimized for queries like 'bitcoin futures volume')",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "category": {
                            "type": "string",
                            "enum": ["bitcoin_futures", "ethereum_futures", "crypto_futures", "micro_bitcoin", "standard_bitcoin", "micro_ethereum", "standard_ethereum"],
                            "description": "Symbol category to analyze"
                        },
                        "date": {
                            "type": "string",
                            "format": "date",
                            "description": "Date to analyze (YYYY-MM-DD)"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange name (LSE, CME, NYQ)"
                        },
                        "metric": {
                            "type": "string",
                            "enum": ["volume", "trade_count", "both"],
                            "description": "Metric to retrieve (default: both)"
                        }
                    },
                    "required": ["category", "date", "exchange"],
                    "additionalProperties": False
                }
            },
            {
                "name": "export_category_data",
                "description": "Export all data for a specific futures category to a CSV file",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "category": {
                            "type": "string",
                            "enum": ["bitcoin_futures", "ethereum_futures", "crypto_futures", "micro_bitcoin", "standard_bitcoin", "micro_ethereum", "standard_ethereum"],
                            "description": "Symbol category to export"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange name (LSE, CME, NYQ)"
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Start date for data export (YYYY-MM-DD, optional)"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date",
                            "description": "End date for data export (YYYY-MM-DD, optional)"
                        },
                        "output_filename": {
                            "type": "string",
                            "description": "Output filename (optional, will auto-generate if not provided)"
                        },
                        "format": {
                            "type": "string",
                            "enum": ["csv", "json"],
                            "description": "Export format (default: csv)"
                        }
                    },
                    "required": ["category", "exchange"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_next_futures_symbols",
                "description": "Generate the next N futures symbols for a product type (bitcoin -> BTC, micro bitcoin -> MBT, others return 'work in progress')",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "product_type": {
                            "type": "string",
                            "description": "Type of product: 'bitcoin', 'micro bitcoin', 'standard bitcoin', 'btc', 'mbt', or any other (returns work in progress)"
                        },
                        "start_month_name": {
                            "type": "string",
                            "enum": ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
                            "description": "Starting month name (full month name with first letter capitalized)"
                        },
                        "start_year": {
                            "type": "integer",
                            "description": "Starting year (e.g., 2025)",
                            "minimum": 2020,
                            "maximum": 2030
                        },
                        "num_futures": {
                            "type": "integer",
                            "description": "Number of consecutive futures contracts to generate",
                            "minimum": 1,
                            "maximum": 24
                        }
                    },
                    "required": ["product_type", "start_month_name", "start_year", "num_futures"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_unique_futures_count",
                "description": "Get the number of unique futures instruments, optionally filtered by exchange and date range, with detailed symbol information",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "exchange": {
                            "type": "string",
                            "description": "Exchange filter (LSE, CME, NYQ)"
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Start date filter (YYYY-MM-DD)"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date",
                            "description": "End date filter (YYYY-MM-DD)"
                        },
                        "include_details": {
                            "type": "boolean",
                            "description": "Include detailed symbol information with trading statistics"
                        }
                    },
                    "additionalProperties": False
                }
            },
            {
                "name": "get_btc_eth_futures_volume_correlation",
                "description": "Compute the correlation between bitcoin and ether futures daily volume for a date range (CME only)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Start date (YYYY-MM-DD)"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date",
                            "description": "End date (YYYY-MM-DD)"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange to use (default: CME)"
                        }
                    },
                    "required": ["start_date", "end_date"],
                    "additionalProperties": False
                }
            },
            {
                "name": "generate_minute_bars_csv",
                "description": "Generate minute bars for specified symbols and export to CSV file",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbols": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of RIC symbols to process (e.g., ['BTCH25', 'BTCM25'])"
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Start date (YYYY-MM-DD)"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date",
                            "description": "End date (YYYY-MM-DD)"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange name (default: CME)"
                        },
                        "output_filename": {
                            "type": "string",
                            "description": "Output filename (optional, auto-generated if not provided)"
                        },
                        "session_start": {
                            "type": "string",
                            "description": "Trading session start time (HH:MM:SS, default: 08:00:00)"
                        },
                        "session_end": {
                            "type": "string",
                            "description": "Trading session end time (HH:MM:SS, default: 17:00:00)"
                        }
                    },
                    "required": ["symbols", "start_date", "end_date"],
                    "additionalProperties": False
                }
            },
            {
                "name": "generate_minute_bars_data",
                "description": "Generate minute bars data for Jupyter notebook analysis",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbols": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of RIC symbols to process (e.g., ['BTCH25', 'BTCM25'])"
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Start date (YYYY-MM-DD)"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date",
                            "description": "End date (YYYY-MM-DD)"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange name (default: CME)"
                        },
                        "session_start": {
                            "type": "string",
                            "description": "Trading session start time (HH:MM:SS, default: 08:00:00)"
                        },
                        "session_end": {
                            "type": "string",
                            "description": "Trading session end time (HH:MM:SS, default: 17:00:00)"
                        }
                    },
                    "required": ["symbols", "start_date", "end_date"],
                    "additionalProperties": False
                }
            },
            {
                "name": "generate_minute_bars_python_function",
                "description": "Generate a complete Python function for minute bars analysis that users can modify and run",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbols": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of RIC symbols to process (e.g., ['BTCH25', 'BTCM25'])"
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "description": "Start date (YYYY-MM-DD)"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date",
                            "description": "End date (YYYY-MM-DD)"
                        },
                        "exchange": {
                            "type": "string",
                            "description": "Exchange name (default: CME)"
                        },
                        "session_start": {
                            "type": "string",
                            "description": "Trading session start time (HH:MM:SS, default: 08:00:00)"
                        },
                        "session_end": {
                            "type": "string",
                            "description": "Trading session end time (HH:MM:SS, default: 17:00:00)"
                        }
                    },
                    "required": ["symbols", "start_date", "end_date"],
                    "additionalProperties": False
                }
            },
            {
                "name": "check_exchange_holidays",
                "description": "Check if a specific date is a holiday for a given exchange using web scraping and LLM analysis (CME, LSE, NYQ)",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "exchange": {
                            "type": "string",
                            "enum": ["CME", "LSE", "NYQ"],
                            "description": "Exchange name (CME, LSE, or NYQ)"
                        },
                        "date": {
                            "type": "string",
                            "format": "date",
                            "description": "Date to check in YYYY-MM-DD format"
                        },
                        "api_key": {
                            "type": "string",
                            "description": "Firecrawl API key (optional, uses environment variable if not provided)"
                        },
                        "groq_api_key": {
                            "type": "string",
                            "description": "Groq API key for LLM analysis (optional, uses environment variable if not provided)"
                        }
                    },
                    "required": ["exchange", "date"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_exchange_holidays_for_year",
                "description": "Get all holidays for an exchange for a specific year or range of years using AI-powered analysis",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "exchange": {
                            "type": "string",
                            "enum": ["CME", "LSE", "NYQ"],
                            "description": "Exchange name (CME, LSE, or NYQ)"
                        },
                        "year": {
                            "type": "integer",
                            "minimum": 2020,
                            "maximum": 2030,
                            "description": "Starting year for holiday analysis (e.g., 2025)"
                        },
                        "end_year": {
                            "type": "integer",
                            "minimum": 2020,
                            "maximum": 2030,
                            "description": "Optional ending year for multi-year analysis (e.g., 2026)"
                        },
                        "api_key": {
                            "type": "string",
                            "description": "Firecrawl API key (optional, uses environment variable if not provided)"
                        },
                        "groq_api_key": {
                            "type": "string",
                            "description": "Groq API key for LLM analysis (optional, uses environment variable if not provided)"
                        }
                    },
                    "required": ["exchange", "year"],
                    "additionalProperties": False
                }
            }
        ]
        
        return self.create_response(request_id, {
            "tools": tools
        })
    
    def handle_list_prompts(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle prompts/list request"""
        logger.info("Handling prompts/list request")
        
        if not self.initialized:
            return self.create_error(request_id, -32002, "Server not initialized")
        
        prompts = [
            {
                "name": "daily_market_summary",
                "description": "Generate a comprehensive daily market summary with activity, volume leaders, and key statistics",
                "arguments": [
                    {
                        "name": "date",
                        "description": "Trading date to analyze (YYYY-MM-DD)",
                        "required": True
                    },
                    {
                        "name": "exchange", 
                        "description": "Exchange to analyze (LSE, CME, or NYQ)",
                        "required": True
                    },
                    {
                        "name": "top_n",
                        "description": "Number of top/bottom symbols to include (default: 10)",
                        "required": False
                    }
                ]
            },
            {
                "name": "cross_exchange_symbol_analysis",
                "description": "Compare a symbol's trading activity and performance across multiple exchanges",
                "arguments": [
                    {
                        "name": "symbol",
                        "description": "Symbol to analyze (RIC format)",
                        "required": True
                    },
                    {
                        "name": "date",
                        "description": "Analysis date (YYYY-MM-DD)",
                        "required": True
                    },
                    {
                        "name": "exchanges",
                        "description": "Comma-separated list of exchanges to compare (default: LSE,CME,NYQ)",
                        "required": False
                    }
                ]
            },
            {
                "name": "detect_trading_anomalies",
                "description": "Identify unusual trading patterns and outliers for a specific date",
                "arguments": [
                    {
                        "name": "date",
                        "description": "Date to analyze for anomalies (YYYY-MM-DD)",
                        "required": True
                    },
                    {
                        "name": "exchange",
                        "description": "Exchange to analyze (LSE, CME, or NYQ)",
                        "required": True
                    },
                    {
                        "name": "threshold",
                        "description": "Anomaly threshold in standard deviations (default: 3)",
                        "required": False
                    }
                ]
            },
            {
                "name": "volume_trend_analysis",
                "description": "Analyze volume trends and patterns over a date range",
                "arguments": [
                    {
                        "name": "start_date",
                        "description": "Start date for trend analysis (YYYY-MM-DD)",
                        "required": True
                    },
                    {
                        "name": "end_date",
                        "description": "End date for trend analysis (YYYY-MM-DD)",
                        "required": True
                    },
                    {
                        "name": "exchange",
                        "description": "Exchange to analyze (LSE, CME, or NYQ)",
                        "required": True
                    },
                    {
                        "name": "symbols",
                        "description": "Comma-separated list of specific symbols to analyze (optional)",
                        "required": False
                    }
                ]
            },
            {
                "name": "get_quarterly_futures_analysis",
                "description": "Generate futures contracts starting from a specific month and filter to show only quarterly futures (March H, June M, September U, December Z)",
                "arguments": [
                    {
                        "name": "product_type",
                        "description": "Type of product: 'bitcoin', 'micro bitcoin', 'standard bitcoin', 'btc', 'mbt'",
                        "required": True
                    },
                    {
                        "name": "start_month_name",
                        "description": "Starting month name (January, February, March, etc.)",
                        "required": True
                    },
                    {
                        "name": "start_year",
                        "description": "Starting year (e.g., 2025)",
                        "required": True
                    },
                    {
                        "name": "num_quarterly_futures",
                        "description": "Number of quarterly futures contracts to find (default: 2)",
                        "required": False
                    }
                ]
            },
            {
                "name": "complete_quarterly_futures_analysis",
                "description": "Complete 3-step sequential analysis: 1) Generate front futures from start month, 2) Filter to quarterly contracts, 3) Compute trades and quotes for those instruments during continuous sessions",
                "arguments": [
                    {
                        "name": "product_type",
                        "description": "Type of product: 'bitcoin', 'micro bitcoin', 'standard bitcoin', 'btc', 'mbt'",
                        "required": True
                    },
                    {
                        "name": "start_month_name",
                        "description": "Starting month name (January, February, March, etc.)",
                        "required": True
                    },
                    {
                        "name": "start_year",
                        "description": "Starting year (e.g., 2025)",
                        "required": True
                    },
                    {
                        "name": "num_front_months",
                        "description": "Number of front months to generate initially (default: 2)",
                        "required": False
                    },
                    {
                        "name": "analysis_date",
                        "description": "Date for trading analysis (YYYY-MM-DD format, optional - uses latest available if not provided)",
                        "required": False
                    },
                    {
                        "name": "exchange",
                        "description": "Exchange for trading data (CME, LSE, NYQ - default: CME for crypto futures)",
                        "required": False
                    }
                ]
            },
            {
                "name": "analyze_minute_bars",
                "description": "Generate minute bars analysis for specified symbols with comprehensive statistics and data export options",
                "arguments": [
                    {
                        "name": "symbols",
                        "description": "Comma-separated list of RIC symbols to analyze (e.g., 'BTCH25,BTCM25,BTCU25')",
                        "required": True
                    },
                    {
                        "name": "start_date",
                        "description": "Start date for analysis (YYYY-MM-DD)",
                        "required": True
                    },
                    {
                        "name": "end_date",
                        "description": "End date for analysis (YYYY-MM-DD)",
                        "required": True
                    },
                    {
                        "name": "exchange",
                        "description": "Exchange to analyze (default: CME)",
                        "required": False
                    },
                    {
                        "name": "session_start",
                        "description": "Trading session start time (HH:MM:SS, default: 08:00:00)",
                        "required": False
                    },
                    {
                        "name": "session_end",
                        "description": "Trading session end time (HH:MM:SS, default: 17:00:00)",
                        "required": False
                    },
                    {
                        "name": "export_csv",
                        "description": "Whether to export results to CSV (default: false)",
                        "required": False
                    }
                ]
            }
        ]
        
        return self.create_response(request_id, {
            "prompts": prompts
        })
    
    def handle_list_resources(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle resources/list request"""
        logger.info("Handling resources/list request")
        
        if not self.initialized:
            return self.create_error(request_id, -32002, "Server not initialized")
        
        resources = [
            {
                "uri": "forestrat://schemas/bronze",
                "name": "Bronze Layer Schema",
                "description": "Current schema and data types for bronze layer tables",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://schemas/silver", 
                "name": "Silver Layer Schema",
                "description": "Current schema and data types for silver layer tables",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://schemas/gold",
                "name": "Gold Layer Schema", 
                "description": "Current schema and data types for gold layer tables",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://calendars/lse_trading_days",
                "name": "LSE Trading Calendar",
                "description": "Available trading dates for LSE data",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://calendars/cme_trading_days",
                "name": "CME Trading Calendar", 
                "description": "Available trading dates for CME data",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://calendars/nyq_trading_days",
                "name": "NYQ Trading Calendar",
                "description": "Available trading dates for NYQ data", 
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://reports/data_quality",
                "name": "Data Quality Report",
                "description": "Overall data completeness and quality metrics",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://mappings/symbols/LSE",
                "name": "LSE Symbol Directory",
                "description": "Available symbols and their metadata for LSE",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://mappings/symbols/CME", 
                "name": "CME Symbol Directory",
                "description": "Available symbols and their metadata for CME",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://mappings/symbols/NYQ",
                "name": "NYQ Symbol Directory", 
                "description": "Available symbols and their metadata for NYQ",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://stats/database_overview",
                "name": "Database Overview",
                "description": "High-level statistics about the entire database",
                "mimeType": "application/json"
            },
            {
                "uri": "forestrat://categories/symbol_categories",
                "name": "Symbol Categories",
                "description": "Predefined symbol categories for efficient queries (e.g., bitcoin_futures, ethereum_futures)",
                "mimeType": "application/json"
            }
        ]
        
        return self.create_response(request_id, {
            "resources": resources
        })
    
    async def handle_get_prompt(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle prompts/get request"""
        name = params.get("name")
        arguments = params.get("arguments", {})
        
        logger.info(f"📝 Executing prompt: {name}")
        logger.info(f"📝 Prompt arguments: {json.dumps(arguments, indent=2)}")
        
        if not self.initialized:
            return self.create_error(request_id, -32002, "Server not initialized")
        
        try:
            if name == "daily_market_summary":
                content = await self.tools.execute_daily_market_summary(arguments)
                return self.create_response(request_id, {
                    "description": f"Daily market summary for {arguments.get('exchange')} on {arguments.get('date')}",
                    "content": [{"type": "text", "text": content}]
                })
            elif name == "cross_exchange_symbol_analysis":
                content = await self.tools.execute_cross_exchange_analysis(arguments)
                return self.create_response(request_id, {
                    "description": f"Cross-exchange analysis for {arguments.get('symbol')} on {arguments.get('date')}",
                    "content": [{"type": "text", "text": content}]
                })
            elif name == "detect_trading_anomalies":
                content = await self.tools.execute_anomaly_detection(arguments)
                return self.create_response(request_id, {
                    "description": f"Trading anomaly detection for {arguments.get('exchange')} on {arguments.get('date')}",
                    "content": [{"type": "text", "text": content}]
                })
            elif name == "volume_trend_analysis":
                content = await self.tools.execute_volume_trend_analysis(arguments)
                return self.create_response(request_id, {
                    "description": f"Volume trend analysis for {arguments.get('exchange')} from {arguments.get('start_date')} to {arguments.get('end_date')}",
                    "content": [{"type": "text", "text": content}]
                })
            elif name == "get_quarterly_futures_analysis":
                content = await self.tools.execute_quarterly_futures_analysis(arguments)
                return self.create_response(request_id, {
                    "description": f"Quarterly futures analysis for {arguments.get('product_type')} starting from {arguments.get('start_month_name')} {arguments.get('start_year')}",
                    "content": [{"type": "text", "text": content}]
                })
            elif name == "complete_quarterly_futures_analysis":
                content = await self.tools.execute_complete_quarterly_futures_analysis(arguments)
                return self.create_response(request_id, {
                    "description": f"Complete sequential quarterly futures analysis for {arguments.get('product_type')} starting from {arguments.get('start_month_name')} {arguments.get('start_year')}",
                    "content": [{"type": "text", "text": content}]
                })
            elif name == "get_btc_eth_futures_volume_correlation":
                content = await self.tools.get_btc_eth_futures_volume_correlation(
                    arguments["start_date"],
                    arguments["end_date"],
                    arguments.get("exchange", "CME")
                )
                return self.create_response(request_id, {
                    "description": f"BTC-ETH Futures Volume Correlation for {arguments.get('start_date')} to {arguments.get('end_date')} on {arguments.get('exchange', 'CME')}",
                    "content": [{"type": "text", "text": content}]
                })
            elif name == "analyze_minute_bars":
                content = await self.tools.execute_minute_bars_analysis(arguments)
                return self.create_response(request_id, {
                    "description": f"Minute bars analysis for {arguments.get('symbols')} from {arguments.get('start_date')} to {arguments.get('end_date')}",
                    "content": [{"type": "text", "text": content}]
                })
            else:
                return self.create_error(request_id, -32602, f"Unknown prompt: {name}")
                
        except Exception as e:
            logger.error(f"Error executing prompt {name}: {e}")
            return self.create_error(request_id, -32603, f"Internal error: {str(e)}")
    
    async def handle_read_resource(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle resources/read request"""
        uri = params.get("uri")
        
        logger.info(f"📚 Reading resource: {uri}")
        
        if not self.initialized:
            return self.create_error(request_id, -32002, "Server not initialized")
        
        try:
            if uri.startswith("forestrat://schemas/"):
                layer = uri.split("/")[-1]
                content = await self.tools.read_schema_resource(layer)
                return self.create_response(request_id, {
                    "contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(content, indent=2)}]
                })
            elif uri.startswith("forestrat://calendars/"):
                exchange = uri.split("/")[-1].replace("_trading_days", "").upper()
                content = await self.tools.read_calendar_resource(exchange)
                return self.create_response(request_id, {
                    "contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(content, indent=2)}]
                })
            elif uri.startswith("forestrat://mappings/symbols/"):
                exchange = uri.split("/")[-1]
                content = await self.tools.read_symbol_mapping_resource(exchange)
                return self.create_response(request_id, {
                    "contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(content, indent=2)}]
                })
            elif uri == "forestrat://reports/data_quality":
                content = await self.tools.read_data_quality_resource()
                return self.create_response(request_id, {
                    "contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(content, indent=2)}]
                })
            elif uri == "forestrat://stats/database_overview":
                content = await self.tools.read_database_overview_resource()
                return self.create_response(request_id, {
                    "contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(content, indent=2)}]
                })
            elif uri == "forestrat://categories/symbol_categories":
                content = await self.tools.read_symbol_categories_resource()
                return self.create_response(request_id, {
                    "contents": [{"uri": uri, "mimeType": "application/json", "text": json.dumps(content, indent=2)}]
                })
            else:
                return self.create_error(request_id, -32602, f"Unknown resource URI: {uri}")
                
        except Exception as e:
            logger.error(f"Error reading resource {uri}: {e}")
            return self.create_error(request_id, -32603, f"Internal error: {str(e)}")
    
    async def handle_call_tool(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/call request with streaming support"""
        name = params.get("name")
        arguments = params.get("arguments", {})
        
        logger.info(f"🔧 Executing tool: {name}")
        logger.info(f"🔧 Tool arguments: {json.dumps(arguments, indent=2)}")
        
        if not self.initialized:
            logger.error("❌ Server not initialized for tool call")
            return self.create_error(request_id, -32002, "Server not initialized")
        
        # Create streaming progress tracker if enabled
        progress = None
        if self.streaming_enabled:
            progress = StreamingProgress(self, str(request_id), name)
            await progress.update(f"Starting {name} execution", 0)
        
        try:
            if name == "list_datasets":
                if progress:
                    await progress.update("Gathering dataset information", 20)
                result = await self.tools.list_datasets(arguments.get("include_stats", False))
            elif name == "get_dataset_exchanges":
                if progress:
                    await progress.update("Querying dataset exchanges", 30)
                result = await self.tools.get_dataset_exchanges(arguments["dataset"])
            elif name == "get_data_for_time_range":
                if progress:
                    await progress.update("Executing time range query", 25)
                result = await self.tools.get_data_for_time_range(
                    arguments["dataset"],
                    arguments["start_date"],
                    arguments["end_date"],
                    arguments.get("exchange"),
                    arguments.get("limit", 1000)
                )
                if progress:
                    await progress.update("Processing query results", 75)
            elif name == "query_data":
                if progress:
                    await progress.update("Executing SQL query", 30)
                result = await self.tools.query_data(
                    arguments["query"],
                    arguments.get("limit", 1000)
                )
                if progress:
                    await progress.update("Formatting query results", 80)
            elif name == "get_table_schema":
                result = await self.tools.get_table_schema(arguments["table_name"])
            elif name == "get_available_symbols":
                result = await self.tools.get_available_symbols(
                    arguments["exchange"],
                    arguments.get("start_date"),
                    arguments.get("end_date")
                )
            elif name == "get_most_active_symbols":
                result = await self.tools.get_most_active_symbols(
                    arguments["date"],
                    arguments["exchange"],
                    arguments.get("metric", "trade_count"),
                    arguments.get("limit", 10)
                )
            elif name == "get_least_active_symbols":
                result = await self.tools.get_least_active_symbols(
                    arguments["date"],
                    arguments["exchange"],
                    arguments.get("metric", "trade_count"),
                    arguments.get("limit", 10)
                )
            elif name == "get_symbols_by_category":
                result = await self.tools.get_symbols_by_category(
                    arguments["category"],
                    arguments.get("exchange"),
                    arguments.get("include_stats", False),
                    arguments.get("date")
                )
            elif name == "get_category_volume_data":
                result = await self.tools.get_category_volume_data(
                    arguments["category"],
                    arguments["date"],
                    arguments["exchange"],
                    arguments.get("metric", "both")
                )
            elif name == "export_category_data":
                if progress:
                    await progress.update("Preparing data export", 10)
                result = await self.tools.export_category_data(
                    arguments["category"],
                    arguments["exchange"],
                    arguments.get("start_date"),
                    arguments.get("end_date"),
                    arguments.get("output_filename"),
                    arguments.get("format")
                )
                if progress:
                    await progress.update("Export completed successfully", 90)
            elif name == "get_next_futures_symbols":
                if progress:
                    await progress.update("Calculating next futures symbols", 40)
                result = await self.tools.get_next_futures_symbols(
                    arguments["product_type"],
                    arguments["start_month_name"],
                    arguments["start_year"],
                    arguments["num_futures"]
                )
            elif name == "get_unique_futures_count":
                if progress:
                    await progress.update("Analyzing futures contracts", 35)
                result = await self.tools.get_unique_futures_count(
                    arguments.get("exchange"),
                    arguments.get("start_date"),
                    arguments.get("end_date"),
                    arguments.get("include_details", False)
                )
            elif name == "get_btc_eth_futures_volume_correlation":
                if progress:
                    await progress.update("Analyzing BTC-ETH correlation", 20)
                result = await self.tools.get_btc_eth_futures_volume_correlation(
                    arguments["start_date"],
                    arguments["end_date"],
                    arguments.get("exchange", "CME")
                )
                if progress:
                    await progress.update("Correlation analysis complete", 85)
            elif name == "generate_minute_bars_csv":
                if progress:
                    await progress.update("Initializing minute bars generation", 5)
                result = await self.tools.generate_minute_bars_csv(
                    arguments["symbols"],
                    arguments["start_date"],
                    arguments["end_date"],
                    arguments.get("exchange", "CME"),
                    arguments.get("output_filename"),
                    arguments.get("session_start", "08:00:00"),
                    arguments.get("session_end", "17:00:00")
                )
                if progress:
                    await progress.update("Minute bars CSV generation complete", 95)
            elif name == "generate_minute_bars_data":
                if progress:
                    await progress.update("Processing minute bars data", 15)
                result = await self.tools.generate_minute_bars_data(
                    arguments["symbols"],
                    arguments["start_date"],
                    arguments["end_date"],
                    arguments.get("exchange", "CME"),
                    arguments.get("session_start", "08:00:00"),
                    arguments.get("session_end", "17:00:00")
                )
                if progress:
                    await progress.update("Minute bars data processing complete", 90)
            elif name == "generate_minute_bars_python_function":
                if progress:
                    await progress.update("Generating Python function code", 25)
                result = await self.tools.generate_minute_bars_python_function(
                    arguments["symbols"],
                    arguments["start_date"],
                    arguments["end_date"],
                    arguments.get("exchange", "CME"),
                    arguments.get("session_start", "08:00:00"),
                    arguments.get("session_end", "17:00:00")
                )
                if progress:
                    await progress.update("Python function generation complete", 85)
            elif name == "analyze_minute_bars":
                if progress:
                    await progress.update("Starting minute bars analysis", 10)
                result = await self.tools.execute_minute_bars_analysis(arguments)
                if progress:
                    await progress.update("Minute bars analysis complete", 90)
            elif name == "check_exchange_holidays":
                if progress:
                    await progress.update("Checking exchange holiday information", 20)
                result = await self.tools.check_exchange_holidays(
                    arguments["exchange"],
                    arguments["date"],
                    arguments.get("api_key"),
                    arguments.get("groq_api_key")
                )
                if progress:
                    await progress.update("Holiday check complete", 80)
            elif name == "get_exchange_holidays_for_year":
                if progress:
                    await progress.update("Retrieving annual holiday data", 15)
                result = await self.tools.get_exchange_holidays_for_year(
                    arguments["exchange"],
                    arguments["year"],
                    arguments.get("end_year"),
                    arguments.get("api_key"),
                    arguments.get("groq_api_key")
                )
                if progress:
                    await progress.update("Annual holiday data retrieved", 85)
            else:
                logger.error(f"❌ Unknown tool requested: {name}")
                return self.create_error(request_id, -32601, f"Unknown tool: {name}")
                
            # Send completion notification if streaming is enabled
            if progress:
                await progress.complete(f"Tool {name} completed successfully")
                
            logger.info(f"✅ Tool {name} completed successfully")
            logger.info(f"📊 Result summary: {len(str(result))} characters")
            
            return self.create_response(request_id, {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, indent=2, default=str)
                    }
                ]
            })
                
        except Exception as e:
            # Send error notification if streaming is enabled
            if progress:
                await progress.update(f"Error in {name}: {str(e)}", None)
                
            logger.error(f"❌ Error in tool {name}: {str(e)}")
            import traceback
            logger.error(f"❌ Tool error traceback: {traceback.format_exc()}")
            return self.create_error(request_id, -32603, f"Internal error: {str(e)}")
    
    async def handle_request(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle a JSON-RPC request"""
        method = request.get("method")
        request_id = request.get("id")
        params = request.get("params", {})
        
        logger.info(f"📨 Received request - Method: {method}, ID: {request_id}")
        logger.info(f"📋 Request params: {json.dumps(params, indent=2) if params else 'None'}")
        
        if method == "initialize":
            logger.info("🚀 Handling initialization request")
            return self.handle_initialize(request_id, params)
        elif method == "notifications/initialized":
            # This is a notification, no response needed
            logger.info("✅ Received initialized notification")
            return None
        elif method == "tools/list":
            logger.info("🔧 Handling tools/list request")
            return self.handle_list_tools(request_id, params)
        elif method == "tools/call":
            tool_name = params.get("name", "unknown")
            tool_args = params.get("arguments", {})
            logger.info(f"⚙️  Handling tools/call - Tool: {tool_name}")
            logger.info(f"🎯 Tool arguments: {json.dumps(tool_args, indent=2)}")
            return await self.handle_call_tool(request_id, params)
        elif method == "prompts/list":
            logger.info("📝 Handling prompts/list request")
            return self.handle_list_prompts(request_id, params)
        elif method == "prompts/get":
            prompt_name = params.get("name", "unknown")
            logger.info(f"📝 Handling prompts/get - Prompt: {prompt_name}")
            return await self.handle_get_prompt(request_id, params)
        elif method == "resources/list":
            logger.info("📚 Handling resources/list request")
            return self.handle_list_resources(request_id, params)
        elif method == "resources/read":
            resource_uri = params.get("uri", "unknown")
            logger.info(f"📚 Handling resources/read - URI: {resource_uri}")
            return await self.handle_read_resource(request_id, params)
        else:
            logger.warning(f"❌ Unknown method: {method}")
            return self.create_error(request_id, -32601, f"Method not found: {method}")
    
    async def run(self):
        """Run the server with stdio"""
        logger.info("Starting Forestrat MCP server")
        
        try:
            while True:
                # Read line from stdin
                line = await asyncio.get_event_loop().run_in_executor(
                    None, sys.stdin.readline
                )
                
                if not line:
                    break
                
                try:
                    # Parse JSON request
                    request = json.loads(line.strip())
                    logger.info(f"📥 Raw request received: {line.strip()}")
                    
                    # Handle request
                    response = await self.handle_request(request)
                    
                    # Send response if needed
                    if response:
                        try:
                            response_json = json.dumps(response, ensure_ascii=True, separators=(',', ':'))
                            print(response_json, flush=True)
                            logger.info(f"📤 Sending response: {response_json}")
                        except (TypeError, ValueError) as e:
                            logger.error(f"❌ JSON serialization error: {e}")
                            error_response = self.create_error(request.get('id'), -32603, f"JSON serialization error: {str(e)}")
                            error_json = json.dumps(error_response, ensure_ascii=True, separators=(',', ':'))
                            print(error_json, flush=True)
                    else:
                        logger.info("📤 No response needed (notification)")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Invalid JSON received: {e}")
                    logger.error(f"❌ Raw line: {line.strip()}")
                    error_response = self.create_error(None, -32700, "Parse error")
                    error_json = json.dumps(error_response, ensure_ascii=True, separators=(',', ':'))
                    print(error_json, flush=True)
                except Exception as e:
                    logger.error(f"❌ Error handling request: {e}")
                    import traceback
                    logger.error(f"❌ Traceback: {traceback.format_exc()}")
                    error_response = self.create_error(None, -32603, "Internal error")
                    error_json = json.dumps(error_response, ensure_ascii=True, separators=(',', ':'))
                    print(error_json, flush=True)
                    
        except KeyboardInterrupt:
            logger.info("Server shutting down...")
        except Exception as e:
            logger.error(f"Server error: {e}")
            raise
        finally:
            try:
                self.db.close()
            except:
                pass


async def main():
    """Main entry point"""
    import argparse
    from pathlib import Path
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Forestrat MCP Server')
    parser.add_argument('--database-path', '-d', 
                       help='Path to the DuckDB database file',
                       default=None)
    parser.add_argument('--dev', '--development', 
                       action='store_true',
                       help='Run in development mode (adds python-utils to path)')
    args = parser.parse_args()
    
    # Handle development mode - add python-utils to path
    if args.dev:
        PYTHON_UTILS_DIR = Path(__file__).parent.parent.parent / "python-utils"
        if PYTHON_UTILS_DIR.exists():
            sys.path.insert(0, str(PYTHON_UTILS_DIR))
            print(f"✓ Added to Python path: {PYTHON_UTILS_DIR}", file=sys.stderr)
        else:
            print(f"✗ Could not find python-utils directory at: {PYTHON_UTILS_DIR}", file=sys.stderr)
            sys.exit(1)
    
    # Make ForestratTools available globally for the server class
    global ForestratTools
    
    # Import forestrat_utils (either from package or development path)
    try:
        from forestrat_utils import ForestratTools
        if args.dev:
            print("✓ Successfully imported forestrat_utils modules", file=sys.stderr)
    except ImportError as e:
        if args.dev:
            print(f"✗ Failed to import forestrat_utils: {e}", file=sys.stderr)
        else:
            print(f"✗ Failed to import forestrat_utils: {e}", file=sys.stderr)
            print("Try running with --dev flag if working with local development setup", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Initialize the server with optional database path
        server = ForestratMCPServer(database_path=args.database_path)
        
        # Run the server
        await server.run()
            
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
