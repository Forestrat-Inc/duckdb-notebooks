#!/usr/bin/env python3
"""
Forestrat MCP Server for DuckDB Data Lake

This MCP server provides tools to interact with the multi-exchange market data stored in DuckDB.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Sequence
from datetime import datetime, date
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import base64
import io

# Add the parent directory to the path to import from the main project
sys.path.append(str(Path(__file__).parent.parent))

from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.types import (
    CallToolResult,
    ListToolsResult,
    TextContent,
    Tool
)
from database import DuckDBConnection
from pydantic import BaseModel

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("forestrat-mcp")

class ForestratMCPServer:
    """MCP Server for Forestrat DuckDB Data Lake"""
    
    def __init__(self, database_path: Optional[str] = None):
        import os
        if database_path is None:
            database_path = os.getenv("DATABASE_PATH", "../multi_exchange_data_lake.duckdb")
        
        self.server = Server("forestrat-mcp")
        self.db = DuckDBConnection(database_path)
        self._setup_tools()
        
    def _setup_tools(self):
        """Setup all available tools"""
        
        @self.server.list_tools()
        async def handle_list_tools() -> ListToolsResult:
            """List available tools"""
            tools = [
                    Tool(
                        name="list_datasets",
                        description="List all datasets with vendor information and exchanges",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "include_stats": {
                                    "type": "boolean",
                                    "description": "Include record counts and date ranges"
                                }
                            },
                            "additionalProperties": False
                        }
                    ),
                    Tool(
                        name="get_dataset_exchanges",
                        description="Get all exchanges available for a specific dataset",
                        inputSchema={
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
                    ),
                    Tool(
                        name="get_data_for_time_range",
                        description="Get data for a specific dataset and time range",
                        inputSchema={
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
                    ),
                    Tool(
                        name="query_data",
                        description="Execute SQL-like queries on the data",
                        inputSchema={
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
                    ),
                    Tool(
                        name="get_table_schema",
                        description="Get the schema/structure of a specific table",
                        inputSchema={
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
                    ),
                    Tool(
                        name="get_available_symbols",
                        description="Get available symbols/instruments for a given exchange and date range",
                        inputSchema={
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
                    ),
                    Tool(
                        name="get_most_active_symbols",
                        description="Get the most active symbols for a specific date based on volume or trade count",
                        inputSchema={
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
                    ),
                    Tool(
                        name="get_least_active_symbols",
                        description="Get the least active symbols for a specific date based on volume or trade count",
                        inputSchema={
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
                    ),
                    Tool(
                        name="create_activity_plot",
                        description="Create a bar chart plot showing symbol activity (most/least active symbols)",
                        inputSchema={
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
                                "plot_type": {
                                    "type": "string",
                                    "enum": ["most_active", "least_active", "both"],
                                    "description": "Type of plot to create"
                                },
                                "metric": {
                                    "type": "string",
                                    "enum": ["volume", "trade_count"],
                                    "description": "Metric to use for activity (volume or trade_count)"
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Number of symbols to include in plot (default: 10)"
                                }
                            },
                            "required": ["date", "exchange", "plot_type"],
                            "additionalProperties": False
                        }
                    )
                ]
            return ListToolsResult(
                tools=tools,
                nextCursor=None
            )

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> CallToolResult:
            """Handle tool calls"""
            try:
                if name == "list_datasets":
                    result = await self._list_datasets(arguments.get("include_stats", False))
                elif name == "get_dataset_exchanges":
                    result = await self._get_dataset_exchanges(arguments["dataset"])
                elif name == "get_data_for_time_range":
                    result = await self._get_data_for_time_range(
                        arguments["dataset"],
                        arguments["start_date"],
                        arguments["end_date"],
                        arguments.get("exchange"),
                        arguments.get("limit", 1000)
                    )
                elif name == "query_data":
                    result = await self._query_data(
                        arguments["query"],
                        arguments.get("limit", 1000)
                    )
                elif name == "get_table_schema":
                    result = await self._get_table_schema(arguments["table_name"])
                elif name == "get_available_symbols":
                    result = await self._get_available_symbols(
                        arguments["exchange"],
                        arguments.get("start_date"),
                        arguments.get("end_date")
                    )
                elif name == "get_most_active_symbols":
                    result = await self._get_most_active_symbols(
                        arguments["date"],
                        arguments["exchange"],
                        arguments.get("metric", "trade_count"),
                        arguments.get("limit", 10)
                    )
                elif name == "get_least_active_symbols":
                    result = await self._get_least_active_symbols(
                        arguments["date"],
                        arguments["exchange"],
                        arguments.get("metric", "trade_count"),
                        arguments.get("limit", 10)
                    )
                elif name == "create_activity_plot":
                    result = await self._create_activity_plot(
                        arguments["date"],
                        arguments["exchange"],
                        arguments["plot_type"],
                        arguments.get("metric", "trade_count"),
                        arguments.get("limit", 10)
                    )
                else:
                    return CallToolResult(
                        content=[TextContent(type="text", text=f"Unknown tool: {name}")],
                        isError=True
                    )
                    
                return CallToolResult(
                    content=[TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
                )
                
            except Exception as e:
                logger.error(f"Error in tool {name}: {str(e)}")
                return CallToolResult(
                    content=[TextContent(type="text", text=f"Error: {str(e)}")],
                    isError=True
                )
    
    async def _list_datasets(self, include_stats: bool = False) -> Dict[str, Any]:
        """List all datasets with vendor information"""
        try:
            # Get all tables in the database
            tables_query = """
            SELECT 
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'main')
            ORDER BY table_schema, table_name
            """
            
            tables = self.db.execute_query(tables_query)
            
            datasets = {
                "vendor": "LSEG/TRTH",
                "database": "multi_exchange_data_lake.duckdb",
                "schemas": {}
            }
            
            for _, row in tables.iterrows():
                schema = row['table_schema']
                table = row['table_name']
                
                if schema not in datasets["schemas"]:
                    datasets["schemas"][schema] = {
                        "description": self._get_schema_description(schema),
                        "tables": []
                    }
                
                table_info = {
                    "name": table,
                    "full_name": f"{schema}.{table}",
                    "type": row['table_type']
                }
                
                if include_stats:
                    # Get record count and date range
                    try:
                        if 'data_date' in self.db.get_table_columns(f"{schema}.{table}"):
                            stats_query = f"""
                            SELECT 
                                COUNT(*) as record_count,
                                MIN(data_date) as earliest_date,
                                MAX(data_date) as latest_date
                            FROM {schema}.{table}
                            """
                            stats = self.db.execute_query(stats_query)
                            if not stats.empty:
                                table_info["stats"] = {
                                    "record_count": int(stats.iloc[0]['record_count']),
                                    "earliest_date": str(stats.iloc[0]['earliest_date']),
                                    "latest_date": str(stats.iloc[0]['latest_date'])
                                }
                    except Exception as e:
                        logger.warning(f"Could not get stats for {schema}.{table}: {e}")
                
                datasets["schemas"][schema]["tables"].append(table_info)
            
            return datasets
            
        except Exception as e:
            logger.error(f"Error listing datasets: {e}")
            raise
    
    async def _get_dataset_exchanges(self, dataset: str) -> Dict[str, Any]:
        """Get all exchanges for a specific dataset"""
        try:
            # Determine the appropriate table based on dataset name
            table_name = self._resolve_table_name(dataset)
            
            # Check if table exists first
            if not self.db.table_exists(table_name):
                return {
                    "dataset": dataset,
                    "table": table_name,
                    "error": f"Table {table_name} does not exist",
                    "exchanges": [],
                    "note": "This table has not been created yet"
                }
            
            # Check if the table has an exchange column
            columns = self.db.get_table_columns(table_name)
            if 'exchange' not in columns:
                return {
                    "dataset": dataset,
                    "table": table_name,
                    "exchanges": [],
                    "note": "This table does not have exchange information"
                }
            
            query = f"""
            SELECT 
                exchange,
                COUNT(*) as record_count,
                MIN(data_date) as earliest_date,
                MAX(data_date) as latest_date,
                COUNT(DISTINCT "#RIC") as unique_symbols
            FROM {table_name}
            GROUP BY exchange
            ORDER BY exchange
            """
            
            result = self.db.execute_query(query)
            
            exchanges = []
            for _, row in result.iterrows():
                exchanges.append({
                    "exchange": row['exchange'],
                    "record_count": int(row['record_count']),
                    "earliest_date": str(row['earliest_date']),
                    "latest_date": str(row['latest_date']),
                    "unique_symbols": int(row['unique_symbols'])
                })
            
            return {
                "dataset": dataset,
                "table": table_name,
                "exchanges": exchanges
            }
            
        except Exception as e:
            logger.error(f"Error getting dataset exchanges: {e}")
            raise
    
    async def _get_data_for_time_range(
        self, 
        dataset: str, 
        start_date: str, 
        end_date: str, 
        exchange: Optional[str] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """Get data for a specific time range"""
        try:
            table_name = self._resolve_table_name(dataset)
            
            # Build the query
            query = f"""
            SELECT *
            FROM {table_name}
            WHERE data_date BETWEEN '{start_date}' AND '{end_date}'
            """
            
            if exchange:
                query += f" AND exchange = '{exchange}'"
            
            query += f" ORDER BY data_date, \"Date-Time\" LIMIT {limit}"
            
            result = self.db.execute_query(query)
            
            return {
                "dataset": dataset,
                "table": table_name,
                "start_date": start_date,
                "end_date": end_date,
                "exchange": exchange,
                "record_count": len(result),
                "data": result.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error getting data for time range: {e}")
            raise
    
    async def _query_data(self, query: str, limit: int = 1000) -> Dict[str, Any]:
        """Execute a SQL query"""
        try:
            # Add limit if not already present and it's a SELECT query
            if query.strip().upper().startswith('SELECT') and 'LIMIT' not in query.upper():
                query += f" LIMIT {limit}"
            
            result = self.db.execute_query(query)
            
            return {
                "query": query,
                "record_count": len(result),
                "columns": list(result.columns),
                "data": result.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    async def _get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
        try:
            # Get column information
            schema_query = f"DESCRIBE {table_name}"
            schema_result = self.db.execute_query(schema_query)
            
            # Get sample data
            sample_query = f"SELECT * FROM {table_name} LIMIT 5"
            sample_result = self.db.execute_query(sample_query)
            
            return {
                "table_name": table_name,
                "columns": schema_result.to_dict('records'),
                "sample_data": sample_result.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error getting table schema: {e}")
            raise
    
    async def _get_available_symbols(
        self, 
        exchange: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get available symbols for an exchange"""
        try:
            # Find the appropriate table for the exchange
            table_mapping = {
                'LSE': 'bronze.lse_market_data',
                'CME': 'bronze.cme_market_data',
                'NYQ': 'bronze.nyq_market_data'
            }
            
            table_name = table_mapping.get(exchange.upper())
            if not table_name:
                return {
                    "exchange": exchange,
                    "error": f"No table found for exchange {exchange}",
                    "available_exchanges": list(table_mapping.keys())
                }
            
            # Check column types to handle data type differences
            columns = self.db.get_table_columns(table_name)
            
            # Build query with appropriate type casting
            volume_expr = "AVG(Volume)" if columns.get('Volume') in ['BIGINT', 'INTEGER', 'DOUBLE'] else "COUNT(*)"
            volume_alias = "avg_volume" if columns.get('Volume') in ['BIGINT', 'INTEGER', 'DOUBLE'] else "volume_records"
            
            query = f"""
            SELECT 
                "#RIC" as symbol,
                COUNT(*) as trade_count,
                MIN(data_date) as first_seen,
                MAX(data_date) as last_seen,
                AVG(Price) as avg_price,
                {volume_expr} as {volume_alias}
            FROM {table_name}
            WHERE 1=1
            """
            
            if start_date:
                query += f" AND data_date >= '{start_date}'"
            if end_date:
                query += f" AND data_date <= '{end_date}'"
            
            query += """
            GROUP BY "#RIC"
            ORDER BY trade_count DESC, "#RIC"
            """
            
            result = self.db.execute_query(query)
            
            return {
                "exchange": exchange,
                "table": table_name,
                "start_date": start_date,
                "end_date": end_date,
                "symbol_count": len(result),
                "symbols": result.to_dict('records'),
                "note": f"Volume data type: {columns.get('Volume', 'unknown')}"
            }
            
        except Exception as e:
            logger.error(f"Error getting available symbols: {e}")
            raise
    
    async def _get_most_active_symbols(
        self, 
        date: str, 
        exchange: str, 
        metric: str = "trade_count",
        limit: int = 10
    ) -> Dict[str, Any]:
        """Get the most active symbols for a specific date"""
        try:
            # Find the appropriate table for the exchange
            table_mapping = {
                'LSE': 'bronze.lse_market_data',
                'CME': 'bronze.cme_market_data', 
                'NYQ': 'bronze.nyq_market_data'
            }
            
            table_name = table_mapping.get(exchange.upper())
            if not table_name:
                return {
                    "date": date,
                    "exchange": exchange,
                    "error": f"No table found for exchange {exchange}",
                    "available_exchanges": list(table_mapping.keys())
                }
            
            # Check if table exists
            if not self.db.table_exists(table_name):
                return {
                    "date": date,
                    "exchange": exchange,
                    "error": f"Table {table_name} does not exist",
                    "symbols": []
                }
            
            # Check column types to handle data type differences
            columns = self.db.get_table_columns(table_name)
            
            # Build query based on metric type
            if metric == "volume":
                # Use volume if available and numeric
                if columns.get('Volume') in ['BIGINT', 'INTEGER', 'DOUBLE']:
                    order_by = "total_volume DESC"
                    select_metric = "SUM(Volume) as total_volume"
                else:
                    # Fallback to trade count if volume is not numeric
                    order_by = "trade_count DESC"
                    select_metric = "COUNT(*) as trade_count"
                    metric = "trade_count"  # Update metric name for response
            else:
                order_by = "trade_count DESC"
                select_metric = "COUNT(*) as trade_count"
            
            query = f"""
            SELECT 
                "#RIC" as symbol,
                {select_metric},
                AVG(Price) as avg_price,
                MIN(Price) as min_price,
                MAX(Price) as max_price,
                COUNT(*) as trade_count
            FROM {table_name}
            WHERE data_date = '{date}'
            GROUP BY "#RIC"
            ORDER BY {order_by}
            LIMIT {limit}
            """
            
            result = self.db.execute_query(query)
            
            return {
                "date": date,
                "exchange": exchange,
                "metric": metric,
                "symbol_count": len(result),
                "symbols": result.to_dict('records'),
                "note": f"Most active symbols by {metric}"
            }
            
        except Exception as e:
            logger.error(f"Error getting most active symbols: {e}")
            raise
    
    async def _get_least_active_symbols(
        self, 
        date: str, 
        exchange: str, 
        metric: str = "trade_count",
        limit: int = 10
    ) -> Dict[str, Any]:
        """Get the least active symbols for a specific date"""
        try:
            # Find the appropriate table for the exchange
            table_mapping = {
                'LSE': 'bronze.lse_market_data',
                'CME': 'bronze.cme_market_data',
                'NYQ': 'bronze.nyq_market_data'
            }
            
            table_name = table_mapping.get(exchange.upper())
            if not table_name:
                return {
                    "date": date,
                    "exchange": exchange,
                    "error": f"No table found for exchange {exchange}",
                    "available_exchanges": list(table_mapping.keys())
                }
            
            # Check if table exists
            if not self.db.table_exists(table_name):
                return {
                    "date": date,
                    "exchange": exchange,
                    "error": f"Table {table_name} does not exist",
                    "symbols": []
                }
            
            # Check column types to handle data type differences
            columns = self.db.get_table_columns(table_name)
            
            # Build query based on metric type
            if metric == "volume":
                # Use volume if available and numeric
                if columns.get('Volume') in ['BIGINT', 'INTEGER', 'DOUBLE']:
                    order_by = "total_volume ASC"
                    select_metric = "SUM(Volume) as total_volume"
                else:
                    # Fallback to trade count if volume is not numeric
                    order_by = "trade_count ASC"
                    select_metric = "COUNT(*) as trade_count"
                    metric = "trade_count"  # Update metric name for response
            else:
                order_by = "trade_count ASC"
                select_metric = "COUNT(*) as trade_count"
            
            query = f"""
            SELECT 
                "#RIC" as symbol,
                {select_metric},
                AVG(Price) as avg_price,
                MIN(Price) as min_price,
                MAX(Price) as max_price,
                COUNT(*) as trade_count
            FROM {table_name}
            WHERE data_date = '{date}'
            GROUP BY "#RIC"
            ORDER BY {order_by}
            LIMIT {limit}
            """
            
            result = self.db.execute_query(query)
            
            return {
                "date": date,
                "exchange": exchange,
                "metric": metric,
                "symbol_count": len(result),
                "symbols": result.to_dict('records'),
                "note": f"Least active symbols by {metric}"
            }
            
        except Exception as e:
            logger.error(f"Error getting least active symbols: {e}")
            raise
    
    async def _create_activity_plot(
        self, 
        date: str, 
        exchange: str, 
        plot_type: str,
        metric: str = "trade_count",
        limit: int = 10
    ) -> Dict[str, Any]:
        """Create a bar chart plot showing symbol activity"""
        try:
            # Set matplotlib to use a non-interactive backend
            plt.switch_backend('Agg')
            
            # Configure seaborn style
            sns.set_style("whitegrid")
            
            # Get data based on plot type
            if plot_type == "most_active":
                data_result = await self._get_most_active_symbols(date, exchange, metric, limit)
                title_prefix = "Most Active"
                color = "steelblue"
            elif plot_type == "least_active":
                data_result = await self._get_least_active_symbols(date, exchange, metric, limit)
                title_prefix = "Least Active"
                color = "coral"
            elif plot_type == "both":
                # Get both most and least active
                most_active = await self._get_most_active_symbols(date, exchange, metric, limit//2)
                least_active = await self._get_least_active_symbols(date, exchange, metric, limit//2)
                
                # Create subplot
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
                
                # Plot most active
                if most_active["symbols"]:
                    most_symbols = [item["symbol"] for item in most_active["symbols"]]
                    most_values = [item.get(f"total_{metric}", item.get(metric, 0)) for item in most_active["symbols"]]
                    
                    ax1.bar(range(len(most_symbols)), most_values, color="steelblue")
                    ax1.set_title(f"Most Active Symbols - {exchange} ({date})")
                    ax1.set_xlabel("Symbols")
                    ax1.set_ylabel(metric.replace("_", " ").title())
                    ax1.set_xticks(range(len(most_symbols)))
                    ax1.set_xticklabels(most_symbols, rotation=45, ha='right')
                
                # Plot least active
                if least_active["symbols"]:
                    least_symbols = [item["symbol"] for item in least_active["symbols"]]
                    least_values = [item.get(f"total_{metric}", item.get(metric, 0)) for item in least_active["symbols"]]
                    
                    ax2.bar(range(len(least_symbols)), least_values, color="coral")
                    ax2.set_title(f"Least Active Symbols - {exchange} ({date})")
                    ax2.set_xlabel("Symbols")
                    ax2.set_ylabel(metric.replace("_", " ").title())
                    ax2.set_xticks(range(len(least_symbols)))
                    ax2.set_xticklabels(least_symbols, rotation=45, ha='right')
                
                plt.tight_layout()
                
                # Save plot to base64
                buffer = io.BytesIO()
                plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
                buffer.seek(0)
                plot_base64 = base64.b64encode(buffer.getvalue()).decode()
                plt.close()
                
                return {
                    "date": date,
                    "exchange": exchange,
                    "plot_type": plot_type,
                    "metric": metric,
                    "plot_data": {
                        "most_active": most_active,
                        "least_active": least_active
                    },
                    "plot_image_base64": plot_base64,
                    "plot_format": "png"
                }
            else:
                return {
                    "error": f"Invalid plot_type: {plot_type}. Must be 'most_active', 'least_active', or 'both'"
                }
            
            # Single plot handling (most_active or least_active)
            if "error" in data_result:
                return data_result
            
            if not data_result["symbols"]:
                return {
                    "date": date,
                    "exchange": exchange,
                    "error": f"No data found for {exchange} on {date}",
                    "plot_image_base64": None
                }
            
            # Extract data for plotting
            symbols = [item["symbol"] for item in data_result["symbols"]]
            values = [item.get(f"total_{metric}", item.get(metric, 0)) for item in data_result["symbols"]]
            
            # Create the plot
            fig, ax = plt.subplots(figsize=(12, 6))
            bars = ax.bar(range(len(symbols)), values, color=color)
            
            # Customize the plot
            ax.set_title(f"{title_prefix} Symbols - {exchange} ({date})")
            ax.set_xlabel("Symbols")
            ax.set_ylabel(metric.replace("_", " ").title())
            ax.set_xticks(range(len(symbols)))
            ax.set_xticklabels(symbols, rotation=45, ha='right')
            
            # Add value labels on bars
            for i, (bar, value) in enumerate(zip(bars, values)):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{value:,}' if isinstance(value, (int, float)) else str(value),
                       ha='center', va='bottom', fontsize=8)
            
            plt.tight_layout()
            
            # Save plot to base64
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
            buffer.seek(0)
            plot_base64 = base64.b64encode(buffer.getvalue()).decode()
            plt.close()
            
            return {
                "date": date,
                "exchange": exchange,
                "plot_type": plot_type,
                "metric": metric,
                "symbol_count": len(symbols),
                "plot_data": data_result,
                "plot_image_base64": plot_base64,
                "plot_format": "png",
                "note": f"Plot showing {title_prefix.lower()} symbols by {metric}"
            }
            
        except Exception as e:
            logger.error(f"Error creating activity plot: {e}")
            raise

    def _resolve_table_name(self, dataset: str) -> str:
        """Resolve dataset name to actual table name"""
        # Handle schema.table format
        if '.' in dataset:
            return dataset
        
        # Map common dataset names to tables
        mapping = {
            'lse': 'bronze.lse_market_data',
            'cme': 'bronze.cme_market_data',
            'nyq': 'bronze.nyq_market_data',
            'unified': 'silver.market_data_unified',
            'market_data': 'silver.market_data_unified',
            'timeseries': 'silver.price_timeseries',
            'daily_summary': 'gold.daily_market_summary',
            'arbitrage': 'gold.arbitrage_opportunities'
        }
        
        return mapping.get(dataset.lower(), dataset)
    
    def _get_schema_description(self, schema: str) -> str:
        """Get description for schema"""
        descriptions = {
            'bronze': 'Raw ingested data from exchanges',
            'silver': 'Cleaned and normalized data',
            'gold': 'Aggregated and business-ready data',
            'staging': 'Temporary staging tables',
            'audit': 'Audit and monitoring tables',
            'views': 'Database views'
        }
        return descriptions.get(schema, 'Data schema')

async def main():
    """Main entry point"""
    try:
        # Initialize the server
        server = ForestratMCPServer()
        
        # Run the server with stdio
        from mcp.server.stdio import stdio_server
        
        async with stdio_server() as (read_stream, write_stream):
            await server.server.run(
                read_stream, 
                write_stream, 
                InitializationOptions(
                    server_name="forestrat-mcp",
                    server_version="1.0.0",
                    capabilities={
                        "tools": {}
                    }
                )
            )
            
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 