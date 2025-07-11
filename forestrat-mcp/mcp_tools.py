#!/usr/bin/env python3
"""
Forestrat MCP Tools Library

Contains all the tool call methods and business logic for the Forestrat MCP Server.
This separates the MCP protocol handling from the actual data processing logic.
"""

import json
import logging
import os
import pandas as pd
from typing import Any, Dict, List, Optional
from datetime import datetime

from database import DuckDBConnection
from config import Config

logger = logging.getLogger(__name__)

# Load configuration
config = Config()
TABLE_MAPPINGS = config.DATASET_MAPPING

# Predefined symbol categories for efficient queries
SYMBOL_CATEGORIES = {
    "bitcoin_futures": {
        "symbols": ["MBTF25", "BTCF25", "MBTG25", "BTCG25"],
        "description": "Bitcoin futures contracts",
        "exchanges": ["CME"],
        "keywords": ["bitcoin", "btc", "futures"]
    },
    "ethereum_futures": {
        "symbols": ["HTEF25", "ETEF25", "MTEF25", "ETEG25"],
        "description": "Ethereum futures contracts", 
        "exchanges": ["CME"],
        "keywords": ["ethereum", "eth", "futures"]
    },
    "crypto_futures": {
        "symbols": ["MBTF25", "BTCF25", "MBTG25", "BTCG25", "HTEF25", "ETEF25", "MTEF25", "ETEG25"],
        "description": "All cryptocurrency futures contracts",
        "exchanges": ["CME"],
        "keywords": ["crypto", "cryptocurrency", "futures", "digital", "assets"]
    },
    "micro_bitcoin": {
        "symbols": ["MBTF25", "MBTG25"],
        "description": "Micro Bitcoin futures (smaller contract sizes)",
        "exchanges": ["CME"],
        "keywords": ["micro", "bitcoin", "mbt"]
    },
    "standard_bitcoin": {
        "symbols": ["BTCF25", "BTCG25"],
        "description": "Standard Bitcoin futures (full contract sizes)",
        "exchanges": ["CME"],
        "keywords": ["standard", "bitcoin", "btc", "full"]
    },
    "micro_ethereum": {
        "symbols": ["HTEF25", "MTEF25"],
        "description": "Micro Ethereum futures (smaller contract sizes)",
        "exchanges": ["CME"],
        "keywords": ["micro", "ethereum", "hte", "mte"]
    },
    "standard_ethereum": {
        "symbols": ["ETEF25", "ETEG25"],
        "description": "Standard Ethereum futures (full contract sizes)",
        "exchanges": ["CME"],
        "keywords": ["standard", "ethereum", "ete", "full"]
    }
}


class ForestratTools:
    """
    Forestrat MCP Tools Library
    
    Contains all the tool call methods and business logic for data processing.
    """
    
    def __init__(self, db: DuckDBConnection):
        """
        Initialize the tools library with a database connection.
        
        Args:
            db: DuckDBConnection instance for database operations
        """
        self.db = db
        logger.info("ForestratTools initialized")
    
    # === CORE TOOL METHODS ===
    
    async def list_datasets(self, include_stats: bool = False) -> Dict[str, Any]:
        """List all datasets with vendor information"""
        try:
            # Get all tables in the database
            query = """
            SELECT 
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'main')
            ORDER BY table_schema, table_name
            """
            
            result_df = self.db.execute_query(query)
            
            datasets = {
                "vendor": "LSEG/TRTH",
                "database": "multi_exchange_data_lake.duckdb",
                "schemas": {}
            }
            
            # Convert DataFrame to records
            for _, row in result_df.iterrows():
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
                        columns = self.db.get_table_columns(f"{schema}.{table}")
                        if 'data_date' in columns:
                            stats_query = f"""
                            SELECT 
                                COUNT(*) as record_count,
                                MIN(data_date) as earliest_date,
                                MAX(data_date) as latest_date
                            FROM {schema}.{table}
                            """
                            stats_df = self.db.execute_query(stats_query)
                            if not stats_df.empty:
                                stat_row = stats_df.iloc[0]
                                table_info["stats"] = {
                                    "record_count": int(stat_row['record_count']),
                                    "earliest_date": str(stat_row['earliest_date']),
                                    "latest_date": str(stat_row['latest_date'])
                                }
                    except Exception as e:
                        logger.warning(f"Could not get stats for {schema}.{table}: {e}")
                
                datasets["schemas"][schema]["tables"].append(table_info)
            
            return datasets
            
        except Exception as e:
            logger.error(f"Error listing datasets: {e}")
            return {"error": str(e)}
    
    async def get_dataset_exchanges(self, dataset: str) -> Dict[str, Any]:
        """Get all exchanges for a specific dataset"""
        try:
            # Determine the appropriate table based on dataset name
            table_name = self._resolve_table_name(dataset)
            
            if not table_name:
                return {
                    "dataset": dataset,
                    "error": f"Dataset '{dataset}' not found",
                    "available_datasets": list(TABLE_MAPPINGS.keys())
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
            
            result_df = self.db.execute_query(query)
            
            exchanges = []
            for _, row in result_df.iterrows():
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
            return {"error": str(e)}
    
    async def get_data_for_time_range(
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
            
            if not table_name:
                return {
                    "dataset": dataset,
                    "error": f"Dataset '{dataset}' not found",
                    "available_datasets": list(TABLE_MAPPINGS.keys())
                }
            
            # Build the query
            query = f"""
            SELECT *
            FROM {table_name}
            WHERE data_date BETWEEN '{start_date}' AND '{end_date}'
            """
            
            if exchange:
                query += f" AND exchange = '{exchange}'"
            
            query += f" ORDER BY data_date, \"Date-Time\" LIMIT {limit}"
            
            result_df = self.db.execute_query(query)
            
            return {
                "dataset": dataset,
                "table": table_name,
                "start_date": start_date,
                "end_date": end_date,
                "exchange": exchange,
                "record_count": len(result_df),
                "data": result_df.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error getting data for time range: {e}")
            return {"error": str(e)}
    
    async def query_data(self, query: str, limit: int = 1000) -> Dict[str, Any]:
        """Execute a SQL query"""
        try:
            # Add limit if not already present and it's a SELECT query
            if query.strip().upper().startswith('SELECT') and 'LIMIT' not in query.upper():
                query += f" LIMIT {limit}"
            
            result_df = self.db.execute_query(query)
            
            return {
                "query": query,
                "record_count": len(result_df),
                "columns": list(result_df.columns),
                "data": result_df.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return {"error": str(e)}
    
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
        try:
            # Get column information
            schema_query = f"DESCRIBE {table_name}"
            schema_result_df = self.db.execute_query(schema_query)
            
            # Get sample data
            sample_query = f"SELECT * FROM {table_name} LIMIT 5"
            sample_result_df = self.db.execute_query(sample_query)
            
            return {
                "table_name": table_name,
                "columns": schema_result_df.to_dict('records'),
                "sample_data": sample_result_df.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error getting table schema: {e}")
            return {"error": str(e)}
    
    async def get_available_symbols(
        self, 
        exchange: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get available symbols for an exchange"""
        try:
            # Find the appropriate table for the exchange
            table_mapping = {
                'LSE': 'bronze.lse_market_data_raw',
                'CME': 'bronze.cme_market_data_raw',
                'NYQ': 'bronze.nyq_market_data_raw'
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
            
            result_df = self.db.execute_query(query)
            
            return {
                "exchange": exchange,
                "table": table_name,
                "start_date": start_date,
                "end_date": end_date,
                "symbol_count": len(result_df),
                "symbols": result_df.to_dict('records'),
                "note": f"Volume data type: {columns.get('Volume', 'unknown')}"
            }
            
        except Exception as e:
            logger.error(f"Error getting available symbols: {e}")
            return {"error": str(e)}
    
    async def get_most_active_symbols(
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
                'LSE': 'bronze.lse_market_data_raw',
                'CME': 'bronze.cme_market_data_raw', 
                'NYQ': 'bronze.nyq_market_data_raw'
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
            
            result_df = self.db.execute_query(query)
            
            return {
                "date": date,
                "exchange": exchange,
                "metric": metric,
                "symbol_count": len(result_df),
                "symbols": result_df.to_dict('records'),
                "note": f"Most active symbols by {metric}"
            }
            
        except Exception as e:
            logger.error(f"Error getting most active symbols: {e}")
            return {"error": str(e)}
    
    async def get_least_active_symbols(
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
                'LSE': 'bronze.lse_market_data_raw',
                'CME': 'bronze.cme_market_data_raw',
                'NYQ': 'bronze.nyq_market_data_raw'
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
            
            result_df = self.db.execute_query(query)
            
            return {
                "date": date,
                "exchange": exchange,
                "metric": metric,
                "symbol_count": len(result_df),
                "symbols": result_df.to_dict('records'),
                "note": f"Least active symbols by {metric}"
            }
            
        except Exception as e:
            logger.error(f"Error getting least active symbols: {e}")
            return {"error": str(e)}

    async def get_symbols_by_category(
        self, 
        category: str, 
        exchange: Optional[str] = None,
        include_stats: bool = False,
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get predefined symbol lists by category for efficient queries"""
        try:
            if category not in SYMBOL_CATEGORIES:
                return {
                    "error": f"Unknown category: {category}",
                    "available_categories": list(SYMBOL_CATEGORIES.keys())
                }
            
            category_info = SYMBOL_CATEGORIES[category]
            symbols = category_info["symbols"]
            
            # Filter by exchange if specified
            if exchange:
                exchange = exchange.upper()
                if exchange not in category_info["exchanges"]:
                    return {
                        "category": category,
                        "exchange": exchange,
                        "error": f"Category {category} not available on exchange {exchange}",
                        "available_exchanges": category_info["exchanges"],
                        "symbols": []
                    }
            
            result = {
                "category": category,
                "description": category_info["description"],
                "symbols": symbols,
                "symbol_count": len(symbols),
                "exchanges": category_info["exchanges"],
                "keywords": category_info["keywords"]
            }
            
            # Add statistics if requested
            if include_stats and date:
                if not exchange:
                    exchange = category_info["exchanges"][0]  # Use first available exchange
                
                table_mapping = {
                    'LSE': 'bronze.lse_market_data_raw',
                    'CME': 'bronze.cme_market_data_raw',
                    'NYQ': 'bronze.nyq_market_data_raw'
                }
                
                table_name = table_mapping.get(exchange)
                if table_name and self.db.table_exists(table_name):
                    # Build IN clause for symbols
                    symbol_list = "', '".join(symbols)
                    
                    stats_query = f"""
                    SELECT 
                        "#RIC" as symbol,
                        COUNT(*) as trade_count,
                        SUM(CASE WHEN Volume ~ '^[0-9]+$' THEN CAST(Volume AS INTEGER) ELSE 0 END) as total_volume,
                        AVG(Price) as avg_price,
                        MIN(Price) as min_price,
                        MAX(Price) as max_price,
                        STDDEV(Price) as price_volatility
                    FROM {table_name}
                    WHERE data_date = '{date}'
                    AND "#RIC" IN ('{symbol_list}')
                    GROUP BY "#RIC"
                    ORDER BY total_volume DESC
                    """
                    
                    try:
                        stats_df = self.db.execute_query(stats_query)
                        result["statistics"] = {
                            "date": date,
                            "exchange": exchange,
                            "symbol_stats": stats_df.to_dict('records')
                        }
                    except Exception as e:
                        result["statistics_error"] = str(e)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting symbols by category: {e}")
            return {"error": str(e)}

    async def get_category_volume_data(
        self, 
        category: str, 
        date: str, 
        exchange: str,
        metric: str = "both"
    ) -> Dict[str, Any]:
        """Get volume and trading data for a specific symbol category (optimized query)"""
        try:
            if category not in SYMBOL_CATEGORIES:
                return {
                    "error": f"Unknown category: {category}",
                    "available_categories": list(SYMBOL_CATEGORIES.keys())
                }
            
            category_info = SYMBOL_CATEGORIES[category]
            symbols = category_info["symbols"]
            
            # Check if exchange is supported for this category
            exchange = exchange.upper()
            if exchange not in category_info["exchanges"]:
                return {
                    "category": category,
                    "date": date,
                    "exchange": exchange,
                    "error": f"Category {category} not available on exchange {exchange}",
                    "available_exchanges": category_info["exchanges"]
                }
            
            table_mapping = {
                'LSE': 'bronze.lse_market_data_raw',
                'CME': 'bronze.cme_market_data_raw',
                'NYQ': 'bronze.nyq_market_data_raw'
            }
            
            table_name = table_mapping.get(exchange)
            if not table_name:
                return {
                    "error": f"No table found for exchange {exchange}",
                    "available_exchanges": list(table_mapping.keys())
                }
            
            if not self.db.table_exists(table_name):
                return {
                    "error": f"Table {table_name} does not exist"
                }
            
            # Build optimized query using IN clause instead of LIKE
            symbol_list = "', '".join(symbols)
            
            # Check column types to handle data type differences
            columns = self.db.get_table_columns(table_name)
            
            # Build appropriate volume expression
            if columns.get('Volume') in ['BIGINT', 'INTEGER', 'DOUBLE']:
                volume_expr = "SUM(Volume) as total_volume"
                volume_available = True
            else:
                volume_expr = "0 as total_volume"
                volume_available = False
            
            if metric == "volume" and not volume_available:
                metric = "trade_count"  # Fallback if volume not available
            
            query = f"""
            SELECT 
                "#RIC" as symbol,
                COUNT(*) as trade_count,
                {volume_expr},
                AVG(Price) as avg_price,
                MIN(Price) as min_price,
                MAX(Price) as max_price,
                STDDEV(Price) as price_volatility,
                MIN("Date-Time") as first_trade,
                MAX("Date-Time") as last_trade
            FROM {table_name}
            WHERE data_date = '{date}'
            AND "#RIC" IN ('{symbol_list}')
            GROUP BY "#RIC"
            ORDER BY 
                CASE 
                    WHEN '{metric}' = 'volume' THEN total_volume
                    ELSE trade_count
                END DESC
            """
            
            result_df = self.db.execute_query(query)
            
            # Calculate category totals
            if not result_df.empty:
                total_trades = result_df['trade_count'].sum()
                total_volume = result_df['total_volume'].sum() if volume_available else 0
                avg_price_weighted = (result_df['avg_price'] * result_df['trade_count']).sum() / total_trades if total_trades > 0 else 0
                symbols_active = len(result_df)
            else:
                total_trades = total_volume = avg_price_weighted = symbols_active = 0
            
            return {
                "category": category,
                "description": category_info["description"],
                "date": date,
                "exchange": exchange,
                "metric": metric,
                "query_optimization": f"Used IN clause with {len(symbols)} predefined symbols (no LIKE scan)",
                "category_totals": {
                    "symbols_in_category": len(symbols),
                    "symbols_active": symbols_active,
                    "total_trades": int(total_trades),
                    "total_volume": int(total_volume) if volume_available else "N/A",
                    "weighted_avg_price": round(avg_price_weighted, 4) if avg_price_weighted else 0
                },
                "symbol_data": result_df.to_dict('records'),
                "volume_data_available": volume_available,
                "note": f"Efficient query using predefined symbol list for {category}"
            }
            
        except Exception as e:
            logger.error(f"Error getting category volume data: {e}")
            return {"error": str(e)}

    async def export_category_data(
        self, 
        category: str, 
        exchange: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None, 
        output_filename: Optional[str] = None,
        format: Optional[str] = "csv"
    ) -> Dict[str, Any]:
        """Export all data for a specific futures category to a file"""
        try:
            if category not in SYMBOL_CATEGORIES:
                return {
                    "error": f"Unknown category: {category}",
                    "available_categories": list(SYMBOL_CATEGORIES.keys())
                }
            
            category_info = SYMBOL_CATEGORIES[category]
            symbols = category_info["symbols"]
            
            # Check if exchange is supported for this category
            exchange = exchange.upper()
            if exchange not in category_info["exchanges"]:
                return {
                    "category": category,
                    "exchange": exchange,
                    "error": f"Category {category} not available on exchange {exchange}",
                    "available_exchanges": category_info["exchanges"]
                }
            
            table_mapping = {
                'LSE': 'bronze.lse_market_data_raw',
                'CME': 'bronze.cme_market_data_raw',
                'NYQ': 'bronze.nyq_market_data_raw'
            }
            
            table_name = table_mapping.get(exchange)
            if not table_name:
                return {
                    "error": f"No table found for exchange {exchange}",
                    "available_exchanges": list(table_mapping.keys())
                }
            
            if not self.db.table_exists(table_name):
                return {
                    "error": f"Table {table_name} does not exist"
                }
            
            # Build the query to get ALL raw data for the category symbols
            symbol_list = "', '".join(symbols)
            
            # Build WHERE clause
            where_clauses = [f"\"#RIC\" IN ('{symbol_list}')"]
            
            if start_date:
                where_clauses.append(f"data_date >= '{start_date}'")
            if end_date:
                where_clauses.append(f"data_date <= '{end_date}'")
            
            where_clause = " AND ".join(where_clauses)
            
            # Query to get ALL raw data (not aggregated)
            query = f"""
            SELECT 
                data_date,
                "#RIC" as symbol,
                "Date-Time" as datetime,
                "Type",
                Price,
                Volume,
                "Exch Time" as exchange_time,
                "Qualifiers"
            FROM {table_name}
            WHERE {where_clause}
            ORDER BY data_date, "#RIC", "Date-Time"
            """
            
            logger.info(f"Executing export query for {category} on {exchange}")
            result_df = self.db.execute_query(query)
            
            if result_df.empty:
                return {
                    "category": category,
                    "exchange": exchange,
                    "start_date": start_date,
                    "end_date": end_date,
                    "error": "No data found for the specified criteria",
                    "symbols_searched": symbols
                }
            
            # Generate filename if not provided
            if not output_filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                date_suffix = ""
                if start_date and end_date:
                    date_suffix = f"_{start_date}_to_{end_date}"
                elif start_date:
                    date_suffix = f"_from_{start_date}"
                elif end_date:
                    date_suffix = f"_until_{end_date}"
                
                output_filename = f"{category}_{exchange.lower()}{date_suffix}_{timestamp}.{format if format else 'csv'}"
            
            # Ensure the filename has the correct extension
            if not output_filename.endswith(f".{format if format else 'csv'}"):
                output_filename += f".{format if format else 'csv'}"
            
            # Create exports directory if it doesn't exist
            export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports")
            os.makedirs(export_dir, exist_ok=True)
            
            # Full path for the output file
            output_path = os.path.join(export_dir, output_filename)
            
            # Export the data
            if format == "json":
                result_df.to_json(output_path, orient='records', date_format='iso', indent=2)
            else:  # Default to CSV
                result_df.to_csv(output_path, index=False)
            
            # Calculate summary statistics
            summary_stats = {
                "total_records": len(result_df),
                "unique_symbols": result_df['symbol'].nunique(),
                "symbols_found": sorted(result_df['symbol'].unique().tolist()),
                "date_range": {
                    "earliest": str(result_df['data_date'].min()),
                    "latest": str(result_df['data_date'].max())
                },
                "unique_dates": result_df['data_date'].nunique(),
                "total_volume": result_df['Volume'].sum() if result_df['Volume'].dtype in ['int64', 'float64'] else "N/A",
                "avg_price": result_df['Price'].mean() if result_df['Price'].dtype in ['int64', 'float64'] else "N/A"
            }
            
            return {
                "category": category,
                "description": category_info["description"],
                "exchange": exchange,
                "export_details": {
                    "output_file": output_path,
                    "filename": output_filename,
                    "format": format if format else "csv",
                    "file_size_mb": round(os.path.getsize(output_path) / (1024 * 1024), 2)
                },
                "query_info": {
                    "symbols_requested": symbols,
                    "start_date": start_date,
                    "end_date": end_date,
                    "table_queried": table_name
                },
                "summary_statistics": summary_stats,
                "export_timestamp": datetime.now().isoformat(),
                "note": f"Successfully exported {len(result_df)} records for {category} category"
            }
            
        except Exception as e:
            logger.error(f"Error exporting category data: {e}")
            import traceback
            logger.error(f"Export error traceback: {traceback.format_exc()}")
            return {"error": str(e)}

    # === PROMPT EXECUTION METHODS ===
    
    async def execute_daily_market_summary(self, arguments: Dict[str, Any]) -> str:
        """Execute daily market summary prompt"""
        date = arguments.get("date")
        exchange = arguments.get("exchange")
        top_n = arguments.get("top_n", 10)
        
        if not date or not exchange:
            return "Error: Missing required arguments: date, exchange"
        
        summary_parts = []
        summary_parts.append(f"# Daily Market Summary for {exchange} on {date}")
        summary_parts.append(f"Analysis of top {top_n} most and least active symbols\n")
        
        # Get most active symbols by volume
        most_active_vol = await self.get_most_active_symbols(date, exchange, "volume", top_n)
        if "symbols" in most_active_vol:
            summary_parts.append("## Most Active Symbols by Volume")
            for i, symbol in enumerate(most_active_vol["symbols"][:top_n], 1):
                vol_text = f"{symbol.get('total_volume', 0):,}" if 'total_volume' in symbol else f"{symbol.get('trade_count', 0):,} trades"
                summary_parts.append(f"{i}. {symbol['symbol']} - {vol_text}")
        
        # Get least active symbols
        least_active = await self.get_least_active_symbols(date, exchange, "volume", top_n)
        if "symbols" in least_active:
            summary_parts.append("\n## Least Active Symbols by Volume")
            for i, symbol in enumerate(least_active["symbols"][:top_n], 1):
                vol_text = f"{symbol.get('total_volume', 0):,}" if 'total_volume' in symbol else f"{symbol.get('trade_count', 0):,} trades"
                summary_parts.append(f"{i}. {symbol['symbol']} - {vol_text}")
        
        return "\n".join(summary_parts)
    
    async def execute_cross_exchange_analysis(self, arguments: Dict[str, Any]) -> str:
        """Execute cross-exchange symbol analysis prompt"""
        symbol = arguments.get("symbol")
        date = arguments.get("date")
        exchanges = arguments.get("exchanges", "LSE,CME,NYQ").split(",")
        
        if not symbol or not date:
            return "Error: Missing required arguments: symbol, date"
        
        analysis_parts = []
        analysis_parts.append(f"# Cross-Exchange Analysis for {symbol} on {date}")
        analysis_parts.append(f"Comparing activity across exchanges: {', '.join(exchanges)}\n")
        
        table_mapping = {
            'LSE': 'bronze.lse_market_data_raw',
            'CME': 'bronze.cme_market_data_raw',
            'NYQ': 'bronze.nyq_market_data_raw'
        }
        
        for exchange in exchanges:
            exchange = exchange.strip().upper()
            table_name = table_mapping.get(exchange)
            
            analysis_parts.append(f"\n## {exchange} Exchange")
            
            if not table_name:
                analysis_parts.append(f"- No table mapping found for {exchange}")
                continue
                
            try:
                query = f"""
                SELECT 
                    "#RIC" as symbol,
                    COUNT(*) as trade_count,
                    SUM(Volume) as total_volume,
                    AVG(Price) as avg_price,
                    MIN(Price) as min_price,
                    MAX(Price) as max_price
                FROM {table_name}
                WHERE data_date = '{date}' 
                AND "#RIC" ILIKE '%{symbol}%'
                GROUP BY "#RIC"
                ORDER BY total_volume DESC
                LIMIT 5
                """
                
                result_df = self.db.execute_query(query)
                
                if not result_df.empty:
                    for _, row in result_df.iterrows():
                        analysis_parts.append(f"- Symbol: {row['symbol']}")
                        analysis_parts.append(f"  - Trades: {row['trade_count']:,}")
                        if row['total_volume']:
                            analysis_parts.append(f"  - Volume: {row['total_volume']:,}")
                        if row['avg_price']:
                            analysis_parts.append(f"  - Avg Price: ${row['avg_price']:.4f}")
                            analysis_parts.append(f"  - Price Range: ${row['min_price']:.4f} - ${row['max_price']:.4f}")
                else:
                    analysis_parts.append(f"- No data found for {symbol} on {exchange}")
                    
            except Exception as e:
                analysis_parts.append(f"- Error querying {exchange}: {str(e)}")
        
        return "\n".join(analysis_parts)
    
    async def execute_anomaly_detection(self, arguments: Dict[str, Any]) -> str:
        """Execute trading anomaly detection prompt"""
        date = arguments.get("date")
        exchange = arguments.get("exchange")
        threshold = float(arguments.get("threshold", 3))
        
        if not date or not exchange:
            return "Error: Missing required arguments: date, exchange"
        
        table_mapping = {
            'LSE': 'bronze.lse_market_data_raw',
            'CME': 'bronze.cme_market_data_raw',
            'NYQ': 'bronze.nyq_market_data_raw'
        }
        
        table_name = table_mapping.get(exchange.upper())
        if not table_name:
            return f"Error: No table mapping found for exchange: {exchange}"
        
        try:
            # Query to find volume anomalies
            anomaly_query = f"""
            WITH daily_stats AS (
                SELECT 
                    "#RIC" as symbol,
                    SUM(Volume) as total_volume,
                    COUNT(*) as trade_count,
                    AVG(Price) as avg_price,
                    STDDEV(Price) as price_stddev
                FROM {table_name}
                WHERE data_date = '{date}'
                GROUP BY "#RIC"
            ),
            volume_stats AS (
                SELECT 
                    AVG(total_volume) as avg_volume,
                    STDDEV(total_volume) as volume_stddev
                FROM daily_stats
                WHERE total_volume > 0
            )
            SELECT 
                ds.symbol,
                ds.total_volume,
                ds.trade_count,
                ds.avg_price,
                ds.price_stddev,
                vs.avg_volume,
                vs.volume_stddev,
                ABS(ds.total_volume - vs.avg_volume) / NULLIF(vs.volume_stddev, 0) as volume_z_score
            FROM daily_stats ds, volume_stats vs
            WHERE vs.volume_stddev > 0 
            AND ABS(ds.total_volume - vs.avg_volume) / vs.volume_stddev > {threshold}
            ORDER BY volume_z_score DESC
            LIMIT 20
            """
            
            result_df = self.db.execute_query(anomaly_query)
            
            anomaly_parts = []
            anomaly_parts.append(f"# Trading Anomaly Detection for {exchange} on {date}")
            anomaly_parts.append(f"Threshold: {threshold} standard deviations\n")
            
            if not result_df.empty:
                anomaly_parts.append("## Volume Anomalies Detected")
                for _, row in result_df.iterrows():
                    anomaly_parts.append(f"- **{row['symbol']}**")
                    anomaly_parts.append(f"  - Volume: {row['total_volume']:,} (Z-score: {row['volume_z_score']:.2f})")
                    anomaly_parts.append(f"  - Trades: {row['trade_count']:,}")
                    if row['avg_price']:
                        anomaly_parts.append(f"  - Avg Price: ${row['avg_price']:.4f}")
                    if row['price_stddev'] and row['price_stddev'] > 0:
                        anomaly_parts.append(f"  - Price Volatility: ${row['price_stddev']:.4f}")
            else:
                anomaly_parts.append("No significant volume anomalies detected for the specified threshold.")
                
        except Exception as e:
            return f"# Error in Anomaly Detection\nError: {str(e)}"
        
        return "\n".join(anomaly_parts)
    
    async def execute_volume_trend_analysis(self, arguments: Dict[str, Any]) -> str:
        """Execute volume trend analysis prompt"""
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        exchange = arguments.get("exchange")
        symbols = arguments.get("symbols", "").split(",") if arguments.get("symbols") else None
        
        if not start_date or not end_date or not exchange:
            return "Error: Missing required arguments: start_date, end_date, exchange"
        
        table_mapping = {
            'LSE': 'bronze.lse_market_data_raw',
            'CME': 'bronze.cme_market_data_raw',
            'NYQ': 'bronze.nyq_market_data_raw'
        }
        
        table_name = table_mapping.get(exchange.upper())
        if not table_name:
            return f"Error: No table mapping found for exchange: {exchange}"
        
        try:
            # Build the query
            where_clause = f"WHERE data_date BETWEEN '{start_date}' AND '{end_date}'"
            if symbols and symbols[0].strip():
                symbol_list = "', '".join([s.strip() for s in symbols if s.strip()])
                where_clause += f" AND \"#RIC\" IN ('{symbol_list}')"
            
            trend_query = f"""
            SELECT 
                data_date,
                "#RIC" as symbol,
                SUM(Volume) as daily_volume,
                COUNT(*) as daily_trades,
                AVG(Price) as avg_price
            FROM {table_name}
            {where_clause}
            GROUP BY data_date, "#RIC"
            ORDER BY data_date, daily_volume DESC
            """
            
            result_df = self.db.execute_query(trend_query)
            
            trend_parts = []
            trend_parts.append(f"# Volume Trend Analysis for {exchange}")
            trend_parts.append(f"Period: {start_date} to {end_date}")
            if symbols and symbols[0].strip():
                trend_parts.append(f"Symbols: {', '.join([s.strip() for s in symbols if s.strip()])}")
            trend_parts.append("")
            
            if not result_df.empty:
                # Group by date and show top symbols each day
                for date in sorted(result_df['data_date'].unique()):
                    date_data = result_df[result_df['data_date'] == date].head(5)
                    trend_parts.append(f"## {date}")
                    
                    for _, row in date_data.iterrows():
                        vol_text = f"Volume {row['daily_volume']:,}" if row['daily_volume'] else f"Trades {row['daily_trades']:,}"
                        price_text = f", Avg Price ${row['avg_price']:.4f}" if row['avg_price'] else ""
                        trend_parts.append(f"- {row['symbol']}: {vol_text}{price_text}")
                    trend_parts.append("")
                
                # Summary statistics
                trend_parts.append("## Summary Statistics")
                total_volume = result_df['daily_volume'].sum() if result_df['daily_volume'].notna().any() else 0
                total_trades = result_df['daily_trades'].sum()
                
                trend_parts.append(f"- Total Volume: {total_volume:,}")
                trend_parts.append(f"- Total Trades: {total_trades:,}")
                if not result_df.empty:
                    avg_daily_volume = result_df.groupby('data_date')['daily_volume'].sum().mean()
                    trend_parts.append(f"- Average Daily Volume: {avg_daily_volume:,.0f}")
            else:
                trend_parts.append("No data found for the specified criteria.")
                
        except Exception as e:
            return f"# Error in Volume Trend Analysis\nError: {str(e)}"
        
        return "\n".join(trend_parts)
    
    # === RESOURCE READING METHODS ===
    
    async def read_schema_resource(self, layer: str) -> Dict[str, Any]:
        """Read schema information for a specific layer"""
        try:
            # Get all tables for this layer
            schema_query = f"""
            SELECT 
                table_name,
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns 
            WHERE table_schema = '{layer}'
            ORDER BY table_name, ordinal_position
            """
            
            result_df = self.db.execute_query(schema_query)
            
            if result_df.empty:
                return {"error": f"No tables found in {layer} layer"}
            
            tables = {}
            for _, row in result_df.iterrows():
                table_name = row['table_name']
                if table_name not in tables:
                    tables[table_name] = []
                
                tables[table_name].append({
                    "column": row['column_name'],
                    "type": row['data_type'],
                    "nullable": row['is_nullable'] == 'YES'
                })
            
            return {
                "layer": layer,
                "description": self._get_schema_description(layer),
                "tables": tables,
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error reading schema: {e}")
            return {"error": str(e)}
    
    async def read_calendar_resource(self, exchange: str) -> Dict[str, Any]:
        """Read trading calendar for an exchange"""
        try:
            table_mapping = {
                'LSE': 'bronze.lse_market_data_raw',
                'CME': 'bronze.cme_market_data_raw',
                'NYQ': 'bronze.nyq_market_data_raw'
            }
            
            table_name = table_mapping.get(exchange.upper())
            if not table_name:
                return {"error": f"No table found for exchange: {exchange}"}
            
            calendar_query = f"""
            SELECT DISTINCT 
                data_date,
                COUNT(*) as trade_count,
                COUNT(DISTINCT "#RIC") as unique_symbols
            FROM {table_name}
            GROUP BY data_date
            ORDER BY data_date
            """
            
            result_df = self.db.execute_query(calendar_query)
            
            trading_days = []
            for _, row in result_df.iterrows():
                trading_days.append({
                    "date": str(row['data_date']),
                    "trade_count": int(row['trade_count']),
                    "unique_symbols": int(row['unique_symbols'])
                })
            
            return {
                "exchange": exchange,
                "trading_days": trading_days,
                "total_days": len(trading_days),
                "date_range": {
                    "earliest": str(result_df['data_date'].min()) if not result_df.empty else None,
                    "latest": str(result_df['data_date'].max()) if not result_df.empty else None
                },
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error reading calendar: {e}")
            return {"error": str(e)}
    
    async def read_symbol_mapping_resource(self, exchange: str) -> Dict[str, Any]:
        """Read symbol directory for an exchange"""
        try:
            table_mapping = {
                'LSE': 'bronze.lse_market_data_raw',
                'CME': 'bronze.cme_market_data_raw',
                'NYQ': 'bronze.nyq_market_data_raw'
            }
            
            table_name = table_mapping.get(exchange.upper())
            if not table_name:
                return {"error": f"No table found for exchange: {exchange}"}
            
            symbols_query = f"""
            SELECT 
                "#RIC" as symbol,
                COUNT(*) as total_trades,
                SUM(Volume) as total_volume,
                MIN(data_date) as first_date,
                MAX(data_date) as last_date,
                AVG(Price) as avg_price
            FROM {table_name}
            GROUP BY "#RIC"
            ORDER BY total_volume DESC NULLS LAST
            LIMIT 1000
            """
            
            result_df = self.db.execute_query(symbols_query)
            
            symbols = []
            for _, row in result_df.iterrows():
                symbols.append({
                    "symbol": row['symbol'],
                    "total_trades": int(row['total_trades']),
                    "total_volume": int(row['total_volume']) if row['total_volume'] else 0,
                    "first_date": str(row['first_date']),
                    "last_date": str(row['last_date']),
                    "avg_price": float(row['avg_price']) if row['avg_price'] else 0
                })
            
            return {
                "exchange": exchange,
                "symbols": symbols,
                "total_symbols": len(symbols),
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error reading symbols: {e}")
            return {"error": str(e)}
    
    async def read_data_quality_resource(self) -> Dict[str, Any]:
        """Read data quality report"""
        try:
            table_mapping = {
                'LSE': 'bronze.lse_market_data_raw',
                'CME': 'bronze.cme_market_data_raw',
                'NYQ': 'bronze.nyq_market_data_raw'
            }
            
            quality_report = {
                "report_type": "data_quality",
                "exchanges": {},
                "generated_at": datetime.now().isoformat()
            }
            
            for exchange, table_name in table_mapping.items():
                try:
                    quality_query = f"""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT data_date) as unique_dates,
                        COUNT(DISTINCT "#RIC") as unique_symbols,
                        COUNT(CASE WHEN Volume IS NULL THEN 1 END) as null_volume,
                        COUNT(CASE WHEN Price IS NULL THEN 1 END) as null_price
                    FROM {table_name}
                    """
                    
                    result_df = self.db.execute_query(quality_query)
                    
                    if not result_df.empty:
                        row = result_df.iloc[0]
                        quality_report["exchanges"][exchange] = {
                            "total_records": int(row['total_records']),
                            "unique_dates": int(row['unique_dates']),
                            "unique_symbols": int(row['unique_symbols']),
                            "null_volume_pct": (row['null_volume'] / row['total_records']) * 100 if row['total_records'] > 0 else 0,
                            "null_price_pct": (row['null_price'] / row['total_records']) * 100 if row['total_records'] > 0 else 0
                        }
                except Exception as e:
                    quality_report["exchanges"][exchange] = {"error": str(e)}
            
            return quality_report
            
        except Exception as e:
            logger.error(f"Error reading data quality: {e}")
            return {"error": str(e)}
    
    async def read_database_overview_resource(self) -> Dict[str, Any]:
        """Read database overview statistics"""
        try:
            overview = await self.list_datasets(include_stats=True)
            
            # Add summary statistics
            summary = {
                "database_overview": overview,
                "summary": {
                    "total_schemas": len(overview.get("schemas", {})),
                    "total_tables": sum(len(schema.get("tables", [])) for schema in overview.get("schemas", {}).values())
                },
                "generated_at": datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error reading database overview: {e}")
            return {"error": str(e)}

    async def read_symbol_categories_resource(self) -> Dict[str, Any]:
        """Read symbol categories resource"""
        try:
            return {
                "symbol_categories": SYMBOL_CATEGORIES,
                "usage_examples": {
                    "get_bitcoin_futures": {
                        "tool": "get_symbols_by_category",
                        "arguments": {"category": "bitcoin_futures"}
                    },
                    "get_bitcoin_volume_data": {
                        "tool": "get_category_volume_data", 
                        "arguments": {
                            "category": "bitcoin_futures",
                            "date": "2025-01-17",
                            "exchange": "CME"
                        }
                    },
                    "get_crypto_futures_with_stats": {
                        "tool": "get_symbols_by_category",
                        "arguments": {
                            "category": "crypto_futures",
                            "include_stats": True,
                            "date": "2025-01-17",
                            "exchange": "CME"
                        }
                    }
                },
                "performance_benefits": [
                    "Uses IN clause instead of expensive LIKE '%' queries",
                    "Predefined symbol lists avoid table scans",
                    "Optimized for common cryptocurrency futures queries",
                    "Faster response times for category-based analysis"
                ],
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error reading symbol categories: {e}")
            return {"error": str(e)}

    # === UTILITY METHODS ===
    
    def _resolve_table_name(self, dataset: str) -> str:
        """Resolve dataset name to actual table name"""
        # Check direct mapping first
        if dataset in TABLE_MAPPINGS:
            return TABLE_MAPPINGS[dataset]
        
        # Check if it's already a full table name
        if "." in dataset:
            return dataset
        
        # Try to find a partial match
        for key, table in TABLE_MAPPINGS.items():
            if dataset.lower() in key.lower():
                return table
        
        return None
    
    def _get_schema_description(self, schema: str) -> str:
        """Get description for a schema"""
        descriptions = {
            "bronze": "Raw, unprocessed market data from various exchanges",
            "silver": "Cleaned and validated market data with consistent schema",
            "gold": "Aggregated and analyzed market data for business insights"
        }
        return descriptions.get(schema, f"Data schema: {schema}") 