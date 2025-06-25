"""
Configuration for Forestrat MCP Server
"""

import os
from pathlib import Path

class Config:
    """Configuration class for the MCP server"""
    
    # Database configuration
    DATABASE_PATH = os.getenv(
        'DATABASE_PATH', 
        '/Users/kaushal/Documents/Forestrat/duckdb/multi_exchange_data_lake.duckdb'
    )
    
    # Server configuration
    SERVER_NAME = "forestrat-mcp"
    SERVER_VERSION = "1.0.0"
    
    # Query limits
    DEFAULT_QUERY_LIMIT = 1000
    MAX_QUERY_LIMIT = 10000
    
    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Exchange mappings
    EXCHANGE_TABLE_MAPPING = {
        'LSE': 'bronze.lse_market_data_raw',
        'CME': 'bronze.cme_market_data_raw',
        'NYQ': 'bronze.nyq_market_data_raw'
    }
    
    # Dataset mappings
    DATASET_MAPPING = {
        'lse': 'bronze.lse_market_data_raw',
        'cme': 'bronze.cme_market_data_raw',
        'nyq': 'bronze.nyq_market_data_raw',
        'unified': 'silver.market_data_unified',
        'market_data': 'silver.market_data_unified',
        'timeseries': 'silver.price_timeseries',
        'daily_summary': 'gold.daily_market_summary',
        'arbitrage': 'gold.arbitrage_opportunities'
    }
    
    # Schema descriptions
    SCHEMA_DESCRIPTIONS = {
        'bronze': 'Raw ingested data from exchanges',
        'silver': 'Cleaned and normalized data',
        'gold': 'Aggregated and business-ready data',
        'staging': 'Temporary staging tables',
        'audit': 'Audit and monitoring tables',
        'views': 'Database views'
    }

config = Config() 