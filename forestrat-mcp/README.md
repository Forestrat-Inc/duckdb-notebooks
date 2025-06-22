# Forestrat MCP Server

A Model Context Protocol (MCP) server for interacting with multi-exchange financial market data stored in DuckDB.

## Overview

This MCP server provides tools to query and analyze market data from multiple exchanges (LSE, CME, NYQ) stored in a DuckDB data lake. It maintains a persistent connection to avoid reconnecting for every query.

## Features

### Available Tools

1. **list_datasets** - List all datasets with vendor information and exchanges
2. **get_dataset_exchanges** - Get all exchanges available for a specific dataset
3. **get_data_for_time_range** - Get data for a specific dataset and time range
4. **query_data** - Execute SQL-like queries on the data
5. **get_table_schema** - Get the schema/structure of a specific table
6. **get_available_symbols** - Get available symbols/instruments for a given exchange

## Database Structure

The server connects to a DuckDB database with the following schema:

- **Bronze Layer**: Raw ingested data from exchanges
  - `bronze.lse_market_data` - LSE market data
  - `bronze.cme_market_data` - CME market data
  - `bronze.nyq_market_data` - NYQ market data
  - `bronze.market_data_reports` - Processing reports

- **Silver Layer**: Cleaned and normalized data
  - `silver.market_data_unified` - Unified market data across exchanges
  - `silver.price_timeseries` - Time series data

- **Gold Layer**: Aggregated and business-ready data
  - `gold.daily_market_summary` - Daily market summaries
  - `gold.arbitrage_opportunities` - Arbitrage analysis

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Set the database path using the environment variable:

```bash
export FORESTRAT_DB_PATH=/path/to/multi_exchange_data_lake.duckdb
```

Default path: `../multi_exchange_data_lake.duckdb`

## Usage

### Running the Server

```bash
python main.py
```

Or use the server runner:

```bash
python server.py
```

### Example Tool Calls

#### List all datasets
```json
{
  "tool": "list_datasets",
  "arguments": {
    "include_stats": true
  }
}
```

#### Get exchanges for a dataset
```json
{
  "tool": "get_dataset_exchanges",
  "arguments": {
    "dataset": "unified"
  }
}
```

#### Get data for time range
```json
{
  "tool": "get_data_for_time_range",
  "arguments": {
    "dataset": "lse",
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "exchange": "LSE",
    "limit": 100
  }
}
```

#### Execute SQL query
```json
{
  "tool": "query_data",
  "arguments": {
    "query": "SELECT symbol, AVG(price) as avg_price FROM silver.market_data_unified WHERE exchange = 'LSE' GROUP BY symbol",
    "limit": 50
  }
}
```

#### Get table schema
```json
{
  "tool": "get_table_schema",
  "arguments": {
    "table_name": "bronze.lse_market_data"
  }
}
```

#### Get available symbols
```json
{
  "tool": "get_available_symbols",
  "arguments": {
    "exchange": "LSE",
    "start_date": "2024-01-01",
    "end_date": "2024-01-31"
  }
}
```

## Dataset Mapping

The server provides convenient aliases for common datasets:

- `lse` → `bronze.lse_market_data`
- `cme` → `bronze.cme_market_data`
- `nyq` → `bronze.nyq_market_data`
- `unified` → `silver.market_data_unified`
- `market_data` → `silver.market_data_unified`
- `timeseries` → `silver.price_timeseries`
- `daily_summary` → `gold.daily_market_summary`
- `arbitrage` → `gold.arbitrage_opportunities`

## Error Handling

The server includes comprehensive error handling and logging. All errors are returned as structured JSON responses with appropriate error messages.

## Performance

- Persistent database connection for better performance
- Configurable query limits (default: 1000, max: 10000)
- Optimized DuckDB settings for analytics workloads

## Dependencies

- `mcp>=1.0.0` - Model Context Protocol
- `duckdb>=0.10.0` - DuckDB database
- `pandas>=2.0.0` - Data manipulation
- `pydantic>=2.0.0` - Data validation 