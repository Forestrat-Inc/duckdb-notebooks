# Forestrat MCP Server Installation Guide

## 🎯 Overview

The Forestrat MCP Server is now **complete and ready to use**! It provides a Model Context Protocol (MCP) server with 6 powerful tools for querying your multi-exchange DuckDB data lake.

## 📁 Created Files

```
forestrat-mcp/
├── main.py              # Main MCP server implementation
├── database.py          # Persistent DuckDB connection manager  
├── config.py            # Configuration settings
├── requirements.txt     # Python dependencies
├── README.md           # Detailed documentation
├── setup.py            # Automated setup script
├── test_connection.py  # Database connection test
├── example_client.py   # Usage examples and demos
├── server.py           # Simple server runner
└── INSTALL.md          # This installation guide
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
cd forestrat-mcp
pip install -r requirements.txt
```

### 2. Setup (Automated)

```bash
python setup.py
```

### 3. Test Connection

**Important**: Make sure no other processes are using the DuckDB database file before testing.

```bash
python test_connection.py
```

### 4. Test MCP Server Functionality

```bash
python test_mcp_server.py
```

### 5. Run the MCP Server

```bash
python main.py
```

The server communicates via stdin/stdout using the MCP protocol.

## 🛠️ Available Tools

Your MCP server provides these 6 tools:

1. **`list_datasets`** - List all datasets with vendor information and exchanges
2. **`get_dataset_exchanges`** - Get all exchanges available for a specific dataset  
3. **`get_data_for_time_range`** - Get data for a specific dataset and time range
4. **`query_data`** - Execute SQL-like queries on the data
5. **`get_table_schema`** - Get the schema/structure of a specific table
6. **`get_available_symbols`** - Get available symbols/instruments for a given exchange

## 💡 Usage Examples

### Run the Demo
```bash
python example_client.py
```

### Show Tool Schemas
```bash
python example_client.py --schemas
```

### Custom Database Path
```bash
FORESTRAT_DB_PATH=/path/to/your/database.duckdb python main.py
```

## 🎯 Key Features

- ✅ **Persistent Connection**: Avoids reconnecting for every query
- ✅ **6 Powerful Tools**: Complete data access and querying capabilities
- ✅ **Multi-Exchange Support**: LSE, CME, NYQ exchanges
- ✅ **SQL Query Support**: Execute custom SQL queries
- ✅ **Schema Discovery**: Explore database structure
- ✅ **Error Handling**: Comprehensive error handling and logging
- ✅ **Performance Optimized**: Configurable query limits and DuckDB settings

## 📊 Database Schema Mapping

The server works with your existing database structure:

- **Bronze Layer**: `bronze.lse_market_data`, `bronze.cme_market_data`, `bronze.nyq_market_data`
- **Silver Layer**: `silver.market_data_unified`, `silver.price_timeseries`  
- **Gold Layer**: `gold.daily_market_summary`, `gold.arbitrage_opportunities`

## 🔧 Configuration

Set environment variables:

```bash
export FORESTRAT_DB_PATH=/path/to/multi_exchange_data_lake.duckdb
export LOG_LEVEL=INFO
```

## ⚠️ Troubleshooting

### Database Lock Error
If you see "Conflicting lock is held" error:
1. Close any DuckDB CLI sessions
2. Close any Jupyter notebooks using the database
3. Wait a few seconds and try again

### Connection Issues
- Verify the database file exists at the specified path
- Check file permissions
- Ensure DuckDB version compatibility

## 🎉 Success!

Your Forestrat MCP Server is **complete and functional**! 

- All 6 tools are implemented and tested
- Persistent database connection is configured
- Comprehensive error handling is in place
- Documentation and examples are provided

You can now integrate this MCP server with any MCP-compatible client to query your multi-exchange financial data.

## 📞 Next Steps

1. Test with your actual data: `python test_connection.py`
2. Run the demo: `python example_client.py`
3. Start the MCP server: `python main.py`
4. Integrate with your MCP client application

**The MCP server is ready for production use!** 🚀 