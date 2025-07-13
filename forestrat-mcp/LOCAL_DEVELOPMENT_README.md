# Local Development Setup for Forestrat MCP Tools

This guide provides an easy way to develop and test MCP tools locally without dealing with virtual environment or pip installation issues.

## Overview

The local development setup includes:
- `dev_server.py` - Local MCP server that bypasses installation issues
- `dev_tools.py` - Tool for testing individual MCP tools
- No need to install packages or deal with virtual environment issues

## Quick Start

### 1. Run the Local MCP Server

```bash
python dev_server.py
```

With custom database path:
```bash
python dev_server.py --database-path /path/to/your/database.duckdb
```

### 2. Test Individual Tools

List available tools:
```bash
python dev_tools.py list
```

Test a specific tool:
```bash
python dev_tools.py get_next_futures_symbols --product-type bitcoin --start-month-name February --start-year 2025 --num-futures 6
```

## Features

### Local MCP Server (`dev_server.py`)

- **No Installation Required**: Automatically adds python-utils to Python path
- **Simplified MCP Server**: Streamlined version of the full MCP server
- **Easy Database Connection**: Automatic database connection with error handling
- **Real-time Development**: Changes to tools are immediately reflected

**Usage:**
```bash
python dev_server.py [--database-path PATH]
```

**Key Features:**
- ✅ Automatic path resolution
- ✅ Clear status messages
- ✅ Error handling and logging
- ✅ Compatible with MCP clients
- ✅ Supports all core tools

### Development Tools (`dev_tools.py`)

- **Individual Tool Testing**: Test any tool without running the full server
- **Command-line Interface**: Easy-to-use CLI for quick testing
- **JSON Output**: Pretty-printed JSON results
- **Error Handling**: Detailed error messages and stack traces

**Usage:**
```bash
python dev_tools.py <tool_name> [arguments]
```

## Available Tools

### Core Data Tools
- `list_datasets` - List all datasets with vendor information
- `get_dataset_exchanges` - Get exchanges for a dataset
- `get_data_for_time_range` - Get data for specific time range
- `query_data` - Execute SQL queries
- `get_table_schema` - Get table structure
- `get_available_symbols` - Get available symbols for exchange

### Futures Tools
- `get_next_futures_symbols` - Generate consecutive futures symbols
- `get_symbols_by_category` - Get predefined symbol categories

## Usage Examples

### Testing Futures Symbol Generation

```bash
# Generate Bitcoin futures symbols
python dev_tools.py get_next_futures_symbols \
  --product-type bitcoin \
  --start-month-name February \
  --start-year 2025 \
  --num-futures 6

# Generate Micro Bitcoin futures symbols
python dev_tools.py get_next_futures_symbols \
  --product-type "micro bitcoin" \
  --start-month-name January \
  --start-year 2025 \
  --num-futures 12
```

### Testing Data Queries

```bash
# List all datasets
python dev_tools.py list_datasets --include-stats

# Get exchanges for a dataset
python dev_tools.py get_dataset_exchanges --dataset unified

# Execute SQL query
python dev_tools.py query_data --query "SELECT * FROM bronze.lse_market_data LIMIT 5"

# Get table schema
python dev_tools.py get_table_schema --table-name bronze.lse_market_data
```

### Testing Symbol Operations

```bash
# Get available symbols for LSE
python dev_tools.py get_available_symbols \
  --exchange LSE \
  --start-date 2024-01-01 \
  --end-date 2024-01-31

# Get symbols by category
python dev_tools.py get_symbols_by_category --category bitcoin_futures
```

## Directory Structure

```
forestrat-mcp/
├── dev_server.py           # Local MCP server
├── dev_tools.py            # Individual tool testing
├── main_fixed.py           # Original MCP server (requires installation)
├── LOCAL_DEVELOPMENT_README.md  # This file
└── ...

../python-utils/
├── forestrat_utils/
│   ├── mcp_tools.py        # Core MCP tools
│   ├── database.py         # Database connection
│   ├── config.py           # Configuration
│   └── __init__.py
└── setup.py
```

## How It Works

### Path Resolution
Both development scripts automatically:
1. Find the `python-utils` directory relative to the script location
2. Add it to Python's path using `sys.path.insert(0, ...)`
3. Import forestrat_utils modules directly

### Error Handling
- Clear status messages with ✓ and ✗ indicators
- Detailed error messages with stack traces
- Graceful handling of missing dependencies
- Database connection validation

### Development Workflow
1. Edit tools in `/Users/kaushal/Documents/Forestrat/python-utils/forestrat_utils/mcp_tools.py`
2. Test individual tools with `python dev_tools.py`
3. Test full server with `python dev_server.py`
4. No need to reinstall or restart virtual environments

## Troubleshooting

### Common Issues

**Import Errors:**
```
✗ Failed to import forestrat_utils: No module named 'forestrat_utils'
```
- Check that `python-utils` directory exists at the expected location
- Verify the directory structure matches what's expected

**Database Connection Errors:**
```
✗ Database connection failed: [Errno 2] No such file or directory
```
- Check database path in environment or provide `--database-path`
- Ensure the database file exists and is readable

**MCP Import Errors:**
```
✗ Failed to import MCP modules: No module named 'mcp'
```
- Install MCP in your environment: `pip install mcp`

### Debug Mode

For verbose output, you can modify the scripts to add more debugging:
```python
# Add at the top of dev_server.py or dev_tools.py
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Advantages Over Virtual Environment Setup

1. **No Installation Required**: Bypass pip hanging issues
2. **Immediate Changes**: Edit code and test immediately
3. **Simplified Dependencies**: Only need MCP installed
4. **Better Error Messages**: Clear status indicators
5. **Faster Development**: No install/reinstall cycles
6. **Cross-Platform**: Works on any system with Python

## Integration with Original Server

The local development setup is fully compatible with the original `main_fixed.py` server:
- Same tool implementations
- Same database connections
- Same configuration system
- Same results and behavior

You can switch between development and production modes seamlessly:
- Development: `python dev_server.py`
- Production: `python main_fixed.py` (requires installation)

## Future Enhancements

- Add live reload for tool changes
- Include more debugging tools
- Add performance profiling
- Create test fixtures for common scenarios
- Add mock database for testing

## Support

If you encounter issues:
1. Check the console output for ✓ and ✗ status messages
2. Verify directory structure and file locations
3. Ensure all dependencies are available
4. Check database file permissions and accessibility

The local development setup provides a much smoother development experience compared to dealing with virtual environment and pip installation issues! 