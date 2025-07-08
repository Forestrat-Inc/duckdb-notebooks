# Futures Category Data Export

This directory contains tools to export all data for specific futures categories (e.g., bitcoin_futures, ethereum_futures) to CSV or JSON files.

## Available Categories

The following predefined categories are available for export:

- **bitcoin_futures**: Bitcoin futures contracts (MBTF25, BTCF25, MBTG25, BTCG25)
- **ethereum_futures**: Ethereum futures contracts (HTEF25, ETEF25, MTEF25, ETEG25)
- **crypto_futures**: All cryptocurrency futures contracts (combines bitcoin and ethereum)
- **micro_bitcoin**: Micro Bitcoin futures (MBTF25, MBTG25)
- **standard_bitcoin**: Standard Bitcoin futures (BTCF25, BTCG25)
- **micro_ethereum**: Micro Ethereum futures (HTEF25, MTEF25)
- **standard_ethereum**: Standard Ethereum futures (ETEF25, ETEG25)

All categories are currently available on the **CME** exchange.

## Method 1: Using the MCP Tool

### Prerequisites
- MCP server running (`python main_fixed.py`)
- MCP client connected to the server

### Usage via MCP Tool

The `export_category_data` tool is available through the MCP interface:

```json
{
  "name": "export_category_data",
  "arguments": {
    "category": "bitcoin_futures",
    "exchange": "CME",
    "start_date": "2025-01-01",
    "end_date": "2025-01-31",
    "format": "csv"
  }
}
```

#### Required Parameters:
- `category`: One of the available categories
- `exchange`: Exchange name (CME, LSE, NYQ)

#### Optional Parameters:
- `start_date`: Start date for export (YYYY-MM-DD)
- `end_date`: End date for export (YYYY-MM-DD)
- `output_filename`: Custom filename (auto-generated if not provided)
- `format`: Export format ("csv" or "json", default: "csv")

## Method 2: Using the Standalone Script

### Prerequisites
- Python 3.7+
- Required packages: `pandas`, `duckdb`
- Access to the DuckDB database file

### Installation

```bash
# Navigate to the forestrat-mcp directory
cd forestrat-mcp

# Install dependencies if not already installed
pip install pandas duckdb
```

### Usage Examples

#### List available categories:
```bash
python export_category_data.py --list-categories
```

#### Export all Bitcoin futures data:
```bash
python export_category_data.py --category bitcoin_futures --exchange CME
```

#### Export crypto futures for a specific date range:
```bash
python export_category_data.py \
  --category crypto_futures \
  --exchange CME \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --format json
```

#### Export with custom filename and output directory:
```bash
python export_category_data.py \
  --category ethereum_futures \
  --exchange CME \
  --output-filename "eth_futures_q1_2025.csv" \
  --output-dir "/path/to/exports"
```

### Command Line Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `--category` | Yes | Futures category to export | `bitcoin_futures` |
| `--exchange` | Yes | Exchange (CME, LSE, NYQ) | `CME` |
| `--database-path` | No | Path to DuckDB file | `../multi_exchange_data_lake.duckdb` |
| `--start-date` | No | Start date (YYYY-MM-DD) | `2025-01-01` |
| `--end-date` | No | End date (YYYY-MM-DD) | `2025-01-31` |
| `--output-filename` | No | Custom filename | `my_export.csv` |
| `--format` | No | Export format (csv/json) | `csv` |
| `--output-dir` | No | Output directory | `./exports` |
| `--list-categories` | No | List available categories | - |

## Output

### File Location
- Default directory: `./exports/` (created automatically)
- Auto-generated filename format: `{category}_{exchange}_{date_range}_{timestamp}.{format}`
- Example: `bitcoin_futures_cme_2025-01-01_to_2025-01-31_20250117_143022.csv`

### Data Columns
The exported data includes all raw market data columns:

- `data_date`: Trading date
- `symbol`: RIC symbol (e.g., MBTF25)
- `datetime`: Date-time of the trade
- `Type`: Trade type
- `Price`: Trade price
- `Volume`: Trade volume
- `exchange_time`: Exchange timestamp
- `Qualifiers`: Trade qualifiers

### Export Summary
Both methods provide detailed export summaries including:

- Total records exported
- Number of unique symbols found
- Date range of the data
- File size and location
- Summary statistics (total volume, average price)

## Performance

### Optimization Features
- Uses `IN` clause with predefined symbol lists (no expensive `LIKE` operations)
- Efficient queries that avoid full table scans
- Optimized for large datasets

### Expected Performance
- Bitcoin futures (4 symbols): ~10,000-50,000 records per day
- Crypto futures (8 symbols): ~20,000-100,000 records per day
- Processing speed: ~1-5 minutes for monthly exports

## Troubleshooting

### Common Issues

1. **Database not found**
   ```
   Error: Database file not found: ../multi_exchange_data_lake.duckdb
   ```
   - Check the database path
   - Ensure you're running from the correct directory

2. **No data found**
   ```
   No data found for the specified criteria
   ```
   - Verify the date range has data
   - Check if the category exists on the specified exchange
   - Use `--list-categories` to see available options

3. **Permission errors**
   ```
   PermissionError: [Errno 13] Permission denied
   ```
   - Check write permissions for the output directory
   - Try a different output directory with `--output-dir`

### Logging
The script provides detailed logging. For verbose output, the logs include:
- Query execution details
- Export progress
- File creation confirmation

## Examples

### Export Bitcoin futures for a single day (tested example)
```bash
python export_category_data.py \
  --category bitcoin_futures \
  --exchange CME \
  --start-date 2025-01-17 \
  --end-date 2025-01-17 \
  --format csv
```

**Actual output:**
```
✅ Export completed successfully!
📁 Output file: /path/to/forestrat-mcp/exports/bitcoin_futures_cme_2025-01-17_to_2025-01-17_20250703_142849.csv
📊 Records exported: 3,489,942
🎯 Symbols found: 4
📅 Date range: 2025-01-17 to 2025-01-17
💾 File size: 400.56 MB
📈 Symbols exported: BTCF25, BTCG25, MBTF25, MBTG25
```

**CSV Structure:**
```csv
data_date,symbol,datetime,Type,Price,Volume,exchange_time,Qualifiers
2025-01-17,BTCF25,2025-01-16 18:00:00.071378-05:00,Mkt. Condition,,,,"0[HALT_REASN];""  ""[HALT_RSN]"
2025-01-17,BTCF25,2025-01-16 18:00:00.071378-05:00,Mkt. Condition,,,,"0[HALT_REASN];""  ""[HALT_RSN]"
...
```

### Export all crypto futures as JSON
```bash
python export_category_data.py \
  --category crypto_futures \
  --exchange CME \
  --format json
```

This will export all available crypto futures data (both Bitcoin and Ethereum) to a JSON file.

## Integration

The export functionality is designed to integrate seamlessly with:
- Data analysis workflows (pandas, R, etc.)
- Business intelligence tools (Tableau, Power BI)
- Custom trading algorithms
- Research and backtesting frameworks

The exported data maintains all original market data fidelity while providing easy access to category-specific datasets. 