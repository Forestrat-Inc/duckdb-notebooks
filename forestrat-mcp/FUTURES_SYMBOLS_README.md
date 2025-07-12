# Futures Symbols Generation Tool

## Overview

The `get_next_futures_symbols` tool generates consecutive CME futures contract symbols for different product types. It supports Bitcoin products with intelligent mapping and returns "work in progress" status for unsupported products.

## Product Type Mapping

The tool uses intelligent mapping to convert user-friendly product names to CME symbol prefixes:

| User Input | Maps To | Symbol Prefix | Type |
|------------|---------|---------------|------|
| `"bitcoin"` | BTC | BTC | standard |
| `"micro bitcoin"` | MBT | MBT | micro |
| `"standard bitcoin"` | BTC | BTC | standard |
| `"btc"` | BTC | BTC | standard |
| `"mbt"` | MBT | MBT | micro |
| Any other | N/A | N/A | work_in_progress |

## Parameters

- **product_type** (string, required): Type of product to generate symbols for
- **start_month_name** (string, required): Starting month (full name like "January")
- **start_year** (integer, required): Starting year (2020-2030)
- **num_futures** (integer, required): Number of consecutive futures to generate (1-24)

## Month Codes

The tool uses CME's standard month codes:

| Month | Code | Month | Code |
|-------|------|-------|------|
| January | F | July | N |
| February | G | August | Q |
| March | H | September | U |
| April | J | October | V |
| May | K | November | X |
| June | M | December | Z |

## Examples

### Bitcoin Standard Futures
```json
{
  "tool": "get_next_futures_symbols",
  "arguments": {
    "product_type": "bitcoin",
    "start_month_name": "February",
    "start_year": 2025,
    "num_futures": 6
  }
}
```

**Response:**
```json
{
  "product_type": "bitcoin",
  "mapped_to": {
    "symbol_prefix": "BTC",
    "product_category": "standard"
  },
  "generation_parameters": {
    "start_month": "February",
    "start_year": 2025,
    "num_futures": 6
  },
  "symbols": ["BTCG5", "BTCH5", "BTCJ5", "BTCK5", "BTCM5", "BTCN5"],
  "symbol_count": 6,
  "note": "Generated 6 consecutive futures symbols starting from February 2025"
}
```

### Micro Bitcoin Futures
```json
{
  "tool": "get_next_futures_symbols",
  "arguments": {
    "product_type": "micro bitcoin",
    "start_month_name": "January",
    "start_year": 2025,
    "num_futures": 12
  }
}
```

**Response:**
```json
{
  "product_type": "micro bitcoin",
  "mapped_to": {
    "symbol_prefix": "MBT",
    "product_category": "micro"
  },
  "symbols": ["MBTF5", "MBTG5", "MBTH5", "MBTJ5", "MBTK5", "MBTM5", "MBTN5", "MBTQ5", "MBTU5", "MBTV5", "MBTX5", "MBTZ5"],
  "symbol_count": 12
}
```

### Unsupported Product
```json
{
  "tool": "get_next_futures_symbols",
  "arguments": {
    "product_type": "ethereum",
    "start_month_name": "March",
    "start_year": 2025,
    "num_futures": 4
  }
}
```

**Response:**
```json
{
  "product_type": "ethereum",
  "status": "work_in_progress",
  "message": "Product type 'ethereum' is not yet supported. Currently only Bitcoin products are available.",
  "supported_products": ["bitcoin", "micro bitcoin", "standard bitcoin", "btc", "mbt"],
  "symbols": []
}
```

## Symbol Format

Generated symbols follow the CME format: `{PREFIX}{MONTH_CODE}{YEAR_DIGIT}`

- **PREFIX**: Product symbol (BTC, MBT)
- **MONTH_CODE**: Single letter month code (F, G, H, etc.)
- **YEAR_DIGIT**: Last digit of the year (5 for 2025, 6 for 2026, etc.)

## Year Rollover

The tool automatically handles year rollover when generating symbols that cross calendar years:

```json
{
  "tool": "get_next_futures_symbols",
  "arguments": {
    "product_type": "mbt",
    "start_month_name": "November",
    "start_year": 2024,
    "num_futures": 4
  }
}
```

**Generates:** `["MBTX4", "MBTZ4", "MBTF5", "MBTG5"]`

Notice how it transitions from 2024 (year digit 4) to 2025 (year digit 5).

## Error Handling

The tool provides comprehensive error handling:

- **Invalid month names**: Returns validation error
- **Invalid year range**: Must be between 2020-2030
- **Invalid num_futures**: Must be between 1-24
- **Unsupported products**: Returns work_in_progress status

## Integration with Existing Tools

This tool complements existing category-based tools:

1. **get_symbols_by_category**: Get predefined lists of existing symbols
2. **get_category_volume_data**: Analyze trading data for symbol categories
3. **get_next_futures_symbols**: Generate new consecutive symbol lists

## Use Cases

1. **Trading Strategy Development**: Generate symbol lists for backtesting
2. **Portfolio Construction**: Create lists of consecutive futures contracts
3. **Market Analysis**: Generate symbols for cross-contract analysis
4. **Automated Trading**: Generate symbol lists for algorithmic trading systems
5. **Risk Management**: Create hedging strategies across multiple expiration months 