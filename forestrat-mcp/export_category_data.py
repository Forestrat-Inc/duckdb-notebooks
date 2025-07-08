#!/usr/bin/env python3
"""
Standalone Category Data Exporter

Export all data for specific futures categories to CSV or JSON files.
This script can be run independently of the MCP server.

Usage:
    python export_category_data.py --category bitcoin_futures --exchange CME
    python export_category_data.py --category crypto_futures --exchange CME --start-date 2025-01-01 --format json
"""

import argparse
import os
import sys
import logging
from datetime import datetime
from typing import Optional

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import DuckDBConnection
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import symbol categories
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

def export_category_data(
    category: str,
    exchange: str,
    database_path: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_filename: Optional[str] = None,
    format: str = "csv",
    output_dir: Optional[str] = None
) -> dict:
    """Export all data for a specific futures category to a file"""
    
    try:
        # Validate category
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
                "error": f"Category {category} not available on exchange {exchange}",
                "available_exchanges": category_info["exchanges"]
            }
        
        # Initialize database connection
        db = DuckDBConnection(database_path)
        
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
        
        if not db.table_exists(table_name):
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
        logger.info(f"Query: {query}")
        
        result_df = db.execute_query(query)
        
        if result_df.empty:
            return {
                "error": "No data found for the specified criteria",
                "category": category,
                "exchange": exchange,
                "symbols_searched": symbols,
                "start_date": start_date,
                "end_date": end_date
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
            
            output_filename = f"{category}_{exchange.lower()}{date_suffix}_{timestamp}.{format}"
        
        # Ensure the filename has the correct extension
        if not output_filename.endswith(f".{format}"):
            output_filename += f".{format}"
        
        # Set output directory
        if not output_dir:
            output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports")
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Full path for the output file
        output_path = os.path.join(output_dir, output_filename)
        
        # Export the data
        logger.info(f"Exporting {len(result_df)} records to {output_path}")
        
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
        
        file_size_mb = round(os.path.getsize(output_path) / (1024 * 1024), 2)
        
        db.close()
        
        return {
            "success": True,
            "category": category,
            "description": category_info["description"],
            "exchange": exchange,
            "export_details": {
                "output_file": output_path,
                "filename": output_filename,
                "format": format,
                "file_size_mb": file_size_mb
            },
            "query_info": {
                "symbols_requested": symbols,
                "start_date": start_date,
                "end_date": end_date,
                "table_queried": table_name
            },
            "summary_statistics": summary_stats,
            "export_timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error exporting category data: {e}")
        import traceback
        logger.error(f"Export error traceback: {traceback.format_exc()}")
        return {"error": str(e)}

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(
        description="Export futures category data to CSV or JSON files"
    )
    
    parser.add_argument(
        "--category",
        choices=list(SYMBOL_CATEGORIES.keys()),
        help="Futures category to export"
    )
    
    parser.add_argument(
        "--exchange",
        choices=["LSE", "CME", "NYQ"],
        help="Exchange to export data from"
    )
    
    parser.add_argument(
        "--database-path",
        default="../multi_exchange_data_lake.duckdb",
        help="Path to the DuckDB database file"
    )
    
    parser.add_argument(
        "--start-date",
        help="Start date for export (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        help="End date for export (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--output-filename",
        help="Output filename (auto-generated if not provided)"
    )
    
    parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Export format (default: csv)"
    )
    
    parser.add_argument(
        "--output-dir",
        help="Output directory (default: ./exports)"
    )
    
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="List available categories and exit"
    )
    
    args = parser.parse_args()
    
    # List categories if requested
    if args.list_categories:
        print("\nAvailable Categories:")
        print("=" * 50)
        for category, info in SYMBOL_CATEGORIES.items():
            print(f"\n{category}:")
            print(f"  Description: {info['description']}")
            print(f"  Symbols: {', '.join(info['symbols'])}")
            print(f"  Exchanges: {', '.join(info['exchanges'])}")
        return
    
    # Validate required arguments when not listing categories
    if not args.category:
        print("Error: --category is required when not using --list-categories")
        return 1
    
    if not args.exchange:
        print("Error: --exchange is required when not using --list-categories")
        return 1
    
    # Validate database path
    if not os.path.exists(args.database_path):
        print(f"Error: Database file not found: {args.database_path}")
        return 1
    
    # Print configuration
    print(f"\nExporting Category Data")
    print("=" * 50)
    print(f"Category: {args.category}")
    print(f"Exchange: {args.exchange}")
    print(f"Database: {args.database_path}")
    print(f"Start Date: {args.start_date or 'All available'}")
    print(f"End Date: {args.end_date or 'All available'}")
    print(f"Format: {args.format}")
    print(f"Output Dir: {args.output_dir or './exports'}")
    print()
    
    # Export the data
    result = export_category_data(
        category=args.category,
        exchange=args.exchange,
        database_path=args.database_path,
        start_date=args.start_date,
        end_date=args.end_date,
        output_filename=args.output_filename,
        format=args.format,
        output_dir=args.output_dir
    )
    
    # Print results
    if "error" in result:
        print(f"❌ Export failed: {result['error']}")
        if "available_categories" in result:
            print(f"Available categories: {', '.join(result['available_categories'])}")
        if "available_exchanges" in result:
            print(f"Available exchanges: {', '.join(result['available_exchanges'])}")
        return 1
    else:
        print("✅ Export completed successfully!")
        print(f"📁 Output file: {result['export_details']['output_file']}")
        print(f"📊 Records exported: {result['summary_statistics']['total_records']:,}")
        print(f"🎯 Symbols found: {result['summary_statistics']['unique_symbols']}")
        print(f"📅 Date range: {result['summary_statistics']['date_range']['earliest']} to {result['summary_statistics']['date_range']['latest']}")
        print(f"💾 File size: {result['export_details']['file_size_mb']} MB")
        print(f"📈 Symbols exported: {', '.join(result['summary_statistics']['symbols_found'])}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 