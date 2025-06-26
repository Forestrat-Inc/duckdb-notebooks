"""
Load January 2025 Data for All Exchanges

This script loads all available data for January 2025 across all exchanges (LSE, CME, NYQ)
using the MultiExchangeLoader class.
"""

import logging
import sys
from datetime import date, datetime
from pathlib import Path
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from utils.multi_exchange_loader import MultiExchangeLoader
from utils.database import DuckDBManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/january_load_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Load all January 2025 data for all exchanges"""
    
    # Initialize the loader
    logger.info("Initializing MultiExchangeLoader...")
    db_manager = DuckDBManager(database_path="./multi_exchange_data_lake.duckdb")
    loader = MultiExchangeLoader(db_manager)
    
    # Define date range for January 2025
    start_date = date(2025, 1, 1)
    end_date = date(2025, 1, 31)
    
    logger.info(f"Loading data from {start_date} to {end_date} for all exchanges")
    
    # Track overall statistics
    total_stats = {
        'total_files_processed': 0,
        'successful_loads': 0,
        'failed_loads': 0,
        'total_records_loaded': 0,
        'exchanges_processed': set()
    }
    
    exchange_stats = {}
    
    # Process each exchange
    for exchange in ['LSE', 'CME', 'NYQ']:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {exchange} exchange")
        logger.info(f"{'='*60}")
        
        exchange_stats[exchange] = {
            'files_processed': 0,
            'successful_loads': 0,
            'failed_loads': 0,
            'total_records': 0,
            'dates_processed': []
        }
        
        # Get available stages for this exchange
        stages = loader.exchanges[exchange]['stages']
        logger.info(f"Available stages for {exchange}: {stages}")
        
        # For now, focus on 'ingestion' stage as it's available for all exchanges
        stage = 'ingestion'
        
        try:
            # Load date range for this exchange
            logger.info(f"Loading {exchange} data for {stage} stage from {start_date} to {end_date}")
            results = loader.load_date_range(exchange, start_date, end_date, stage, force_reload=False)
            
            # Process results
            for result in results:
                total_stats['total_files_processed'] += 1
                exchange_stats[exchange]['files_processed'] += 1
                
                if result['success']:
                    total_stats['successful_loads'] += 1
                    total_stats['total_records_loaded'] += result['records_loaded']
                    exchange_stats[exchange]['successful_loads'] += 1
                    exchange_stats[exchange]['total_records'] += result['records_loaded']
                    exchange_stats[exchange]['dates_processed'].append(result['date'])
                    
                    logger.info(f"✅ {exchange} {result['date']}: {result['records_loaded']:,} records loaded")
                else:
                    total_stats['failed_loads'] += 1
                    exchange_stats[exchange]['failed_loads'] += 1
                    
                    if "already exists" in str(result.get('error', '')):
                        logger.info(f"⏭️  {exchange} {result['date']}: Data already exists, skipping")
                    else:
                        logger.error(f"❌ {exchange} {result['date']}: {result.get('error', 'Unknown error')}")
            
            total_stats['exchanges_processed'].add(exchange)
            
            # Update pipeline state for this exchange
            if exchange_stats[exchange]['successful_loads'] > 0:
                last_date = max(exchange_stats[exchange]['dates_processed'])
                loader.update_pipeline_state(
                    exchange=exchange,
                    stage=stage,
                    last_processed_date=last_date,
                    records_processed=exchange_stats[exchange]['total_records'],
                    status='completed'
                )
                logger.info(f"Updated pipeline state for {exchange}")
            
        except Exception as e:
            logger.error(f"Error processing {exchange}: {e}")
            continue
    
    # Print summary
    logger.info(f"\n{'='*60}")
    logger.info("LOADING SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Exchanges processed: {', '.join(total_stats['exchanges_processed'])}")
    logger.info(f"Total files processed: {total_stats['total_files_processed']}")
    logger.info(f"Successful loads: {total_stats['successful_loads']}")
    logger.info(f"Failed loads: {total_stats['failed_loads']}")
    logger.info(f"Total records loaded: {total_stats['total_records_loaded']:,}")
    
    # Exchange breakdown
    logger.info(f"\n{'='*30}")
    logger.info("EXCHANGE BREAKDOWN")
    logger.info(f"{'='*30}")
    for exchange, stats in exchange_stats.items():
        logger.info(f"\n{exchange}:")
        logger.info(f"  Files processed: {stats['files_processed']}")
        logger.info(f"  Successful: {stats['successful_loads']}")
        logger.info(f"  Failed: {stats['failed_loads']}")
        logger.info(f"  Records loaded: {stats['total_records']:,}")
        logger.info(f"  Date range: {min(stats['dates_processed'])} to {max(stats['dates_processed'])}" 
                   if stats['dates_processed'] else "  No dates processed")
    
    # Check current loading status
    logger.info(f"\n{'='*30}")
    logger.info("CURRENT DATABASE STATUS")
    logger.info(f"{'='*30}")
    
    try:
        status_df = loader.get_loading_status(days_back=35)  # January + some buffer
        if not status_df.empty:
            # Group by exchange
            for exchange in ['LSE', 'CME', 'NYQ']:
                exchange_data = status_df[status_df['exchange'] == exchange]
                if not exchange_data.empty:
                    total_records = exchange_data['record_count'].sum()
                    date_range = f"{exchange_data['data_date'].min()} to {exchange_data['data_date'].max()}"
                    logger.info(f"{exchange}: {total_records:,} records, dates {date_range}")
        else:
            logger.warning("No loading status found in database")
    except Exception as e:
        logger.error(f"Error getting loading status: {e}")
    
    logger.info(f"\n✅ January data loading completed!")
    logger.info(f"Check the log file for detailed results.")

if __name__ == "__main__":
    main() 