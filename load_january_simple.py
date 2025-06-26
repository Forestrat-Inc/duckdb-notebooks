"""
Load January 2025 Data for All Exchanges - Simple Version

This script loads all available data for January 2025 across all exchanges (LSE, CME, NYQ)
using dynamic table creation without complex audit logging.
"""

import logging
import sys
from datetime import date, datetime
from pathlib import Path
import pandas as pd
import duckdb

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from utils.database import DuckDBManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/january_load_simple_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class SimpleMultiExchangeLoader:
    """
    Simple loader that creates tables based on actual file schemas
    """
    
    def __init__(self, db_manager: DuckDBManager = None):
        """Initialize the loader with database connection"""
        self.db_manager = db_manager or DuckDBManager(database_path="./multi_exchange_data_lake.duckdb")
        self.logger = logging.getLogger(__name__)
        
        # Supported exchanges and their configurations
        self.exchanges = ['LSE', 'CME', 'NYQ']
        
        # Ensure schemas exist
        self._create_schemas()
    
    def _create_schemas(self):
        """Create necessary schemas"""
        try:
            self.db_manager.execute_sql("CREATE SCHEMA IF NOT EXISTS bronze")
            self.logger.info("Schemas created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating schemas: {e}")
    
    def get_s3_path(self, exchange: str, data_date: date, stage: str = 'ingestion', file_type: str = 'Data') -> str:
        """Generate S3 path for a specific exchange, date, and processing stage"""
        date_str = data_date.isoformat()
        return (f"s3://vendor-data-s3/LSEG/TRTH/{exchange.upper()}/{stage}/"
                f"{date_str}/data/merged/{exchange.upper()}-{date_str}-NORMALIZEDMP-{file_type}-1-of-1.csv.gz")
    
    def create_dynamic_table(self, exchange: str, sample_file_path: str) -> bool:
        """Create table dynamically based on actual file schema"""
        table_name = f"bronze.{exchange.lower()}_market_data_raw"
        
        try:
            # First, check if table already exists
            check_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = '{exchange.lower()}_market_data_raw'"
            existing = self.db_manager.execute_query(check_query)
            
            if not existing.empty:
                self.logger.info(f"Table {table_name} already exists, skipping creation")
                return True
            
            # Create table from sample file
            create_sql = f"""
            CREATE TABLE {table_name} AS 
            SELECT 
                *,
                CAST(NULL AS DATE) as data_date,
                CAST(NULL AS VARCHAR) as exchange,
                CAST(NULL AS VARCHAR) as processing_stage,
                CAST(NULL AS VARCHAR) as source_file,
                CAST(NULL AS TIMESTAMP) as ingestion_timestamp
            FROM read_csv('{sample_file_path}', 
                         AUTO_DETECT=true,
                         SAMPLE_SIZE=10000,
                         FILENAME=false,
                         IGNORE_ERRORS=true)
            LIMIT 0
            """
            
            self.db_manager.execute_sql(create_sql)
            self.logger.info(f"Created dynamic table {table_name} from schema of {sample_file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating dynamic table for {exchange}: {e}")
            return False
    
    def load_single_file(self, exchange: str, data_date: date, stage: str = 'ingestion') -> dict:
        """Load a single file for a specific exchange and date"""
        result = {
            'exchange': exchange,
            'date': data_date,
            'stage': stage,
            'success': False,
            'records_loaded': 0,
            'error': None
        }
        
        try:
            # Log which date we're processing
            self.logger.info(f"Loading {exchange} data for date: {data_date}")
            
            # Get S3 path
            data_path = self.get_s3_path(exchange, data_date, stage, 'Data')
            table_name = f"bronze.{exchange.lower()}_market_data_raw"
            
            # Check if table exists, if not create it
            check_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = '{exchange.lower()}_market_data_raw'"
            existing = self.db_manager.execute_query(check_query)
            
            if existing.empty:
                if not self.create_dynamic_table(exchange, data_path):
                    result['error'] = f"Failed to create table for {exchange}"
                    return result
            
            # Check if data already exists
            check_existing = f"""
            SELECT COUNT(*) as count 
            FROM {table_name} 
            WHERE data_date = '{data_date}' AND processing_stage = '{stage}'
            """
            existing_data = self.db_manager.execute_query(check_existing)
            if existing_data.iloc[0]['count'] > 0:
                result['error'] = f"Data already exists for {exchange} {data_date} {stage}"
                return result
            
            # Load data with dynamic approach
            insert_sql = f"""
            INSERT INTO {table_name}
            SELECT 
                *,
                '{data_date}'::DATE as data_date,
                '{exchange}' as exchange,
                '{stage}' as processing_stage,
                '{data_path}' as source_file,
                NOW() as ingestion_timestamp
            FROM read_csv('{data_path}', 
                         AUTO_DETECT=true,
                         FILENAME=false,
                         IGNORE_ERRORS=true)
            """
            
            # Execute the insert
            self.db_manager.execute_sql(insert_sql)
            
            # Get count of loaded records
            count_query = f"""
            SELECT COUNT(*) as count 
            FROM {table_name} 
            WHERE data_date = '{data_date}' AND processing_stage = '{stage}'
            """
            count_result = self.db_manager.execute_query(count_query)
            records_loaded = count_result.iloc[0]['count']
            
            result['success'] = True
            result['records_loaded'] = records_loaded
            
            return result
            
        except Exception as e:
            result['error'] = str(e)
            return result
    
    def load_date_range(self, exchange: str, start_date: date, end_date: date, stage: str = 'ingestion') -> list:
        """Load data for an exchange across a date range"""
        results = []
        current_date = start_date
        
        while current_date <= end_date:
            result = self.load_single_file(exchange, current_date, stage)
            results.append(result)
            
            # Move to next date
            current_date = datetime.combine(current_date, datetime.min.time()) + pd.Timedelta(days=1)
            current_date = current_date.date()
            
        return results

def main():
    """Load all January 2025 data for all exchanges using simple dynamic schema"""
    
    # Initialize the loader
    logger.info("Initializing Simple MultiExchangeLoader...")
    db_manager = DuckDBManager(database_path="./multi_exchange_data_lake.duckdb")
    loader = SimpleMultiExchangeLoader(db_manager)
    
    # Define date range for January 2025 (user modified to 1-3)
    start_date = date(2025, 1, 17)
    end_date = date(2025, 1, 17)
    
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
        
        stage = 'ingestion'
        
        try:
            # Load date range for this exchange
            logger.info(f"Loading {exchange} data for {stage} stage from {start_date} to {end_date}")
            results = loader.load_date_range(exchange, start_date, end_date, stage)
            
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
    
    logger.info(f"\n✅ January data loading completed!")
    logger.info(f"Check the log file for detailed results.")

if __name__ == "__main__":
    main() 