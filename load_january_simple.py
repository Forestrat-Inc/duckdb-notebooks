"""
Load January 2025 Data for All Exchanges - Simple Version

This script loads all available data for January 2025 across all exchanges (LSE, CME, NYQ)
using dynamic table creation without complex audit logging.

Features:
- Graceful shutdown on interruption (Ctrl+C)
- Idempotent mode for resuming interrupted loads
- Transaction handling for atomic operations
- Comprehensive progress tracking and statistics
"""

import logging
import sys
import signal
import argparse
import time
import threading
from datetime import date, datetime
from pathlib import Path
import pandas as pd
import duckdb

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from utils.database import DuckDBManager
from utils.supabase_manager import SupabaseManager

# Global flag for graceful shutdown
shutdown_requested = False
shutdown_file_path = Path("./shutdown_load_january.flag")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    signal_name = signal.Signals(signum).name
    print(f"\n🛑 Received {signal_name} signal. Initiating graceful shutdown...")
    print("   Current transaction will complete before stopping.")
    print("   Use --idempotent flag to resume from where we left off.")
    shutdown_requested = True

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

def check_shutdown_file() -> bool:
    """Check if shutdown file exists (useful for NiFi integration)"""
    return shutdown_file_path.exists()

def create_shutdown_file():
    """Create shutdown file to signal script to stop"""
    shutdown_file_path.write_text(f"Shutdown requested at {datetime.now().isoformat()}")
    print(f"🛑 Shutdown file created: {shutdown_file_path}")

def remove_shutdown_file():
    """Remove shutdown file if it exists"""
    if shutdown_file_path.exists():
        shutdown_file_path.unlink()
        print(f"🗑️ Shutdown file removed: {shutdown_file_path}")
    # No message if file doesn't exist (silent cleanup)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Load January 2025 market data for a specific date",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python load_january_simple.py --date 2025-01-15                    # Load specific date
  python load_january_simple.py --date 2025-01-15 --idempotent       # Resume/skip existing data
  python load_january_simple.py --date 2025-01-15 --exchanges LSE CME # Load specific exchanges only
  
Multiple Instances (Parallel Processing):
  python load_january_simple.py --date 2025-01-01 --idempotent &     # Instance 1
  python load_january_simple.py --date 2025-01-02 --idempotent &     # Instance 2
  python load_january_simple.py --date 2025-01-03 --idempotent &     # Instance 3
  
NiFi Integration:
  # To stop while running in NiFi:
  python load_january_simple.py --create-shutdown-file
  
  # To remove shutdown file:
  python load_january_simple.py --remove-shutdown-file
        """
    )
    
    parser.add_argument(
        '--idempotent', '--resume',
        action='store_true',
        help='Skip already loaded data instead of failing (allows resuming interrupted loads)'
    )
    
    parser.add_argument(
        '--exchanges',
        nargs='+',
        choices=['LSE', 'CME', 'NYQ'],
        default=['LSE', 'CME', 'NYQ'],
        help='Specific exchanges to process (default: all)'
    )
    
    parser.add_argument(
        '--date',
        type=lambda d: datetime.strptime(d, '%Y-%m-%d').date(),
        default=date.today(),
        help='Date to process in YYYY-MM-DD format (default: today)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    # NiFi integration options
    parser.add_argument(
        '--create-shutdown-file',
        action='store_true',
        help='Create shutdown file to stop running script (useful for NiFi)'
    )
    
    parser.add_argument(
        '--remove-shutdown-file',
        action='store_true',
        help='Remove shutdown file'
    )
    
    parser.add_argument(
        '--check-shutdown-file',
        action='store_true',
        help='Check if shutdown file exists and exit with appropriate code'
    )
    
    return parser.parse_args()

class SimpleMultiExchangeLoader:
    """
    Simple loader that creates tables based on actual file schemas
    """
    
    def __init__(self, db_manager: DuckDBManager = None, idempotent: bool = False):
        """Initialize the loader with database connections"""
        self.db_manager = db_manager or DuckDBManager(database_path="./multi_exchange_data_lake.duckdb")
        self.logger = logging.getLogger(__name__)
        self.idempotent = idempotent
        
        # Initialize Supabase connection for statistics
        try:
            self.supabase_manager = SupabaseManager()
            self.logger.info("✅ Supabase connection established for statistics tracking")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to connect to Supabase - statistics will only be tracked locally: {e}")
            self.supabase_manager = None
        
        # Supported exchanges and their configurations
        self.exchanges = ['LSE', 'CME', 'NYQ']
        
        # Statistics for tracking
        self.stats = {
            'files_processed': 0,
            'files_skipped': 0,
            'files_loaded': 0,
            'files_failed': 0,
            'total_records': 0,
            'interrupted': False
        }
        
        # Ensure schemas exist
        self._create_schemas()
        
        # Initialize statistics tracking
        self._initialize_stats_tracking()
        
        # Progress tracking for long operations (thread-safe)
        self._active_progress_threads = {}
    
    def _start_progress_logger(self, operation_name: str, exchange: str, data_date: date):
        """Start a background thread to log progress during long operations"""
        # Create unique key for this operation
        operation_key = f"{exchange}_{data_date}_{operation_name}"
        
        # Stop any existing thread for this operation
        self._stop_progress_logger(operation_key)
        
        # Create operation-specific tracking
        operation_data = {
            'start_time': time.time(),
            'active': True,
            'operation_name': operation_name,
            'exchange': exchange,
            'data_date': data_date
        }
        
        def log_progress():
            while operation_data['active']:
                time.sleep(30)  # Log every 30 seconds
                if operation_data['active']:
                    elapsed = time.time() - operation_data['start_time']
                    self.logger.info(f"⏳ {operation_data['operation_name']} for {operation_data['exchange']} {operation_data['data_date']} - {elapsed:.0f}s elapsed, still processing...")
        
        # Start background thread
        progress_thread = threading.Thread(target=log_progress, daemon=True)
        progress_thread.start()
        
        # Store thread info
        self._active_progress_threads[operation_key] = {
            'thread': progress_thread,
            'data': operation_data
        }
    
    def _stop_progress_logger(self, operation_key: str = None):
        """Stop the progress logger for a specific operation or all operations"""
        if operation_key:
            # Stop specific operation
            if operation_key in self._active_progress_threads:
                self._active_progress_threads[operation_key]['data']['active'] = False
                del self._active_progress_threads[operation_key]
        else:
            # Stop all progress loggers (for cleanup)
            for key in list(self._active_progress_threads.keys()):
                self._active_progress_threads[key]['data']['active'] = False
            self._active_progress_threads.clear()
    
    def check_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested"""
        global shutdown_requested
        
        # Check signal-based shutdown
        if shutdown_requested:
            self.stats['interrupted'] = True
            self.logger.info("🛑 Shutdown requested via signal - stopping processing")
            return True
        
        # Check file-based shutdown (useful for NiFi integration)
        if check_shutdown_file():
            self.stats['interrupted'] = True
            self.logger.info("🛑 Shutdown requested via file - stopping processing")
            return True
            
        return False
    
    def _create_schemas(self):
        """Create necessary schemas"""
        try:
            self.db_manager.execute_sql("CREATE SCHEMA IF NOT EXISTS bronze")
            self.db_manager.execute_sql("CREATE SCHEMA IF NOT EXISTS silver")  
            self.db_manager.execute_sql("CREATE SCHEMA IF NOT EXISTS gold")
            self.logger.info("Schemas created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating schemas: {e}")
    
    def _initialize_stats_tracking(self):
        """Initialize statistics tracking tables"""
        try:
            # Create simple load progress table
            progress_table_sql = """
            CREATE TABLE IF NOT EXISTS bronze.load_progress (
                id INTEGER PRIMARY KEY,
                exchange VARCHAR NOT NULL,
                data_date DATE NOT NULL,
                file_path VARCHAR NOT NULL,
                start_time TIMESTAMP DEFAULT NOW(),
                end_time TIMESTAMP,
                status VARCHAR DEFAULT 'started', -- started, completed, failed
                records_loaded INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
            self.db_manager.execute_sql(progress_table_sql)
            
            # Create daily stats table
            daily_stats_sql = """
            CREATE TABLE IF NOT EXISTS gold.daily_load_stats (
                id INTEGER PRIMARY KEY,
                stats_date DATE NOT NULL,
                exchange VARCHAR NOT NULL,
                total_files INTEGER DEFAULT 0,
                successful_files INTEGER DEFAULT 0,
                failed_files INTEGER DEFAULT 0,
                total_records BIGINT DEFAULT 0,
                avg_records_per_file DECIMAL(20,2),
                total_processing_time_seconds DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(stats_date, exchange)
            )
            """
            self.db_manager.execute_sql(daily_stats_sql)
            
            # Create weekly rolling stats table
            weekly_stats_sql = """
            CREATE TABLE IF NOT EXISTS gold.weekly_load_stats (
                id INTEGER PRIMARY KEY,
                week_ending DATE NOT NULL,
                exchange VARCHAR NOT NULL,
                avg_daily_files DECIMAL(10,2),
                avg_daily_records DECIMAL(20,2),
                total_files INTEGER DEFAULT 0,
                total_records BIGINT DEFAULT 0,
                avg_processing_time_seconds DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(week_ending, exchange)
            )
            """
            self.db_manager.execute_sql(weekly_stats_sql)
            
            self.logger.info("Statistics tracking tables initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing statistics tracking: {e}")
    
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
        """Load a single file for a specific exchange and date with transaction handling"""
        result = {
            'exchange': exchange,
            'date': data_date,
            'stage': stage,
            'success': False,
            'records_loaded': 0,
            'error': None,
            'processing_time': 0,
            'skipped': False
        }
        
        # Check for shutdown request before processing
        if self.check_shutdown_requested():
            result['error'] = "Shutdown requested"
            return result
        
        start_time = datetime.now()
        progress_id = None
        # Initialize progress_tracking early to avoid reference before assignment
        progress_tracking = None
        
        try:
            # Log which date we're processing
            self.logger.info(f"📂 Loading {exchange} data for date: {data_date}")
            
            # Get S3 path
            data_path = self.get_s3_path(exchange, data_date, stage, 'Data')
            table_name = f"bronze.{exchange.lower()}_market_data_raw"
            
            self.logger.info(f"📍 Step 1/5: Checking table schema for {exchange}")
            
            # Check if table exists, if not create it
            check_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = '{exchange.lower()}_market_data_raw'"
            existing = self.db_manager.execute_query(check_query)
            
            if existing.empty:
                self.logger.info(f"📋 Creating new table for {exchange}")
                if not self.create_dynamic_table(exchange, data_path):
                    result['error'] = f"Failed to create table for {exchange}"
                    return result
                self.logger.info(f"✅ Table created successfully for {exchange}")
            else:
                self.logger.info(f"✅ Table already exists for {exchange}")
            
            self.logger.info(f"📍 Step 2/5: Checking for existing data")
            
            # Check if data already exists
            check_existing = f"""
            SELECT COUNT(*) as count 
            FROM {table_name} 
            WHERE data_date = '{data_date}' AND processing_stage = '{stage}'
            """
            existing_data = self.db_manager.execute_query(check_existing)
            
            if existing_data.iloc[0]['count'] > 0:
                if self.idempotent:
                    # In idempotent mode, skip existing data gracefully
                    result['skipped'] = True
                    result['success'] = True
                    result['records_loaded'] = existing_data.iloc[0]['count']
                    self.logger.info(f"⏭️  {exchange} {data_date}: Data already exists ({result['records_loaded']:,} records), skipping")
                    self.stats['files_skipped'] += 1
                    return result
                else:
                    # In non-idempotent mode, treat as error
                    result['error'] = f"Data already exists for {exchange} {data_date} {stage}"
                    self.logger.warning(f"⚠️  {exchange} {data_date}: Data already exists (use --idempotent to skip)")
                    return result
            else:
                self.logger.info(f"✅ No existing data found - proceeding with load")
            
            self.logger.info(f"📍 Step 3/5: Recording progress tracking")
            
            # Generate next ID for progress tracking
            next_id_query = "SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM bronze.load_progress"
            next_id_result = self.db_manager.execute_query(next_id_query)
            next_id = next_id_result.iloc[0]['next_id']
            
            # Record start of processing in DuckDB
            insert_progress_sql = f"""
            INSERT INTO bronze.load_progress (id, exchange, data_date, file_path, start_time, status)
            VALUES ({next_id}, '{exchange}', '{data_date}', '{data_path}', NOW(), 'started')
            """
            self.db_manager.execute_sql(insert_progress_sql)
            
            # Also record in Supabase if available
            supabase_progress_id = None
            if self.supabase_manager:
                supabase_progress_id = self.supabase_manager.insert_progress_record(exchange, data_date, data_path)
            
            # Get the progress ID from DuckDB
            progress_id_query = f"""
            SELECT id FROM bronze.load_progress 
            WHERE exchange = '{exchange}' AND data_date = '{data_date}' AND file_path = '{data_path}'
            ORDER BY created_at DESC LIMIT 1
            """
            progress_result = self.db_manager.execute_query(progress_id_query)
            progress_id = progress_result.iloc[0]['id'] if not progress_result.empty else None
            
            # Store both IDs for proper tracking
            progress_tracking = {
                'duckdb_id': progress_id,
                'supabase_id': supabase_progress_id
            }
            
            # Use Supabase progress ID if available for progress tracking
            if supabase_progress_id:
                progress_id = supabase_progress_id
            
            # Check for shutdown request before starting transaction
            if self.check_shutdown_requested():
                result['error'] = "Shutdown requested before transaction"
                self._update_progress_failed(progress_tracking, result['error'])
                return result
            
            self.logger.info(f"📍 Step 4/5: Loading data from S3")
            self.logger.info(f"📁 Source: {data_path}")
            
            # Start progress logger for long operations
            operation_name = "Data Loading"
            operation_key = f"{exchange}_{data_date}_{operation_name}"
            self._start_progress_logger(operation_name, exchange, data_date)
            
            # Use transaction for non-blocking load
            self.logger.info(f"🔄 Starting transaction for {exchange} {data_date}")
            
            # Begin transaction
            self.db_manager.execute_sql("BEGIN TRANSACTION")
            
            try:
                # Load data with dynamic approach
                self.logger.info(f"📊 Executing INSERT operation - this may take several minutes for large files...")
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
                
                # Execute the insert within transaction
                insert_start = time.time()
                self.db_manager.execute_sql(insert_sql)
                insert_duration = time.time() - insert_start
                
                # Stop progress logger for this specific operation
                self._stop_progress_logger(operation_key)
                self.logger.info(f"✅ INSERT completed in {insert_duration:.1f}s")
                
                # Check for shutdown request before committing
                if self.check_shutdown_requested():
                    self.db_manager.execute_sql("ROLLBACK")
                    result['error'] = "Shutdown requested during transaction"
                    self._update_progress_failed(progress_tracking, result['error'])
                    return result
                
                self.logger.info(f"📍 Step 5/5: Finalizing transaction")
                
                # Get count of loaded records
                self.logger.info(f"🔢 Counting loaded records...")
                count_query = f"""
                SELECT COUNT(*) as count 
                FROM {table_name} 
                WHERE data_date = '{data_date}' AND processing_stage = '{stage}'
                """
                count_result = self.db_manager.execute_query(count_query)
                records_loaded = count_result.iloc[0]['count']
                self.logger.info(f"📊 Records counted: {records_loaded:,}")
                
                # Commit transaction
                self.logger.info(f"💾 Committing transaction...")
                self.db_manager.execute_sql("COMMIT")
                self.logger.info(f"✅ Transaction committed successfully")
                
                end_time = datetime.now()
                processing_time = (end_time - start_time).total_seconds()
                
                result['success'] = True
                result['records_loaded'] = records_loaded
                result['processing_time'] = processing_time
                
                # Update progress as completed
                self._update_progress_completed(progress_tracking, records_loaded, processing_time)
                
                self.logger.info(f"✅ Successfully loaded {records_loaded:,} records for {exchange} {data_date} in {processing_time:.2f}s")
                self.stats['files_loaded'] += 1
                self.stats['total_records'] += records_loaded
                
            except Exception as e:
                # Stop progress logger on error for this specific operation
                self._stop_progress_logger(operation_key)
                # Rollback transaction on error
                self.db_manager.execute_sql("ROLLBACK")
                raise e
                
            return result
            
        except Exception as e:
            result['error'] = str(e)
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            result['processing_time'] = processing_time
            
            # Update progress as failed
            self._update_progress_failed(progress_tracking, str(e))
            
            self.logger.error(f"❌ Failed to load {exchange} {data_date}: {e}")
            self.stats['files_failed'] += 1
            return result
        
        finally:
            self.stats['files_processed'] += 1
    
    def _update_progress_completed(self, progress_tracking: dict, records_loaded: int, processing_time: float):
        """Update progress record as completed"""
        if not progress_tracking:
            self.logger.warning("No progress tracking info available for completion update")
            return
            
        try:
            # Update DuckDB if we have the ID
            duckdb_id = progress_tracking.get('duckdb_id')
            if duckdb_id:
                update_sql = f"""
                UPDATE bronze.load_progress 
                SET end_time = NOW(), 
                    status = 'completed',
                    records_loaded = {records_loaded}
                WHERE id = {duckdb_id}
                """
                self.db_manager.execute_sql(update_sql)
            
            # Update Supabase if available and we have the ID
            supabase_id = progress_tracking.get('supabase_id')
            if self.supabase_manager and supabase_id:
                self.supabase_manager.update_progress_completed(supabase_id, records_loaded)
                
        except Exception as e:
            self.logger.error(f"Failed to update progress: {e}")
    
    def _update_progress_failed(self, progress_tracking: dict, error_message: str):
        """Update progress record as failed"""
        if not progress_tracking:
            self.logger.warning("No progress tracking info available for failure update")
            return
            
        try:
            # Update DuckDB if we have the ID
            duckdb_id = progress_tracking.get('duckdb_id')
            if duckdb_id:
                # Escape single quotes in error message
                escaped_error = error_message.replace("'", "''")
                update_sql = f"""
                UPDATE bronze.load_progress 
                SET end_time = NOW(), 
                    status = 'failed',
                    error_message = '{escaped_error}'
                WHERE id = {duckdb_id}
                """
                self.db_manager.execute_sql(update_sql)
            
            # Update Supabase if available and we have the ID
            supabase_id = progress_tracking.get('supabase_id')
            if self.supabase_manager and supabase_id:
                self.supabase_manager.update_progress_failed(supabase_id, error_message)
                
        except Exception as e:
            self.logger.error(f"Failed to update progress: {e}")
    
    def load_date_range(self, exchange: str, start_date: date, end_date: date, stage: str = 'ingestion') -> list:
        """Load data for an exchange across a date range with graceful shutdown support"""
        results = []
        current_date = start_date
        
        while current_date <= end_date:
            # Check for shutdown request at the start of each iteration
            if self.check_shutdown_requested():
                self.logger.info(f"🛑 Stopping {exchange} processing at {current_date}")
                break
            
            result = self.load_single_file(exchange, current_date, stage)
            results.append(result)
            
            # Update daily statistics after each file (only if successful)
            if result['success'] and not result.get('skipped', False):
                self._update_daily_stats(exchange, current_date)
            
            # Check for shutdown request after processing
            if self.check_shutdown_requested():
                self.logger.info(f"🛑 Stopping {exchange} processing after {current_date}")
                break
            
            # Move to next date
            current_date = datetime.combine(current_date, datetime.min.time()) + pd.Timedelta(days=1)
            current_date = current_date.date()
            
        return results
    
    def load_single_date(self, target_date: date, stage: str = 'ingestion') -> dict:
        """Load data for a single date across all specified exchanges"""
        results = {
            'date': target_date,
            'exchanges': {},
            'total_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'total_records': 0,
            'interrupted': False
        }
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Processing date: {target_date}")
        self.logger.info(f"Exchanges to process: {', '.join(self.exchanges)} ({len(self.exchanges)} total)")
        self.logger.info(f"{'='*60}")
        
        # Process each exchange for this date
        for idx, exchange in enumerate(self.exchanges, 1):
            # Check for shutdown request before processing each exchange
            if self.check_shutdown_requested():
                self.logger.info(f"🛑 Skipping {exchange} for {target_date} due to shutdown request")
                results['interrupted'] = True
                break
            
            self.logger.info(f"\n🏢 Processing {exchange} for {target_date} ({idx}/{len(self.exchanges)})")
            self.logger.info(f"📈 Progress: {((idx-1)/len(self.exchanges)*100):.1f}% complete")
            
            # Load single file for this exchange and date
            exchange_start = time.time()
            result = self.load_single_file(exchange, target_date, stage)
            exchange_duration = time.time() - exchange_start
            results['exchanges'][exchange] = result
            results['total_files'] += 1
            
            # Update counters
            if result['success']:
                if result.get('skipped', False):
                    results['skipped_files'] += 1
                    self.logger.info(f"⏭️  {exchange} completed in {exchange_duration:.1f}s - {result['records_loaded']:,} records already loaded")
                else:
                    results['successful_files'] += 1
                    # Update daily statistics after each successful file
                    self._update_daily_stats(exchange, target_date)
                    self.logger.info(f"✅ {exchange} completed in {exchange_duration:.1f}s - {result['records_loaded']:,} new records loaded")
                
                results['total_records'] += result['records_loaded']
            else:
                results['failed_files'] += 1
                self.logger.info(f"❌ {exchange} failed after {exchange_duration:.1f}s - {result.get('error', 'Unknown error')}")
            
            # Show running totals
            self.logger.info(f"📊 Running totals: {results['total_records']:,} records, {results['successful_files']} successful, {results['failed_files']} failed, {results['skipped_files']} skipped")
            
            # Check for shutdown request after processing
            if self.check_shutdown_requested():
                self.logger.info(f"🛑 Stopping processing after {exchange} for {target_date}")
                results['interrupted'] = True
                break
        
        return results
    
    def _update_daily_stats(self, exchange: str, stats_date: date):
        """Update daily statistics for an exchange"""
        try:
            # Update DuckDB daily stats
            # Generate next ID for daily stats
            next_id_query = "SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM gold.daily_load_stats"
            next_id_result = self.db_manager.execute_query(next_id_query)
            next_id = next_id_result.iloc[0]['next_id']
            
            # Calculate daily stats from progress table
            stats_sql = f"""
            INSERT INTO gold.daily_load_stats (
                id,
                stats_date, 
                exchange, 
                total_files, 
                successful_files, 
                failed_files, 
                total_records,
                avg_records_per_file,
                total_processing_time_seconds
            )
            SELECT 
                {next_id} as id,
                '{stats_date}' as stats_date,
                '{exchange}' as exchange,
                COUNT(*) as total_files,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_files,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_files,
                SUM(COALESCE(records_loaded, 0)) as total_records,
                AVG(COALESCE(records_loaded, 0)) as avg_records_per_file,
                SUM(EXTRACT(EPOCH FROM (COALESCE(end_time, NOW()) - start_time))) as total_processing_time_seconds
            FROM bronze.load_progress
            WHERE exchange = '{exchange}' 
            AND data_date = '{stats_date}'
            ON CONFLICT (stats_date, exchange) DO UPDATE SET
                total_files = EXCLUDED.total_files,
                successful_files = EXCLUDED.successful_files,
                failed_files = EXCLUDED.failed_files,
                total_records = EXCLUDED.total_records,
                avg_records_per_file = EXCLUDED.avg_records_per_file,
                total_processing_time_seconds = EXCLUDED.total_processing_time_seconds,
                created_at = NOW()
            """
            self.db_manager.execute_sql(stats_sql)
            
            # Also update Supabase if available
            if self.supabase_manager:
                self.supabase_manager.upsert_daily_stats(exchange, stats_date)
                self.logger.debug(f"📊 Updated Supabase daily stats for {exchange} {stats_date}")
            
        except Exception as e:
            self.logger.error(f"Failed to update daily stats for {exchange} {stats_date}: {e}")
    
    def _update_weekly_stats(self, exchange: str, week_ending: date):
        """Update weekly rolling statistics for an exchange"""
        try:
            # Update DuckDB weekly stats
            # Generate next ID for weekly stats
            next_id_query = "SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM gold.weekly_load_stats"
            next_id_result = self.db_manager.execute_query(next_id_query)
            next_id = next_id_result.iloc[0]['next_id']
            
            # Calculate 7-day rolling stats
            weekly_sql = f"""
            INSERT INTO gold.weekly_load_stats (
                id,
                week_ending,
                exchange,
                avg_daily_files,
                avg_daily_records,
                total_files,
                total_records,
                avg_processing_time_seconds
            )
            SELECT 
                {next_id} as id,
                '{week_ending}' as week_ending,
                '{exchange}' as exchange,
                AVG(total_files) as avg_daily_files,
                AVG(total_records) as avg_daily_records,
                SUM(total_files) as total_files,
                SUM(total_records) as total_records,
                AVG(total_processing_time_seconds) as avg_processing_time_seconds
            FROM gold.daily_load_stats
            WHERE exchange = '{exchange}' 
            AND stats_date >= DATE('{week_ending}') - INTERVAL '6 days'
            AND stats_date <= '{week_ending}'
            ON CONFLICT (week_ending, exchange) DO UPDATE SET
                avg_daily_files = EXCLUDED.avg_daily_files,
                avg_daily_records = EXCLUDED.avg_daily_records,
                total_files = EXCLUDED.total_files,
                total_records = EXCLUDED.total_records,
                avg_processing_time_seconds = EXCLUDED.avg_processing_time_seconds,
                created_at = NOW()
            """
            self.db_manager.execute_sql(weekly_sql)
            
            # Also update Supabase if available
            if self.supabase_manager:
                self.supabase_manager.upsert_weekly_stats(exchange, week_ending)
                self.logger.debug(f"📊 Updated Supabase weekly stats for {exchange} {week_ending}")
            
        except Exception as e:
            self.logger.error(f"Failed to update weekly stats for {exchange} {week_ending}: {e}")
    
    def generate_final_stats_report(self):
        """Generate comprehensive statistics after loading completion"""
        try:
            # Update weekly stats for all exchanges
            for exchange in self.exchanges:
                self._update_weekly_stats(exchange, date.today())
            
            # Generate summary report
            summary_sql = """
            SELECT 
                d.exchange,
                d.stats_date,
                d.total_files,
                d.successful_files,
                d.failed_files,
                d.total_records,
                ROUND(d.avg_records_per_file, 0) as avg_records_per_file,
                ROUND(d.total_processing_time_seconds, 2) as total_processing_time_seconds,
                ROUND(d.total_records / NULLIF(d.total_processing_time_seconds, 0), 0) as records_per_second
            FROM gold.daily_load_stats d
            ORDER BY d.exchange, d.stats_date
            """
            
            daily_stats = self.db_manager.execute_query(summary_sql)
            
            if not daily_stats.empty:
                self.logger.info("\n" + "="*80)
                self.logger.info("DAILY STATISTICS SUMMARY")
                self.logger.info("="*80)
                for _, row in daily_stats.iterrows():
                    self.logger.info(f"{row['exchange']} - {row['stats_date']}:")
                    self.logger.info(f"  Files: {row['total_files']} total, {row['successful_files']} successful, {row['failed_files']} failed")
                    self.logger.info(f"  Records: {row['total_records']:,} total, {row['avg_records_per_file']:,} avg per file")
                    self.logger.info(f"  Performance: {row['total_processing_time_seconds']}s total, {row['records_per_second']:,} records/sec")
                    self.logger.info("")
            
            # Weekly stats report
            weekly_sql = """
            SELECT 
                exchange,
                week_ending,
                ROUND(avg_daily_files, 1) as avg_daily_files,
                ROUND(avg_daily_records, 0) as avg_daily_records,
                total_files,
                total_records,
                ROUND(avg_processing_time_seconds, 2) as avg_processing_time_seconds
            FROM gold.weekly_load_stats
            ORDER BY exchange, week_ending DESC
            """
            
            weekly_stats = self.db_manager.execute_query(weekly_sql)
            
            if not weekly_stats.empty:
                self.logger.info("\n" + "="*80)
                self.logger.info("WEEKLY ROLLING STATISTICS")
                self.logger.info("="*80)
                for _, row in weekly_stats.iterrows():
                    self.logger.info(f"{row['exchange']} - Week ending {row['week_ending']}:")
                    self.logger.info(f"  Daily averages: {row['avg_daily_files']} files, {row['avg_daily_records']:,} records")
                    self.logger.info(f"  Weekly totals: {row['total_files']} files, {row['total_records']:,} records")
                    self.logger.info(f"  Avg processing time: {row['avg_processing_time_seconds']}s")
                    self.logger.info("")
            
            # Generate Supabase summary report if available
            if self.supabase_manager:
                try:
                    self.logger.info("\n" + "="*80)
                    self.logger.info("SUPABASE STATISTICS SUMMARY")
                    self.logger.info("="*80)
                    
                    # Get progress summary from Supabase
                    progress_summary = self.supabase_manager.get_progress_summary()
                    if not progress_summary.empty:
                        for _, row in progress_summary.iterrows():
                            self.logger.info(f"{row['exchange']} - {row['data_date']}:")
                            self.logger.info(f"  Total files: {row['total_files']}, Completed: {row['completed_files']}, Failed: {row['failed_files']}")
                            self.logger.info(f"  Total records: {row['total_records']:,}")
                            self.logger.info("")
                    
                    self.logger.info("📊 Statistics have been synchronized to Supabase database")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to generate Supabase report: {e}")
                    
        except Exception as e:
            self.logger.error(f"Failed to generate final stats report: {e}")
    
    def print_progress_summary(self):
        """Print current progress summary"""
        self.logger.info(f"\n📊 PROGRESS SUMMARY:")
        self.logger.info(f"   Files processed: {self.stats['files_processed']}")
        self.logger.info(f"   Files loaded: {self.stats['files_loaded']}")
        self.logger.info(f"   Files skipped: {self.stats['files_skipped']}")
        self.logger.info(f"   Files failed: {self.stats['files_failed']}")
        self.logger.info(f"   Total records: {self.stats['total_records']:,}")
        if self.stats['interrupted']:
            self.logger.info(f"   Status: INTERRUPTED")
        else:
            self.logger.info(f"   Status: COMPLETED")
    
    def cleanup(self):
        """Clean up database connections and stop all progress loggers"""
        try:
            # Stop all active progress loggers
            self._stop_progress_logger()
            
            if self.supabase_manager:
                self.supabase_manager.disconnect()
                self.logger.info("🔌 Supabase connection closed")
        except Exception as e:
            self.logger.warning(f"⚠️ Error closing Supabase connection: {e}")

def main():
    """Load all January 2025 data for all exchanges using simple dynamic schema"""
    
    # Setup signal handlers for graceful shutdown
    setup_signal_handlers()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Handle utility commands for NiFi integration
    if args.create_shutdown_file:
        create_shutdown_file()
        print("Use this file to stop the script when running in NiFi")
        return
    
    if args.remove_shutdown_file:
        remove_shutdown_file()
        print("Shutdown file removed - script will no longer be interrupted")
        return
    
    if args.check_shutdown_file:
        if check_shutdown_file():
            print("Shutdown file exists - script will be interrupted")
            sys.exit(1)
        else:
            print("No shutdown file found - script will continue normally")
            sys.exit(0)
    
    # Remove any existing shutdown file at start (clean slate)
    if check_shutdown_file():
        print("🗑️ Removing existing shutdown file for clean start")
        remove_shutdown_file()
    
    # Configure logging based on verbosity
    log_level = logging.DEBUG if args.verbose else logging.INFO
    
    # Ensure logs directory exists
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/january_load_simple_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    # Initialize the loader
    logger.info("Initializing Simple MultiExchangeLoader...")
    if args.idempotent:
        logger.info("🔄 Running in IDEMPOTENT mode - will skip existing data")
    
    logger.info(f"🛑 To stop gracefully: python {sys.argv[0]} --create-shutdown-file")
    
    db_manager = DuckDBManager(database_path="./multi_exchange_data_lake.duckdb")
    loader = SimpleMultiExchangeLoader(db_manager, idempotent=args.idempotent)
    
    # Set the exchanges to process (filter the loader's default exchanges)
    loader.exchanges = args.exchanges
    
    # Define target date and stage
    target_date = args.date
    stage = 'ingestion'
    
    logger.info(f"Loading data for {target_date} for exchanges: {', '.join(args.exchanges)}")
    
    try:
        # Process the single date across all exchanges
        results = loader.load_single_date(target_date, stage)
        
        # Check if processing was interrupted
        if results['interrupted']:
            logger.info("🛑 Processing was interrupted")
        
        # Log detailed results for each exchange
        logger.info(f"\n{'='*60}")
        logger.info(f"DETAILED RESULTS FOR {target_date}")
        logger.info(f"{'='*60}")
        for exchange, result in results['exchanges'].items():
            if result['success']:
                if result.get('skipped', False):
                    logger.info(f"⏭️  {exchange}: {result['records_loaded']:,} records already loaded")
                else:
                    rate = result['records_loaded'] / result['processing_time'] if result['processing_time'] > 0 else 0
                    logger.info(f"✅ {exchange}: {result['records_loaded']:,} records loaded in {result['processing_time']:.2f}s ({rate:,.0f} records/sec)")
            else:
                if "already exists" in str(result.get('error', '')):
                    logger.info(f"⏭️  {exchange}: Data already exists, skipping")
                else:
                    logger.error(f"❌ {exchange}: {result.get('error', 'Unknown error')}")
        
        # Print summary
        logger.info(f"\n📊 PROCESSING SUMMARY FOR {target_date}:")
        logger.info(f"   Total files processed: {results['total_files']}")
        logger.info(f"   Successful loads: {results['successful_files']}")
        logger.info(f"   Skipped loads: {results['skipped_files']}")
        logger.info(f"   Failed loads: {results['failed_files']}")
        logger.info(f"   Total records loaded: {results['total_records']:,}")
        
        if results['interrupted']:
            logger.info(f"   Status: INTERRUPTED")
        else:
            logger.info(f"   Status: COMPLETED")
            
    except Exception as e:
        logger.error(f"Error processing date {target_date}: {e}")
        return
    
    except KeyboardInterrupt:
        logger.info("🛑 Received keyboard interrupt")
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    
    finally:
        # Print loader's internal statistics
        loader.print_progress_summary()
        
        # Generate comprehensive statistics report if we completed normally
        if not shutdown_requested:
            logger.info("Generating final statistics report...")
            loader.generate_final_stats_report()
        
        # Clean up database connections
        loader.cleanup()
        
        logger.info(f"\n📊 Database queries to check status:")
        logger.info(f"   DuckDB Progress: SELECT * FROM bronze.load_progress WHERE data_date = '{target_date}' ORDER BY start_time DESC")
        logger.info(f"   DuckDB Daily stats: SELECT * FROM gold.daily_load_stats WHERE stats_date = '{target_date}' ORDER BY exchange")
        logger.info(f"   DuckDB Weekly stats: SELECT * FROM gold.weekly_load_stats ORDER BY week_ending DESC, exchange")
        logger.info(f"   Supabase Progress: SELECT * FROM bronze.load_progress WHERE data_date = '{target_date}' ORDER BY start_time DESC")
        logger.info(f"   Supabase Daily stats: SELECT * FROM gold.daily_load_stats WHERE stats_date = '{target_date}' ORDER BY exchange")
        
        if shutdown_requested or check_shutdown_file():
            logger.info(f"\n🔄 To resume: python {sys.argv[0]} --date {target_date} --idempotent")
            logger.info(f"🛑 To stop via file: python {sys.argv[0]} --create-shutdown-file")
            sys.exit(130)  # Exit code for script terminated by Ctrl+C
        else:
            # Clean up shutdown file on successful completion
            remove_shutdown_file()  # Remove regardless of whether it exists
            logger.info("✅ Script completed successfully - statistics saved to both DuckDB and Supabase")

if __name__ == "__main__":
    main() 