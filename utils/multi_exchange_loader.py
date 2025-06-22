"""
Multi-Exchange Data Loader for DuckDB Data Lake

This module handles loading data from the updated S3 structure which supports
multiple exchanges (LSE, CME, NYQ) and multiple processing stages.
"""

import pandas as pd
import duckdb
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime, date
import sys
import os

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from config import config
from utils.database import DuckDBManager

class MultiExchangeLoader:
    """
    Handles loading data from multiple exchanges with proper partitioning
    """
    
    def __init__(self, db_manager: DuckDBManager = None):
        """Initialize the loader with database connection"""
        self.db_manager = db_manager or DuckDBManager()
        self.logger = logging.getLogger(__name__)
        
        # Supported exchanges and their configurations
        self.exchanges = {
            'LSE': {
                'stages': ['extraction', 'ingestion', 'normalization', 'transformation'],
                'table': 'bronze.lse_market_data_raw',
                'timezone': 'Europe/London'
            },
            'CME': {
                'stages': ['ingestion'],
                'table': 'bronze.cme_market_data_raw',
                'timezone': 'America/Chicago'
            },
            'NYQ': {
                'stages': ['ingestion'], 
                'table': 'bronze.nyq_market_data_raw',
                'timezone': 'America/New_York'
            }
        }
    
    def get_s3_path(self, exchange: str, data_date: date, stage: str = 'ingestion', file_type: str = 'Data') -> str:
        """
        Generate S3 path for a specific exchange, date, and processing stage
        
        Args:
            exchange: Exchange name (LSE, CME, NYQ)
            data_date: Date of the data
            stage: Processing stage (ingestion, normalization, etc.)
            file_type: Data or Report
            
        Returns:
            Full S3 path to the file
        """
        date_str = data_date.isoformat()
        return (f"s3://vendor-data-s3/LSEG/TRTH/{exchange.upper()}/{stage}/"
                f"{date_str}/data/merged/{exchange.upper()}-{date_str}-NORMALIZEDMP-{file_type}-1-of-1.csv.gz")
    
    def list_available_files(self, exchange: str, stage: str = 'ingestion', 
                           start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[Dict]:
        """
        List available files for an exchange and stage within a date range
        
        Args:
            exchange: Exchange name
            stage: Processing stage
            start_date: Start date (optional)
            end_date: End date (optional)
            
        Returns:
            List of available files with metadata
        """
        files = []
        
        # Use SQL to scan S3 directory structure
        path_pattern = f"s3://vendor-data-s3/LSEG/TRTH/{exchange.upper()}/{stage}/*/data/merged/*.csv.gz"
        
        try:
            # Get list of files from S3
            query = f"""
            SELECT 
                filename,
                file_size,
                file_last_modified
            FROM glob('{path_pattern}')
            ORDER BY filename
            """
            
            result = self.db_manager.execute_query(query)
            
            for _, row in result.iterrows():
                # Extract date from filename
                filename = row['filename']
                parts = filename.split('/')
                if len(parts) >= 6:
                    date_part = parts[5]  # Date should be in position 5
                    try:
                        file_date = datetime.fromisoformat(date_part).date()
                        
                        # Filter by date range if specified
                        if start_date and file_date < start_date:
                            continue
                        if end_date and file_date > end_date:
                            continue
                            
                        files.append({
                            'exchange': exchange,
                            'stage': stage,
                            'date': file_date,
                            'filename': filename,
                            'size': row['file_size'],
                            'modified': row['file_last_modified']
                        })
                    except ValueError:
                        self.logger.warning(f"Could not parse date from filename: {filename}")
                        
        except Exception as e:
            self.logger.error(f"Error listing files for {exchange}/{stage}: {e}")
            
        return files
    
    def load_single_file(self, exchange: str, data_date: date, stage: str = 'ingestion', 
                        force_reload: bool = False) -> Dict:
        """
        Load a single file for a specific exchange and date
        
        Args:
            exchange: Exchange name
            data_date: Date of the data
            stage: Processing stage
            force_reload: Whether to reload if data already exists
            
        Returns:
            Dictionary with loading results
        """
        result = {
            'exchange': exchange,
            'date': data_date,
            'stage': stage,
            'success': False,
            'records_loaded': 0,
            'error': None
        }
        
        try:
            # Check if data already exists
            if not force_reload:
                check_query = f"""
                SELECT COUNT(*) as count 
                FROM {self.exchanges[exchange]['table']} 
                WHERE data_date = '{data_date}' AND processing_stage = '{stage}'
                """
                existing = self.db_manager.execute_query(check_query)
                if existing.iloc[0]['count'] > 0:
                    result['error'] = f"Data already exists for {exchange} {data_date} {stage}"
                    return result
            
            # Get S3 paths
            data_path = self.get_s3_path(exchange, data_date, stage, 'Data')
            report_path = self.get_s3_path(exchange, data_date, stage, 'Report')
            
            # Load main data file
            data_query = f"""
            INSERT INTO {self.exchanges[exchange]['table']}
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
            self.db_manager.execute_sql(data_query)
            
            # Get count of loaded records
            count_query = f"""
            SELECT COUNT(*) as count 
            FROM {self.exchanges[exchange]['table']} 
            WHERE data_date = '{data_date}' AND processing_stage = '{stage}'
            """
            count_result = self.db_manager.execute_query(count_query)
            records_loaded = count_result.iloc[0]['count']
            
            # Load report file if it exists
            try:
                report_query = f"""
                INSERT INTO bronze.market_data_reports
                SELECT 
                    gen_random_uuid() as report_id,
                    '{exchange}' as exchange,
                    '{data_date}'::DATE as data_date,
                    'NORMALIZEDMP' as report_type,
                    *,
                    '{stage}' as processing_stage,
                    NOW() as created_at
                FROM read_csv('{report_path}', 
                             AUTO_DETECT=true,
                             FILENAME=false,
                             IGNORE_ERRORS=true)
                """
                self.db_manager.execute_sql(report_query)
                self.logger.info(f"Loaded report file for {exchange} {data_date}")
            except Exception as report_error:
                self.logger.warning(f"Could not load report file: {report_error}")
            
            # Log the ingestion
            log_query = f"""
            INSERT INTO audit.data_ingestion_log 
            (exchange, processing_stage, source_path, record_count, data_date, processing_status)
            VALUES ('{exchange}', '{stage}', '{data_path}', {records_loaded}, '{data_date}', 'bronze')
            """
            self.db_manager.execute_sql(log_query)
            
            result['success'] = True
            result['records_loaded'] = records_loaded
            
            self.logger.info(f"Successfully loaded {records_loaded} records for {exchange} {data_date} {stage}")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"Error loading {exchange} {data_date} {stage}: {e}")
            
        return result
    
    def load_date_range(self, exchange: str, start_date: date, end_date: date, 
                       stage: str = 'ingestion', force_reload: bool = False) -> List[Dict]:
        """
        Load data for an exchange across a date range
        
        Args:
            exchange: Exchange name
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            stage: Processing stage
            force_reload: Whether to reload existing data
            
        Returns:
            List of loading results for each date
        """
        results = []
        current_date = start_date
        
        while current_date <= end_date:
            result = self.load_single_file(exchange, current_date, stage, force_reload)
            results.append(result)
            
            # Move to next date
            current_date = datetime.combine(current_date, datetime.min.time()) + pd.Timedelta(days=1)
            current_date = current_date.date()
            
        return results
    
    def load_all_exchanges(self, data_date: date, stage: str = 'ingestion', 
                          force_reload: bool = False) -> Dict[str, Dict]:
        """
        Load data for all exchanges for a specific date
        
        Args:
            data_date: Date to load
            stage: Processing stage
            force_reload: Whether to reload existing data
            
        Returns:
            Dictionary with results for each exchange
        """
        results = {}
        
        for exchange in self.exchanges.keys():
            # Check if the exchange supports this stage
            if stage in self.exchanges[exchange]['stages']:
                result = self.load_single_file(exchange, data_date, stage, force_reload)
                results[exchange] = result
            else:
                results[exchange] = {
                    'exchange': exchange,
                    'date': data_date,
                    'stage': stage,
                    'success': False,
                    'records_loaded': 0,
                    'error': f"Stage '{stage}' not supported for {exchange}"
                }
                
        return results
    
    def get_loading_status(self, exchange: Optional[str] = None, 
                          days_back: int = 30) -> pd.DataFrame:
        """
        Get loading status for exchanges
        
        Args:
            exchange: Specific exchange (optional)
            days_back: Number of days to look back
            
        Returns:
            DataFrame with loading status
        """
        where_clause = ""
        if exchange:
            where_clause = f"WHERE exchange = '{exchange}'"
        
        query = f"""
        SELECT 
            exchange,
            processing_stage,
            data_date,
            record_count,
            processing_status,
            ingestion_timestamp,
            CURRENT_DATE - data_date as days_ago
        FROM audit.data_ingestion_log
        {where_clause}
        WHERE data_date >= CURRENT_DATE - INTERVAL '{days_back} days'
        ORDER BY data_date DESC, exchange, processing_stage
        """
        
        return self.db_manager.execute_query(query)
    
    def update_pipeline_state(self, exchange: str, stage: str, last_processed_date: date, 
                            records_processed: int, status: str = 'completed'):
        """
        Update pipeline state tracking
        
        Args:
            exchange: Exchange name
            stage: Processing stage
            last_processed_date: Last date processed
            records_processed: Number of records processed
            status: Processing status
        """
        pipeline_name = f"{exchange.lower()}_market_data"
        
        query = f"""
        INSERT INTO audit.pipeline_state 
        (pipeline_name, exchange, processing_stage, last_processed_date, 
         processing_status, records_processed, last_updated)
        VALUES ('{pipeline_name}', '{exchange}', '{stage}', '{last_processed_date}', 
                '{status}', {records_processed}, NOW())
        ON CONFLICT (pipeline_name, exchange, processing_stage) 
        DO UPDATE SET
            last_processed_date = EXCLUDED.last_processed_date,
            processing_status = EXCLUDED.processing_status,
            records_processed = EXCLUDED.records_processed,
            last_updated = NOW()
        """
        
        self.db_manager.execute_sql(query)
    
    def discover_file_schema(self, exchange: str, data_date: date, stage: str = 'ingestion') -> pd.DataFrame:
        """
        Discover the schema of a file without loading it
        
        Args:
            exchange: Exchange name
            data_date: Date of the data
            stage: Processing stage
            
        Returns:
            DataFrame with column information
        """
        data_path = self.get_s3_path(exchange, data_date, stage, 'Data')
        
        query = f"""
        DESCRIBE (
            SELECT * FROM read_csv('{data_path}', 
                                  AUTO_DETECT=true,
                                  SAMPLE_SIZE=10000) 
            LIMIT 0
        )
        """
        
        return self.db_manager.execute_query(query)
    
    def validate_data_quality(self, exchange: str, data_date: date, stage: str = 'ingestion') -> Dict:
        """
        Validate data quality for a specific exchange and date
        
        Args:
            exchange: Exchange name
            data_date: Date of the data
            stage: Processing stage
            
        Returns:
            Dictionary with quality metrics
        """
        table_name = self.exchanges[exchange]['table']
        
        query = f"""
        WITH quality_check AS (
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN price IS NULL THEN 1 END) as null_price_count,
                COUNT(CASE WHEN volume IS NULL THEN 1 END) as null_volume_count,
                COUNT(CASE WHEN symbol IS NULL OR symbol = '' THEN 1 END) as null_symbol_count,
                COUNT(CASE WHEN price <= 0 THEN 1 END) as invalid_price_count,
                COUNT(CASE WHEN volume < 0 THEN 1 END) as invalid_volume_count,
                COUNT(*) - COUNT(DISTINCT *) as duplicate_count
            FROM {table_name}
            WHERE data_date = '{data_date}' AND processing_stage = '{stage}'
        )
        SELECT 
            *,
            ROUND(100.0 * (total_records - null_price_count - null_volume_count - null_symbol_count) / NULLIF(total_records, 0), 2) as completeness_score,
            ROUND(100.0 * (total_records - invalid_price_count - invalid_volume_count) / NULLIF(total_records, 0), 2) as accuracy_score,
            ROUND(100.0 * (total_records - duplicate_count) / NULLIF(total_records, 0), 2) as consistency_score
        FROM quality_check
        """
        
        result = self.db_manager.execute_query(query)
        
        if not result.empty:
            quality_dict = result.iloc[0].to_dict()
            
            # Calculate overall quality score
            completeness = quality_dict.get('completeness_score', 0)
            accuracy = quality_dict.get('accuracy_score', 0)
            consistency = quality_dict.get('consistency_score', 0)
            overall_score = (completeness + accuracy + consistency) / 3
            quality_dict['overall_quality_score'] = round(overall_score, 2)
            
            # Log quality metrics
            log_query = f"""
            INSERT INTO audit.data_quality_metrics 
            (exchange, table_name, data_date, total_records, valid_records, invalid_records,
             null_price_count, null_volume_count, duplicate_count, anomaly_count,
             completeness_score, accuracy_score, consistency_score, overall_quality_score)
            VALUES ('{exchange}', '{table_name}', '{data_date}', 
                    {quality_dict['total_records']}, 
                    {quality_dict['total_records'] - quality_dict['invalid_price_count'] - quality_dict['invalid_volume_count']},
                    {quality_dict['invalid_price_count'] + quality_dict['invalid_volume_count']},
                    {quality_dict['null_price_count']}, {quality_dict['null_volume_count']}, 
                    {quality_dict['duplicate_count']}, 0,
                    {quality_dict['completeness_score']}, {quality_dict['accuracy_score']}, 
                    {quality_dict['consistency_score']}, {overall_score})
            """
            self.db_manager.execute_sql(log_query)
            
            return quality_dict
        
        return {}


def main():
    """Example usage of the MultiExchangeLoader"""
    from datetime import date, timedelta
    
    # Initialize loader
    loader = MultiExchangeLoader()
    
    # Example: Load recent data for all exchanges
    recent_date = date.today() - timedelta(days=1)
    
    print(f"Loading data for {recent_date}")
    results = loader.load_all_exchanges(recent_date)
    
    for exchange, result in results.items():
        if result['success']:
            print(f"✅ {exchange}: {result['records_loaded']} records")
        else:
            print(f"❌ {exchange}: {result['error']}")
    
    # Check loading status
    status = loader.get_loading_status(days_back=7)
    print(f"\nRecent loading status:")
    print(status)


if __name__ == "__main__":
    main() 