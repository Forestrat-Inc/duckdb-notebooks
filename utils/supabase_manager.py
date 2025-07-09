"""
Supabase Manager for Market Data Loading Statistics

This module provides a PostgreSQL/Supabase interface for storing and managing
market data loading statistics and progress tracking.
"""

import logging
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import os
from datetime import date, datetime
from typing import Optional, Dict, Any, List, Union
import json


class SupabaseManager:
    """
    Manager class for Supabase/PostgreSQL operations
    """
    
    def __init__(self, 
                 host: str = None,
                 port: int = None,
                 database: str = None, 
                 user: str = None,
                 password: str = None):
        """Initialize Supabase connection"""
        # Use environment variables if parameters not provided
        self.host = host or os.getenv('SUPABASE_HOST')
        self.port = port or int(os.getenv('SUPABASE_PORT', '6543'))
        self.database = database or os.getenv('SUPABASE_DATABASE', 'postgres')
        self.user = user or os.getenv('SUPABASE_USER')
        self.password = password or os.getenv('SUPABASE_PASSWORD')
        
        # Validate required credentials
        if not all([self.host, self.user, self.password]):
            raise ValueError(
                "Supabase credentials not provided. Please set environment variables: "
                "SUPABASE_HOST, SUPABASE_USER, SUPABASE_PASSWORD"
            )
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
        # Connect to database
        self.connect()
        
        # Initialize schemas and tables
        self.initialize_schemas()
    
    def connect(self) -> bool:
        """Establish connection to Supabase"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                sslmode='require'
            )
            self.connection.autocommit = False  # We'll handle transactions manually
            self.logger.info("✅ Connected to Supabase successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Supabase: {e}")
            return False
    
    def disconnect(self):
        """Close Supabase connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Disconnected from Supabase")
    
    def _convert_numpy_types(self, value: Any) -> Any:
        """Convert numpy types to native Python types for PostgreSQL compatibility"""
        if isinstance(value, (np.integer, np.int64, np.int32)):
            return int(value)
        elif isinstance(value, (np.floating, np.float64, np.float32)):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif isinstance(value, np.ndarray):
            return value.tolist()
        else:
            return value
    
    def execute_sql(self, sql: str, params: Optional[tuple] = None) -> bool:
        """Execute SQL statement"""
        try:
            # Check connection status and reconnect if needed
            if self.connection.closed:
                self.logger.warning("Connection closed, attempting to reconnect...")
                if not self.connect():
                    self.logger.error("Failed to reconnect to Supabase")
                    return False
            
            # Convert numpy types to Python types
            if params:
                params = tuple(self._convert_numpy_types(param) for param in params)
            
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                self.connection.commit()
                return True
        except Exception as e:
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            self.logger.error(f"SQL execution failed: {e}")
            self.logger.error(f"SQL: {sql}")
            if params:
                self.logger.error(f"Params: {params} (types: {[type(p).__name__ for p in params]})")
            return False
    
    def execute_query(self, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        try:
            # Check connection status and reconnect if needed
            if self.connection.closed:
                self.logger.warning("Connection closed, attempting to reconnect...")
                if not self.connect():
                    self.logger.error("Failed to reconnect to Supabase")
                    return pd.DataFrame()
            
            # Convert numpy types to Python types
            if params:
                params = tuple(self._convert_numpy_types(param) for param in params)
            
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(sql, params)
                results = cursor.fetchall()
                return pd.DataFrame(results)
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            self.logger.error(f"SQL: {sql}")
            if params:
                self.logger.error(f"Params: {params} (types: {[type(p).__name__ for p in params]})")
            return pd.DataFrame()
    
    def initialize_schemas(self):
        """Create necessary schemas and tables"""
        try:
            # Create schemas
            schemas = [
                "CREATE SCHEMA IF NOT EXISTS bronze",
                "CREATE SCHEMA IF NOT EXISTS silver", 
                "CREATE SCHEMA IF NOT EXISTS gold"
            ]
            
            for schema_sql in schemas:
                self.execute_sql(schema_sql)
            
            # Create tables
            self._create_progress_table()
            self._create_daily_stats_table()
            self._create_weekly_stats_table()
            
            self.logger.info("✅ Supabase schemas and tables initialized")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize Supabase schemas: {e}")
    
    def _create_progress_table(self):
        """Create load progress tracking table"""
        sql = """
        CREATE TABLE IF NOT EXISTS bronze.load_progress (
            id SERIAL PRIMARY KEY,
            exchange VARCHAR(50) NOT NULL,
            data_date DATE NOT NULL,
            file_path TEXT NOT NULL,
            start_time TIMESTAMP DEFAULT NOW(),
            end_time TIMESTAMP,
            status VARCHAR(20) DEFAULT 'started',
            records_loaded INTEGER DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
        self.execute_sql(sql)
        
        # Create index for better performance
        index_sql = "CREATE INDEX IF NOT EXISTS idx_load_progress_date_exchange ON bronze.load_progress(data_date, exchange)"
        self.execute_sql(index_sql)
    
    def _create_daily_stats_table(self):
        """Create daily statistics table"""
        sql = """
        CREATE TABLE IF NOT EXISTS gold.daily_load_stats (
            id SERIAL PRIMARY KEY,
            stats_date DATE NOT NULL,
            exchange VARCHAR(50) NOT NULL,
            total_files INTEGER DEFAULT 0,
            successful_files INTEGER DEFAULT 0,
            failed_files INTEGER DEFAULT 0,
            total_records BIGINT DEFAULT 0,
                            avg_records_per_file DECIMAL(15,2),
            total_processing_time_seconds DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(stats_date, exchange)
        )
        """
        self.execute_sql(sql)
        
        # Create indexes
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON gold.daily_load_stats(stats_date)",
            "CREATE INDEX IF NOT EXISTS idx_daily_stats_exchange ON gold.daily_load_stats(exchange)"
        ]
        for index_sql in indexes:
            self.execute_sql(index_sql)
    
    def _create_weekly_stats_table(self):
        """Create weekly statistics table"""
        sql = """
        CREATE TABLE IF NOT EXISTS gold.weekly_load_stats (
            id SERIAL PRIMARY KEY,
            week_ending DATE NOT NULL,
            exchange VARCHAR(50) NOT NULL,
            avg_daily_files DECIMAL(10,2),
            avg_daily_records DECIMAL(15,2),
            total_files INTEGER DEFAULT 0,
            total_records BIGINT DEFAULT 0,
            avg_processing_time_seconds DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(week_ending, exchange)
        )
        """
        self.execute_sql(sql)
        
        # Create index
        index_sql = "CREATE INDEX IF NOT EXISTS idx_weekly_stats_week_exchange ON gold.weekly_load_stats(week_ending, exchange)"
        self.execute_sql(index_sql)
    
    def insert_progress_record(self, exchange: str, data_date: date, file_path: str) -> Optional[int]:
        """Insert new progress record and return its ID"""
        try:
            # Check connection status and reconnect if needed
            if self.connection.closed:
                self.logger.warning("Connection closed, attempting to reconnect...")
                if not self.connect():
                    self.logger.error("Failed to reconnect to Supabase")
                    return None
            
            sql = """
            INSERT INTO bronze.load_progress (exchange, data_date, file_path, start_time, status)
            VALUES (%s, %s, %s, NOW(), 'started')
            RETURNING id
            """
            
            # Convert numpy types to Python types
            params = (
                self._convert_numpy_types(exchange),
                self._convert_numpy_types(data_date),
                self._convert_numpy_types(file_path)
            )
            
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                progress_id = cursor.fetchone()[0]
                self.connection.commit()
                return progress_id
                
        except Exception as e:
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            self.logger.error(f"Failed to insert progress record: {e}")
            return None
    
    def update_progress_completed(self, progress_id: int, records_loaded: int):
        """Update progress record as completed"""
        try:
            # Convert numpy types to Python types explicitly
            progress_id = self._convert_numpy_types(progress_id)
            records_loaded = self._convert_numpy_types(records_loaded)
            
            sql = """
            UPDATE bronze.load_progress 
            SET end_time = NOW(), 
                status = 'completed',
                records_loaded = %s
            WHERE id = %s
            """
            self.execute_sql(sql, (records_loaded, progress_id))
            
        except Exception as e:
            self.logger.error(f"Failed to update progress as completed: {e}")
            self.logger.error(f"progress_id type: {type(progress_id)}, records_loaded type: {type(records_loaded)}")
    
    def update_progress_failed(self, progress_id: int, error_message: str):
        """Update progress record as failed"""
        try:
            # Convert numpy types to Python types explicitly
            progress_id = self._convert_numpy_types(progress_id)
            
            # Truncate error message if too long
            error_message = error_message[:1000] if len(error_message) > 1000 else error_message
            
            sql = """
            UPDATE bronze.load_progress 
            SET end_time = NOW(), 
                status = 'failed',
                error_message = %s
            WHERE id = %s
            """
            self.execute_sql(sql, (error_message, progress_id))
            
        except Exception as e:
            self.logger.error(f"Failed to update progress as failed: {e}")
            self.logger.error(f"progress_id type: {type(progress_id)}")
    
    def upsert_daily_stats(self, exchange: str, stats_date: date):
        """Update/insert daily statistics for an exchange"""
        try:
            sql = """
            INSERT INTO gold.daily_load_stats (
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
                %s as stats_date,
                %s as exchange,
                COUNT(*) as total_files,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_files,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_files,
                SUM(COALESCE(records_loaded, 0)) as total_records,
                AVG(COALESCE(records_loaded, 0)) as avg_records_per_file,
                SUM(EXTRACT(EPOCH FROM (COALESCE(end_time, NOW()) - start_time))) as total_processing_time_seconds
            FROM bronze.load_progress
            WHERE exchange = %s 
            AND data_date = %s
            ON CONFLICT (stats_date, exchange) DO UPDATE SET
                total_files = EXCLUDED.total_files,
                successful_files = EXCLUDED.successful_files,
                failed_files = EXCLUDED.failed_files,
                total_records = EXCLUDED.total_records,
                avg_records_per_file = EXCLUDED.avg_records_per_file,
                total_processing_time_seconds = EXCLUDED.total_processing_time_seconds,
                created_at = NOW()
            """
            
            self.execute_sql(sql, (stats_date, exchange, exchange, stats_date))
            
        except Exception as e:
            self.logger.error(f"Failed to upsert daily stats for {exchange} {stats_date}: {e}")
    
    def upsert_weekly_stats(self, exchange: str, week_ending: date):
        """Update/insert weekly rolling statistics for an exchange"""
        try:
            sql = """
            INSERT INTO gold.weekly_load_stats (
                week_ending,
                exchange,
                avg_daily_files,
                avg_daily_records,
                total_files,
                total_records,
                avg_processing_time_seconds
            )
            SELECT 
                %s as week_ending,
                %s as exchange,
                AVG(total_files) as avg_daily_files,
                AVG(total_records) as avg_daily_records,
                SUM(total_files) as total_files,
                SUM(total_records) as total_records,
                AVG(total_processing_time_seconds) as avg_processing_time_seconds
            FROM gold.daily_load_stats
            WHERE exchange = %s 
            AND stats_date >= %s - INTERVAL '6 days'
            AND stats_date <= %s
            ON CONFLICT (week_ending, exchange) DO UPDATE SET
                avg_daily_files = EXCLUDED.avg_daily_files,
                avg_daily_records = EXCLUDED.avg_daily_records,
                total_files = EXCLUDED.total_files,
                total_records = EXCLUDED.total_records,
                avg_processing_time_seconds = EXCLUDED.avg_processing_time_seconds,
                created_at = NOW()
            """
            
            self.execute_sql(sql, (week_ending, exchange, exchange, week_ending, week_ending))
            
        except Exception as e:
            self.logger.error(f"Failed to upsert weekly stats for {exchange} {week_ending}: {e}")
    
    def get_daily_stats(self, exchange: Optional[str] = None, stats_date: Optional[date] = None) -> pd.DataFrame:
        """Get daily statistics"""
        sql = "SELECT * FROM gold.daily_load_stats WHERE 1=1"
        params = []
        
        if exchange:
            sql += " AND exchange = %s"
            params.append(exchange)
        
        if stats_date:
            sql += " AND stats_date = %s"
            params.append(stats_date)
        
        sql += " ORDER BY stats_date DESC, exchange"
        
        return self.execute_query(sql, params)
    
    def get_weekly_stats(self, exchange: Optional[str] = None) -> pd.DataFrame:
        """Get weekly statistics"""
        sql = "SELECT * FROM gold.weekly_load_stats WHERE 1=1"
        params = []
        
        if exchange:
            sql += " AND exchange = %s"
            params.append(exchange)
        
        sql += " ORDER BY week_ending DESC, exchange"
        
        return self.execute_query(sql, params)
    
    def get_progress_summary(self, data_date: Optional[date] = None) -> pd.DataFrame:
        """Get progress summary"""
        sql = """
        SELECT 
            exchange,
            data_date,
            COUNT(*) as total_files,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_files,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_files,
            COUNT(CASE WHEN status = 'started' THEN 1 END) as running_files,
            SUM(COALESCE(records_loaded, 0)) as total_records
        FROM bronze.load_progress
        WHERE 1=1
        """
        params = []
        
        if data_date:
            sql += " AND data_date = %s"
            params.append(data_date)
        
        sql += " GROUP BY exchange, data_date ORDER BY data_date DESC, exchange"
        
        return self.execute_query(sql, params)
    
    def cleanup_old_records(self, days_to_keep: int = 90):
        """Clean up old progress records (keep only recent data)"""
        try:
            sql = """
            DELETE FROM bronze.load_progress 
            WHERE created_at < NOW() - INTERVAL '%s days'
            """
            self.execute_sql(sql % days_to_keep)
            self.logger.info(f"Cleaned up progress records older than {days_to_keep} days")
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old records: {e}")


# Factory function for easy initialization
def create_supabase_manager() -> SupabaseManager:
    """Create a Supabase manager with default settings"""
    return SupabaseManager()


# Test function
def test_connection():
    """Test Supabase connection"""
    try:
        manager = create_supabase_manager()
        
        # Test basic query
        result = manager.execute_query("SELECT NOW() as current_time")
        print(f"✅ Supabase connection test successful: {result.iloc[0]['current_time']}")
        
        manager.disconnect()
        return True
        
    except Exception as e:
        print(f"❌ Supabase connection test failed: {e}")
        return False


if __name__ == "__main__":
    # Run connection test
    test_connection() 