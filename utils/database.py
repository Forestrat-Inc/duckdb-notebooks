import duckdb
import pandas as pd
import boto3
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path
import os
import sys

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import config

class DuckDBManager:
    """Manager class for DuckDB operations with S3 integration"""
    
    def __init__(self, database_path: Optional[str] = None):
        self.database_path = database_path or config.DUCKDB_DATABASE_PATH
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(self.database_path), exist_ok=True)
        
    def connect(self) -> duckdb.DuckDBPyConnection:
        """Create and configure DuckDB connection"""
        if self.connection is None:
            self.connection = duckdb.connect(self.database_path)
            self._configure_connection()
            
        return self.connection
    
    def _configure_connection(self):
        """Configure DuckDB connection with S3 and performance settings"""
        conn = self.connection
        
        # Install and load required extensions
        conn.execute("INSTALL httpfs")
        conn.execute("INSTALL aws") 
        conn.execute("LOAD httpfs")
        conn.execute("LOAD aws")
        
        # Configure memory and performance settings
        conn.execute(f"SET memory_limit='{config.DUCKDB_MEMORY_LIMIT}'")
        conn.execute(f"SET threads={config.DUCKDB_THREADS}")
        
        # Try to enable progress bar if supported (optional feature)
        try:
            conn.execute("SET enable_progress_bar=1")
        except Exception:
            # Progress bar setting not supported in this DuckDB version, skip it
            pass
        
        # Configure S3 credentials
        self._configure_s3_credentials(conn)
        
        self.logger.info("DuckDB connection configured successfully")
    
    def _configure_s3_credentials(self, conn):
        """Configure S3 credentials for DuckDB"""
        if config.AWS_PROFILE:
            # Use AWS profile
            conn.execute(f"""
                CREATE OR REPLACE SECRET s3_secret (
                    TYPE s3,
                    PROVIDER credential_chain,
                    CHAIN 'config',
                    PROFILE '{config.AWS_PROFILE}'
                )
            """)
        elif config.AWS_ACCESS_KEY_ID and config.AWS_SECRET_ACCESS_KEY:
            # Use explicit credentials
            conn.execute(f"""
                CREATE OR REPLACE SECRET s3_secret (
                    TYPE s3,
                    KEY_ID '{config.AWS_ACCESS_KEY_ID}',
                    SECRET '{config.AWS_SECRET_ACCESS_KEY}',
                    REGION '{config.AWS_DEFAULT_REGION}'
                )
            """)
        else:
            # Use credential chain (environment variables, IAM roles, etc.)
            conn.execute("""
                CREATE OR REPLACE SECRET s3_secret (
                    TYPE s3,
                    PROVIDER credential_chain
                )
            """)
        
        self.logger.info("S3 credentials configured")
    
    def test_s3_connection(self) -> bool:
        """Test S3 connection by listing files"""
        try:
            conn = self.connect()
            result = conn.execute(f"""
                SELECT filename, count(*) as record_count
                FROM read_csv('{config.INGESTION_PATH}/*/*.csv.gz', 
                             AUTO_DETECT=true, 
                             FILENAME=true,
                             SAMPLE_SIZE=100) 
                GROUP BY filename
                LIMIT 5
            """).fetchall()
            
            self.logger.info(f"S3 connection test successful. Found {len(result)} files")
            for file_info in result:
                self.logger.info(f"  File: {file_info[0]}, Records: {file_info[1]}")
            return True
            
        except Exception as e:
            self.logger.error(f"S3 connection test failed: {e}")
            return False
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return pandas DataFrame"""
        try:
            conn = self.connect()
            result = conn.execute(query).df()
            self.logger.info(f"Query executed successfully. Returned {len(result)} rows")
            return result
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_sql(self, sql: str) -> Any:
        """Execute SQL statement (non-query)"""
        try:
            conn = self.connect()
            result = conn.execute(sql)
            self.logger.info("SQL statement executed successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"SQL execution failed: {e}")
            raise
    
    def create_table_from_s3(self, table_name: str, s3_path: str, **kwargs) -> bool:
        """Create table from S3 CSV files"""
        try:
            conn = self.connect()
            
            # Generate CREATE TABLE statement
            sql = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv('{s3_path}', 
                                     AUTO_DETECT=true,
                                     UNION_BY_NAME=true,
                                     FILENAME=true,
                                     {', '.join([f"{k}={v}" for k, v in kwargs.items()])})
            """
            
            conn.execute(sql)
            self.logger.info(f"Table {table_name} created from {s3_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def export_to_s3(self, table_name: str, s3_path: str, 
                     format: str = 'parquet', **kwargs) -> bool:
        """Export table to S3"""
        try:
            conn = self.connect()
            
            copy_options = ', '.join([f"{k} {v}" for k, v in kwargs.items()])
            if copy_options:
                copy_options = f"({copy_options})"
            
            sql = f"""
                COPY {table_name} TO '{s3_path}' 
                (FORMAT {format} {copy_options})
            """
            
            conn.execute(sql)
            self.logger.info(f"Table {table_name} exported to {s3_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to export table {table_name}: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """Get table schema information"""
        return self.execute_query(f"DESCRIBE {table_name}")
    
    def list_tables(self) -> pd.DataFrame:
        """List all tables in the database"""
        return self.execute_query("SHOW TABLES")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Database connection closed")

class S3Manager:
    """Manager class for direct S3 operations"""
    
    def __init__(self):
        self.s3_client = self._create_s3_client()
        self.logger = logging.getLogger(__name__)
    
    def _create_s3_client(self):
        """Create S3 client with appropriate credentials"""
        session = boto3.Session(
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            region_name=config.AWS_DEFAULT_REGION,
            profile_name=config.AWS_PROFILE
        )
        
        return session.client('s3', endpoint_url=config.S3_ENDPOINT_URL)
    
    def list_files(self, prefix: str) -> List[Dict[str, Any]]:
        """List files in S3 bucket with given prefix"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=config.S3_BUCKET,
                Prefix=prefix
            )
            
            files = []
            for obj in response.get('Contents', []):
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'etag': obj['ETag']
                })
            
            return files
            
        except Exception as e:
            self.logger.error(f"Failed to list files with prefix {prefix}: {e}")
            return []
    
    def get_file_info(self, key: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific S3 object"""
        try:
            response = self.s3_client.head_object(
                Bucket=config.S3_BUCKET,
                Key=key
            )
            
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType'),
                'etag': response['ETag']
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get file info for {key}: {e}")
            return None

# Create global instances
db_manager = DuckDBManager()
s3_manager = S3Manager()
