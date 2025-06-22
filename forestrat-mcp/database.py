"""
DuckDB Connection Manager for Forestrat MCP Server

This module provides a persistent connection to the DuckDB database
to avoid reconnecting for every query.
"""

import duckdb
import pandas as pd
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path
import atexit

logger = logging.getLogger(__name__)

class DuckDBConnection:
    """Persistent DuckDB connection manager"""
    
    def __init__(self, database_path: str):
        self.database_path = Path(database_path).resolve()
        self._connection = None
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Ensure database exists
        if not self.database_path.exists():
            raise FileNotFoundError(f"Database not found at {self.database_path}")
        
        # Register cleanup on exit
        atexit.register(self.close)
        
        self.logger.info(f"Initialized DuckDB connection manager for {self.database_path}")
    
    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create the database connection"""
        if self._connection is None:
            try:
                self._connection = duckdb.connect(str(self.database_path))
                self._configure_connection()
                self.logger.info("Database connection established")
            except Exception as e:
                self.logger.error(f"Failed to connect to database: {e}")
                raise
        
        return self._connection
    
    def _configure_connection(self):
        """Configure the DuckDB connection with optimal settings"""
        try:
            conn = self._connection
            
            # Set memory and performance settings
            conn.execute("SET memory_limit='4GB'")
            conn.execute("SET threads=4")
            
            # Enable progress bar if available
            try:
                conn.execute("SET enable_progress_bar=1")
            except Exception:
                pass  # Not available in all versions
            
            # Set pragmas for better performance
            conn.execute("PRAGMA enable_profiling")
            conn.execute("PRAGMA profiling_mode = 'detailed'")
            
        except Exception as e:
            self.logger.warning(f"Could not configure connection settings: {e}")
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame"""
        try:
            result = self.connection.execute(query).df()
            self.logger.debug(f"Query executed successfully, returned {len(result)} rows")
            return result
        except Exception as e:
            self.logger.error(f"Query execution failed: {query[:100]}... Error: {e}")
            raise
    
    def execute_sql(self, sql: str) -> Any:
        """Execute a SQL statement (non-query)"""
        try:
            result = self.connection.execute(sql)
            self.logger.debug("SQL statement executed successfully")
            return result
        except Exception as e:
            self.logger.error(f"SQL execution failed: {sql[:100]}... Error: {e}")
            raise
    
    def get_table_columns(self, table_name: str) -> Dict[str, str]:
        """Get column information for a table"""
        try:
            result = self.execute_query(f"DESCRIBE {table_name}")
            
            # Convert to a dictionary mapping column_name -> column_type
            column_dict = {}
            for row in result.to_dict('records'):
                column_dict[row['column_name']] = row['column_type']
            
            logger.info(f"Retrieved columns for {table_name}: {len(column_dict)} columns")
            return column_dict
            
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {e}")
            return {}
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            # Parse schema and table name
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
                query = """
                SELECT COUNT(*) as count
                FROM information_schema.tables 
                WHERE table_schema = ? AND table_name = ?
                """
                result = self.connection.execute(query, [schema, table]).fetchone()
            else:
                query = """
                SELECT COUNT(*) as count
                FROM information_schema.tables 
                WHERE table_name = ?
                """
                result = self.connection.execute(query, [table_name]).fetchone()
            
            return result[0] > 0
        except Exception as e:
            self.logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive table information"""
        try:
            info = {
                "table_name": table_name,
                "exists": self.table_exists(table_name)
            }
            
            if info["exists"]:
                # Get schema
                schema_result = self.execute_query(f"DESCRIBE {table_name}")
                info["columns"] = schema_result.to_dict('records')
                
                # Get row count
                count_result = self.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
                info["row_count"] = int(count_result.iloc[0]['count'])
                
                # Get sample data
                sample_result = self.execute_query(f"SELECT * FROM {table_name} LIMIT 3")
                info["sample_data"] = sample_result.to_dict('records')
            
            return info
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {e}")
            raise
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get overall database statistics"""
        try:
            stats = {}
            
            # Get all tables
            tables_query = """
            SELECT 
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'main')
            ORDER BY table_schema, table_name
            """
            
            tables = self.execute_query(tables_query)
            stats["total_tables"] = len(tables)
            stats["schemas"] = tables['table_schema'].unique().tolist()
            stats["tables_by_schema"] = tables.groupby('table_schema')['table_name'].count().to_dict()
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}
    
    def test_connection(self) -> bool:
        """Test the database connection"""
        try:
            result = self.connection.execute("SELECT 1 as test").fetchone()
            return result[0] == 1
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
    
    def close(self):
        """Close the database connection"""
        if self._connection:
            try:
                self._connection.close()
                self._connection = None
                self.logger.info("Database connection closed")
            except Exception as e:
                self.logger.error(f"Error closing connection: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def __del__(self):
        """Destructor to ensure connection is closed"""
        self.close() 