import os
import logging
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for the data lake project"""
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID: Optional[str] = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION: str = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    AWS_PROFILE: Optional[str] = os.getenv('AWS_PROFILE')
    
    # S3 Configuration
    S3_BUCKET: str = os.getenv('S3_BUCKET', 'vendor-data-s3')
    S3_ENDPOINT_URL: str = os.getenv('S3_ENDPOINT_URL', 'https://s3.amazonaws.com')
    
    # DuckDB Configuration
    DUCKDB_DATABASE_PATH: str = os.getenv('DUCKDB_DATABASE_PATH', './data_lake.duckdb')
    DUCKDB_MEMORY_LIMIT: str = os.getenv('DUCKDB_MEMORY_LIMIT', '8GB')
    DUCKDB_THREADS: int = int(os.getenv('DUCKDB_THREADS', '4'))
    
    # Data Processing Configuration
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', '100000'))
    MAX_MEMORY_USAGE: str = os.getenv('MAX_MEMORY_USAGE', '4GB')
    TEMP_DIRECTORY: str = os.getenv('TEMP_DIRECTORY', './temp')
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE: str = os.getenv('LOG_FILE', './logs/datalake.log')
    
    # Data paths
    BASE_S3_PATH: str = f"s3://{S3_BUCKET}/LSEG/TRTH/LSE"
    INGESTION_PATH: str = f"{BASE_S3_PATH}/ingestion"
    TRANSFORMATION_PATH: str = f"{BASE_S3_PATH}/transformation"
    NORMALIZATION_PATH: str = f"{BASE_S3_PATH}/normalization"
    
    @classmethod
    def setup_logging(cls):
        """Setup logging configuration"""
        os.makedirs(os.path.dirname(cls.LOG_FILE), exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, cls.LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(cls.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        
        return logging.getLogger(__name__)
    
    @classmethod
    def validate_config(cls):
        """Validate required configuration"""
        errors = []
        
        if not cls.AWS_ACCESS_KEY_ID and not cls.AWS_PROFILE:
            errors.append("Either AWS_ACCESS_KEY_ID or AWS_PROFILE must be set")
        
        if cls.AWS_ACCESS_KEY_ID and not cls.AWS_SECRET_ACCESS_KEY:
            errors.append("AWS_SECRET_ACCESS_KEY must be set when using AWS_ACCESS_KEY_ID")
        
        if errors:
            raise ValueError(f"Configuration errors: {'; '.join(errors)}")
        
        return True

# Create global config instance
config = Config()
