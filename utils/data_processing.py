import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date, timedelta
import re

class DataProcessor:
    """Data processing utilities for market data"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def clean_market_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate market data"""
        original_count = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        df = self._handle_missing_values(df)
        
        # Validate data types and ranges
        df = self._validate_data_ranges(df)
        
        # Standardize column names
        df = self._standardize_columns(df)
        
        cleaned_count = len(df)
        self.logger.info(f"Data cleaning: {original_count} -> {cleaned_count} rows "
                        f"({original_count - cleaned_count} removed)")
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in market data"""
        # Define strategies for different column types
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if col.lower() in ['price', 'open', 'high', 'low', 'close']:
                # For price columns, remove rows with null values
                df = df.dropna(subset=[col])
            elif col.lower() in ['volume']:
                # For volume, fill with 0
                df[col] = df[col].fillna(0)
        
        return df
    
    def _validate_data_ranges(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data ranges for market data"""
        # Remove rows with invalid prices (negative or zero)
        price_columns = [col for col in df.columns 
                        if any(price_term in col.lower() 
                              for price_term in ['price', 'open', 'high', 'low', 'close'])]
        
        for col in price_columns:
            if col in df.columns:
                df = df[df[col] > 0]
        
        # Remove rows with negative volume
        volume_columns = [col for col in df.columns 
                         if 'volume' in col.lower()]
        
        for col in volume_columns:
            if col in df.columns:
                df = df[df[col] >= 0]
        
        return df
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names"""
        # Convert column names to lowercase and replace spaces/special chars
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') 
                     for col in df.columns]
        
        return df
    
    def add_technical_indicators(self, df: pd.DataFrame, 
                               price_col: str = 'close_price') -> pd.DataFrame:
        """Add technical indicators to market data"""
        df = df.copy()
        
        # Sort by timestamp for proper calculation
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp')
        elif 'date' in df.columns:
            df = df.sort_values('date')
        
        # Simple Moving Averages
        for window in [5, 10, 20, 50]:
            df[f'sma_{window}'] = df[price_col].rolling(window=window, min_periods=1).mean()
        
        # Exponential Moving Average
        df['ema_12'] = df[price_col].ewm(span=12).mean()
        df['ema_26'] = df[price_col].ewm(span=26).mean()
        
        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        df['macd_histogram'] = df['macd'] - df['macd_signal']
        
        # RSI
        df['rsi'] = self._calculate_rsi(df[price_col])
        
        # Bollinger Bands
        bb_data = self._calculate_bollinger_bands(df[price_col])
        df['bb_upper'] = bb_data['upper']
        df['bb_lower'] = bb_data['lower']
        df['bb_middle'] = bb_data['middle']
        
        # Price change percentage
        df['price_change_pct'] = df[price_col].pct_change() * 100
        
        self.logger.info("Technical indicators added successfully")
        return df
    
    def _calculate_rsi(self, prices: pd.Series, window: int = 14) -> pd.Series:
        """Calculate Relative Strength Index"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _calculate_bollinger_bands(self, prices: pd.Series, 
                                  window: int = 20, num_std: float = 2) -> Dict[str, pd.Series]:
        """Calculate Bollinger Bands"""
        rolling_mean = prices.rolling(window=window).mean()
        rolling_std = prices.rolling(window=window).std()
        
        return {
            'upper': rolling_mean + (rolling_std * num_std),
            'lower': rolling_mean - (rolling_std * num_std),
            'middle': rolling_mean
        }
    
    def aggregate_by_timeframe(self, df: pd.DataFrame, 
                              timeframe: str = 'D',
                              price_col: str = 'price',
                              volume_col: str = 'volume',
                              timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """Aggregate data by timeframe (D=daily, H=hourly, etc.)"""
        
        # Ensure timestamp column is datetime
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Set timestamp as index for resampling
        df_indexed = df.set_index(timestamp_col)
        
        # Define aggregation functions
        agg_functions = {
            price_col: ['first', 'max', 'min', 'last'],
            volume_col: 'sum'
        }
        
        # Add other numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col not in [price_col, volume_col]:
                agg_functions[col] = 'mean'
        
        # Resample and aggregate
        aggregated = df_indexed.resample(timeframe).agg(agg_functions)
        
        # Flatten column names
        aggregated.columns = ['_'.join(col).strip() if isinstance(col, tuple) 
                             else col for col in aggregated.columns]
        
        # Rename OHLC columns
        column_mapping = {
            f'{price_col}_first': 'open',
            f'{price_col}_max': 'high',
            f'{price_col}_min': 'low',
            f'{price_col}_last': 'close',
            f'{volume_col}_sum': 'volume'
        }
        
        aggregated = aggregated.rename(columns=column_mapping)
        
        # Reset index to get timestamp back as column
        aggregated = aggregated.reset_index()
        
        return aggregated
    
    def detect_outliers(self, df: pd.DataFrame, 
                       columns: List[str], 
                       method: str = 'iqr') -> pd.DataFrame:
        """Detect outliers in specified columns"""
        outlier_mask = pd.Series([False] * len(df))
        
        for col in columns:
            if col not in df.columns:
                continue
                
            if method == 'iqr':
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                col_outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
                
            elif method == 'zscore':
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                col_outliers = z_scores > 3
            
            outlier_mask |= col_outliers
        
        return df[~outlier_mask]
    
    def create_market_summary(self, df: pd.DataFrame, 
                            group_by: str = 'symbol') -> pd.DataFrame:
        """Create market summary statistics"""
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        summary_stats = []
        
        for name, group in df.groupby(group_by):
            stats = {
                group_by: name,
                'count': len(group),
                'avg_price': group['price'].mean() if 'price' in group.columns else None,
                'min_price': group['price'].min() if 'price' in group.columns else None,
                'max_price': group['price'].max() if 'price' in group.columns else None,
                'total_volume': group['volume'].sum() if 'volume' in group.columns else None,
                'volatility': group['price'].std() if 'price' in group.columns else None
            }
            
            summary_stats.append(stats)
        
        return pd.DataFrame(summary_stats)

class DataQualityChecker:
    """Data quality checking utilities"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def run_quality_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Run comprehensive data quality checks"""
        
        quality_report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'checks': {}
        }
        
        # Null value checks
        quality_report['checks']['null_values'] = self._check_null_values(df)
        
        # Duplicate checks
        quality_report['checks']['duplicates'] = self._check_duplicates(df)
        
        # Data type checks
        quality_report['checks']['data_types'] = self._check_data_types(df)
        
        # Range checks for numeric columns
        quality_report['checks']['numeric_ranges'] = self._check_numeric_ranges(df)
        
        # Pattern checks for string columns
        quality_report['checks']['string_patterns'] = self._check_string_patterns(df)
        
        return quality_report
    
    def _check_null_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for null values"""
        null_counts = df.isnull().sum()
        null_percentages = (null_counts / len(df) * 100).round(2)
        
        return {
            'columns_with_nulls': null_counts[null_counts > 0].to_dict(),
            'null_percentages': null_percentages[null_percentages > 0].to_dict(),
            'total_null_values': null_counts.sum()
        }
    
    def _check_duplicates(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for duplicate records"""
        total_duplicates = df.duplicated().sum()
        duplicate_percentage = (total_duplicates / len(df) * 100).round(2)
        
        return {
            'total_duplicates': total_duplicates,
            'duplicate_percentage': duplicate_percentage,
            'unique_rows': len(df) - total_duplicates
        }
    
    def _check_data_types(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check data types"""
        type_info = {}
        
        for col in df.columns:
            type_info[col] = {
                'dtype': str(df[col].dtype),
                'non_null_count': df[col].count(),
                'unique_values': df[col].nunique()
            }
        
        return type_info
    
    def _check_numeric_ranges(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check numeric column ranges"""
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        range_info = {}
        
        for col in numeric_columns:
            range_info[col] = {
                'min': df[col].min(),
                'max': df[col].max(),
                'mean': df[col].mean(),
                'std': df[col].std(),
                'negative_values': (df[col] < 0).sum(),
                'zero_values': (df[col] == 0).sum()
            }
        
        return range_info
    
    def _check_string_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check string column patterns"""
        string_columns = df.select_dtypes(include=['object']).columns
        pattern_info = {}
        
        for col in string_columns:
            if df[col].dtype == 'object':
                pattern_info[col] = {
                    'unique_values': df[col].nunique(),
                    'avg_length': df[col].astype(str).str.len().mean(),
                    'max_length': df[col].astype(str).str.len().max(),
                    'empty_strings': (df[col] == '').sum(),
                    'sample_values': df[col].dropna().head(5).tolist()
                }
        
        return pattern_info

# Create global instances
data_processor = DataProcessor()
quality_checker = DataQualityChecker()
