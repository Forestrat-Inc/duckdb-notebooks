#!/usr/bin/env python3
"""
Market Data Dashboard - Flask Application
Displays real-time statistics and progress for the multi-exchange data lake
"""

import sys
import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from flask import Flask, render_template, jsonify, request
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from utils.database import DuckDBManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['DEBUG'] = False  # Disable debug mode to prevent crashes

# Initialize database manager in READ-ONLY mode for security
db_manager = DuckDBManager(database_path="./multi_exchange_data_lake.duckdb", read_only=True)

def safe_json_convert(data):
    """Convert data to JSON-safe format"""
    if isinstance(data, list):
        return [safe_json_convert(item) for item in data]
    elif isinstance(data, dict):
        return {str(k): safe_json_convert(v) for k, v in data.items()}
    elif pd.isna(data) or data is None:
        return None
    elif isinstance(data, (pd.Timestamp, datetime)):
        return data.isoformat()
    elif isinstance(data, date):
        return data.isoformat()
    elif isinstance(data, (int, float, str, bool)):
        return data
    else:
        return str(data)

def execute_safe_query(query, default_result=None):
    """Execute query safely with error handling"""
    try:
        result = db_manager.execute_query(query)
        return result
    except Exception as e:
        logger.error(f"Query failed: {e}")
        if default_result is not None:
            return pd.DataFrame(default_result)
        return pd.DataFrame()

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/system-status')
def system_status():
    """Get overall system status"""
    try:
        # Check if database is accessible
        test_query = "SELECT 1 as test"
        test_result = execute_safe_query(test_query)
        
        if not test_result.empty:
            status = "Running"
        else:
            status = "Error"
        
        # Get recent activity count
        recent_activity_query = """
        SELECT COUNT(*) as count 
        FROM bronze.load_progress 
        WHERE start_time >= NOW() - INTERVAL '24 hours'
        """
        recent_activity = execute_safe_query(recent_activity_query, [{'count': 0}])
        
        return jsonify({
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'recent_activity': int(recent_activity.iloc[0]['count']) if not recent_activity.empty else 0
        })
    except Exception as e:
        logger.error(f"System status error: {e}")
        return jsonify({
            'status': 'Error',
            'timestamp': datetime.now().isoformat(),
            'recent_activity': 0,
            'error': str(e)
        })

@app.route('/api/data-summary')
def data_summary():
    """Get data summary statistics"""
    try:
        exchanges = ['lse', 'cme', 'nyq']
        summary = []
        total_records = 0
        
        for exchange in exchanges:
            try:
                # Get record count
                count_query = f"SELECT COUNT(*) as count FROM bronze.{exchange}_market_data_raw"
                count_result = execute_safe_query(count_query, [{'count': 0}])
                record_count = int(count_result.iloc[0]['count']) if not count_result.empty else 0
                
                # Get date range
                date_range_query = f"""
                SELECT 
                    MIN(data_date) as min_date,
                    MAX(data_date) as max_date
                FROM bronze.{exchange}_market_data_raw
                WHERE data_date IS NOT NULL
                """
                date_result = execute_safe_query(date_range_query)
                
                min_date = None
                max_date = None
                if not date_result.empty and date_result.iloc[0]['min_date'] is not None:
                    min_date = date_result.iloc[0]['min_date']
                    max_date = date_result.iloc[0]['max_date']
                
                summary.append({
                    'exchange': exchange.upper(),
                    'records': record_count,
                    'min_date': min_date.isoformat() if min_date else None,
                    'max_date': max_date.isoformat() if max_date else None
                })
                
                total_records += record_count
                
            except Exception as e:
                logger.error(f"Error getting data for {exchange}: {e}")
                summary.append({
                    'exchange': exchange.upper(),
                    'records': 0,
                    'min_date': None,
                    'max_date': None,
                    'error': str(e)
                })
        
        return jsonify({
            'exchanges': summary,
            'total_records': total_records
        })
    except Exception as e:
        logger.error(f"Data summary error: {e}")
        return jsonify({
            'exchanges': [],
            'total_records': 0,
            'error': str(e)
        })

@app.route('/api/recent-activity')
def recent_activity():
    """Get recent loading activity"""
    try:
        # Get recent progress records
        query = """
        SELECT 
            exchange,
            data_date,
            status,
            records_loaded,
            start_time,
            end_time,
            error_message
        FROM bronze.load_progress 
        ORDER BY start_time DESC 
        LIMIT 20
        """
        
        result = execute_safe_query(query)
        
        if result.empty:
            return jsonify([])
        
        # Convert to safe JSON format
        activities = []
        for _, row in result.iterrows():
            activity = {}
            for col in result.columns:
                activity[col] = safe_json_convert(row[col])
            activities.append(activity)
        
        return jsonify(activities)
    except Exception as e:
        logger.error(f"Recent activity error: {e}")
        return jsonify([])

@app.route('/api/daily-stats')
def daily_stats():
    """Get daily statistics for the past 30 days"""
    try:
        # Check if daily stats table exists
        check_table_query = """
        SELECT COUNT(*) as count 
        FROM information_schema.tables 
        WHERE table_schema = 'gold' AND table_name = 'daily_load_stats'
        """
        table_exists = execute_safe_query(check_table_query)
        
        if table_exists.empty or table_exists.iloc[0]['count'] == 0:
            # If no daily stats table, create summary from raw data
            summary_query = """
            SELECT 
                exchange,
                data_date,
                COUNT(*) as record_count
            FROM (
                SELECT 'LSE' as exchange, data_date FROM bronze.lse_market_data_raw WHERE data_date IS NOT NULL
                UNION ALL
                SELECT 'CME' as exchange, data_date FROM bronze.cme_market_data_raw WHERE data_date IS NOT NULL  
                UNION ALL
                SELECT 'NYQ' as exchange, data_date FROM bronze.nyq_market_data_raw WHERE data_date IS NOT NULL
            ) combined
            WHERE data_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY exchange, data_date
            ORDER BY data_date DESC, exchange
            LIMIT 100
            """
        else:
            # Use existing daily stats
            summary_query = """
            SELECT 
                exchange,
                stats_date as data_date,
                total_records as record_count,
                successful_files,
                failed_files
            FROM gold.daily_load_stats 
            WHERE stats_date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY stats_date DESC, exchange
            """
        
        result = execute_safe_query(summary_query)
        
        if result.empty:
            return jsonify([])
        
        # Convert to safe JSON format
        stats = []
        for _, row in result.iterrows():
            stat = {}
            for col in result.columns:
                stat[col] = safe_json_convert(row[col])
            stats.append(stat)
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Daily stats error: {e}")
        return jsonify([])

@app.route('/api/performance-stats')
def performance_stats():
    """Get performance statistics"""
    try:
        # Basic performance metrics from load progress
        perf_query = """
        SELECT 
            exchange,
            COUNT(*) as total_loads,
            AVG(records_loaded) as avg_records_per_load,
            SUM(records_loaded) as total_records,
            AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds
        FROM bronze.load_progress 
        WHERE status = 'completed'
        AND start_time >= NOW() - INTERVAL '7 days'
        GROUP BY exchange
        ORDER BY exchange
        """
        
        result = execute_safe_query(perf_query)
        
        if result.empty:
            # Fallback to basic data count
            fallback_query = """
            SELECT 
                'LSE' as exchange, COUNT(*) as total_records FROM bronze.lse_market_data_raw
            UNION ALL
            SELECT 
                'CME' as exchange, COUNT(*) as total_records FROM bronze.cme_market_data_raw
            UNION ALL
            SELECT 
                'NYQ' as exchange, COUNT(*) as total_records FROM bronze.nyq_market_data_raw
            """
            result = execute_safe_query(fallback_query)
        
        # Convert to safe JSON format
        stats = []
        for _, row in result.iterrows():
            stat = {}
            for col in result.columns:
                stat[col] = safe_json_convert(row[col])
            stats.append(stat)
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Performance stats error: {e}")
        return jsonify([])

@app.route('/api/error-summary')
def error_summary():
    """Get error summary"""
    try:
        # Get recent errors from load progress
        error_query = """
        SELECT 
            exchange,
            COUNT(*) as error_count,
            error_message
        FROM bronze.load_progress 
        WHERE status = 'failed'
        AND start_time >= NOW() - INTERVAL '7 days'
        GROUP BY exchange, error_message
        ORDER BY error_count DESC
        LIMIT 10
        """
        
        result = execute_safe_query(error_query)
        
        if result.empty:
            return jsonify({'total_errors': 0, 'recent_errors': []})
        
        # Convert to safe JSON format
        errors = []
        for _, row in result.iterrows():
            error = {}
            for col in result.columns:
                error[col] = safe_json_convert(row[col])
            errors.append(error)
        
        total_errors = sum(error['error_count'] for error in errors)
        
        return jsonify({
            'total_errors': total_errors,
            'recent_errors': errors
        })
    except Exception as e:
        logger.error(f"Error summary error: {e}")
        return jsonify({'total_errors': 0, 'recent_errors': []})

if __name__ == '__main__':
    print("Starting Market Data Dashboard...")
    print("Dashboard will be available at: http://localhost:12345")
    print("Press Ctrl+C to stop")
    
    try:
        app.run(host='0.0.0.0', port=12345, debug=False)
    except KeyboardInterrupt:
        print("\nDashboard stopped by user")
    except Exception as e:
        print(f"Dashboard error: {e}") 