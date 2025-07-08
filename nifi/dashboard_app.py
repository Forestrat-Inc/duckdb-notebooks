#!/usr/bin/env python3
"""
Web Dashboard for January 2025 Data Loading Progress

This Flask application provides a real-time dashboard to monitor:
- Loading progress across all exchanges
- Daily and weekly statistics
- Error tracking and status
- Shutdown control capabilities
"""

import os
import json
from datetime import datetime, date
from pathlib import Path
import pandas as pd
import duckdb
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash

# Add project root to path
import sys
project_root = Path(__file__).parent.parent  # Go up one level since we're in nifi/
sys.path.append(str(project_root))

from utils.database import DuckDBManager

app = Flask(__name__)
app.secret_key = 'january-2025-data-loading-dashboard'

# Global database manager
db_manager = None

def get_db_manager():
    """Get or create database manager"""
    global db_manager
    if db_manager is None:
        # Database is in parent directory
        db_manager = DuckDBManager(database_path="../multi_exchange_data_lake.duckdb")
    return db_manager

def safe_query(query, default_value=None):
    """Execute query safely and return result or default"""
    try:
        return get_db_manager().execute_query(query)
    except Exception as e:
        print(f"Query error: {e}")
        return pd.DataFrame() if default_value is None else default_value

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/overview')
def api_overview():
    """API endpoint for overview statistics"""
    
    # Get overall progress
    progress_query = """
    SELECT 
        exchange,
        COUNT(*) as total_files,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_files,
        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_files,
        COUNT(CASE WHEN status = 'started' THEN 1 END) as running_files,
        SUM(COALESCE(records_loaded, 0)) as total_records,
        MIN(data_date) as start_date,
        MAX(data_date) as end_date
    FROM bronze.load_progress
    GROUP BY exchange
    ORDER BY exchange
    """
    
    progress_df = safe_query(progress_query)
    
    # Get latest statistics
    stats_query = """
    SELECT 
        exchange,
        stats_date,
        total_files,
        successful_files,
        failed_files,
        total_records,
        ROUND(avg_records_per_file, 0) as avg_records_per_file,
        ROUND(total_processing_time_seconds, 2) as processing_time_seconds
    FROM gold.daily_load_stats
    ORDER BY stats_date DESC, exchange
    LIMIT 10
    """
    
    stats_df = safe_query(stats_query)
    
    # Check if script is currently running
    running_query = """
    SELECT COUNT(*) as running_count
    FROM bronze.load_progress
    WHERE status = 'started'
    """
    
    running_df = safe_query(running_query)
    is_running = running_df.iloc[0]['running_count'] > 0 if not running_df.empty else False
    
    # Check shutdown status
    shutdown_file = Path("../shutdown_load_january.flag")  # In parent directory
    shutdown_requested = shutdown_file.exists()
    
    # Convert DataFrames to JSON-safe dictionaries
    overview_data = []
    if not progress_df.empty:
        for _, row in progress_df.iterrows():
            overview_data.append({
                'exchange': str(row.get('exchange', '')),
                'total_files': int(row.get('total_files', 0)),
                'successful_files': int(row.get('successful_files', 0)),
                'failed_files': int(row.get('failed_files', 0)),
                'total_records': int(row.get('total_records', 0))
            })
    
    recent_stats_data = []
    if not stats_df.empty:
        for _, row in stats_df.iterrows():
            recent_stats_data.append({
                'stats_date': str(row.get('stats_date', '')),
                'exchange': str(row.get('exchange', '')),
                'total_files': int(row.get('total_files', 0)),
                'successful_files': int(row.get('successful_files', 0)),
                'total_records': int(row.get('total_records', 0))
            })
    
    return jsonify({
        'overview': overview_data,
        'recent_stats': recent_stats_data,
        'is_running': bool(is_running),
        'shutdown_requested': bool(shutdown_requested),
        'last_updated': datetime.now().isoformat()
    })

@app.route('/api/progress_detail')
def api_progress_detail():
    """API endpoint for detailed progress information"""
    
    # Get recent progress entries
    recent_progress_query = """
    SELECT 
        exchange,
        data_date,
        status,
        records_loaded,
        start_time,
        end_time,
        error_message,
        CASE 
            WHEN end_time IS NOT NULL AND start_time IS NOT NULL THEN
                ROUND(EXTRACT(EPOCH FROM (end_time - start_time)), 2)
            ELSE NULL
        END as processing_time_seconds
    FROM bronze.load_progress
    ORDER BY start_time DESC
    LIMIT 50
    """
    
    progress_df = safe_query(recent_progress_query)
    
    # Get daily progress chart data
    daily_chart_query = """
    SELECT 
        stats_date,
        exchange,
        successful_files,
        failed_files,
        total_records
    FROM gold.daily_load_stats
    ORDER BY stats_date, exchange
    """
    
    chart_df = safe_query(daily_chart_query)
    
    # Convert DataFrames to JSON-safe dictionaries
    recent_progress_data = []
    if not progress_df.empty:
        for _, row in progress_df.iterrows():
            recent_progress_data.append({
                'exchange': str(row.get('exchange', '')),
                'data_date': str(row.get('data_date', '')),
                'status': str(row.get('status', '')),
                'records_loaded': int(row.get('records_loaded', 0)),
                'start_time': str(row.get('start_time', '')),
                'end_time': str(row.get('end_time', '')) if row.get('end_time') else None,
                'error_message': str(row.get('error_message', '')) if row.get('error_message') else None,
                'processing_time_seconds': float(row.get('processing_time_seconds', 0)) if row.get('processing_time_seconds') else None
            })
    
    daily_chart_data = []
    if not chart_df.empty:
        for _, row in chart_df.iterrows():
            daily_chart_data.append({
                'stats_date': str(row.get('stats_date', '')),
                'exchange': str(row.get('exchange', '')),
                'successful_files': int(row.get('successful_files', 0)),
                'failed_files': int(row.get('failed_files', 0)),
                'total_records': int(row.get('total_records', 0))
            })
    
    return jsonify({
        'recent_progress': recent_progress_data,
        'daily_chart_data': daily_chart_data,
        'last_updated': datetime.now().isoformat()
    })

@app.route('/api/errors')
def api_errors():
    """API endpoint for error tracking"""
    
    errors_query = """
    SELECT 
        exchange,
        data_date,
        error_message,
        start_time,
        file_path
    FROM bronze.load_progress
    WHERE status = 'failed'
    ORDER BY start_time DESC
    LIMIT 20
    """
    
    errors_df = safe_query(errors_query)
    
    # Get error summary
    error_summary_query = """
    SELECT 
        exchange,
        COUNT(*) as error_count,
        COUNT(DISTINCT DATE(start_time)) as error_days
    FROM bronze.load_progress
    WHERE status = 'failed'
    GROUP BY exchange
    ORDER BY error_count DESC
    """
    
    error_summary_df = safe_query(error_summary_query)
    
    # Convert DataFrames to JSON-safe dictionaries
    recent_errors_data = []
    if not errors_df.empty:
        for _, row in errors_df.iterrows():
            recent_errors_data.append({
                'exchange': str(row.get('exchange', '')),
                'data_date': str(row.get('data_date', '')),
                'error_message': str(row.get('error_message', '')),
                'start_time': str(row.get('start_time', '')),
                'file_path': str(row.get('file_path', ''))
            })
    
    error_summary_data = []
    if not error_summary_df.empty:
        for _, row in error_summary_df.iterrows():
            error_summary_data.append({
                'exchange': str(row.get('exchange', '')),
                'error_count': int(row.get('error_count', 0)),
                'error_days': int(row.get('error_days', 0))
            })
    
    return jsonify({
        'recent_errors': recent_errors_data,
        'error_summary': error_summary_data,
        'last_updated': datetime.now().isoformat()
    })

@app.route('/api/statistics')
def api_statistics():
    """API endpoint for detailed statistics"""
    
    # Weekly statistics
    weekly_stats_query = """
    SELECT 
        exchange,
        week_ending,
        ROUND(avg_daily_files, 1) as avg_daily_files,
        ROUND(avg_daily_records, 0) as avg_daily_records,
        total_files,
        total_records,
        ROUND(avg_processing_time_seconds, 2) as avg_processing_time_seconds
    FROM gold.weekly_load_stats
    ORDER BY week_ending DESC, exchange
    """
    
    weekly_df = safe_query(weekly_stats_query)
    
    # Performance metrics
    performance_query = """
    SELECT 
        exchange,
        AVG(records_loaded) as avg_records_per_file,
        AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_processing_time,
        MAX(records_loaded) as max_records_per_file,
        MIN(records_loaded) as min_records_per_file
    FROM bronze.load_progress
    WHERE status = 'completed'
    GROUP BY exchange
    """
    
    performance_df = safe_query(performance_query)
    
    # Convert DataFrames to JSON-safe dictionaries
    weekly_stats_data = []
    if not weekly_df.empty:
        for _, row in weekly_df.iterrows():
            weekly_stats_data.append({
                'exchange': str(row.get('exchange', '')),
                'week_ending': str(row.get('week_ending', '')),
                'avg_daily_files': float(row.get('avg_daily_files', 0)),
                'avg_daily_records': float(row.get('avg_daily_records', 0)),
                'total_files': int(row.get('total_files', 0)),
                'total_records': int(row.get('total_records', 0)),
                'avg_processing_time_seconds': float(row.get('avg_processing_time_seconds', 0))
            })
    
    performance_metrics_data = []
    if not performance_df.empty:
        for _, row in performance_df.iterrows():
            performance_metrics_data.append({
                'exchange': str(row.get('exchange', '')),
                'avg_records_per_file': float(row.get('avg_records_per_file', 0)),
                'avg_processing_time': float(row.get('avg_processing_time', 0)),
                'max_records_per_file': int(row.get('max_records_per_file', 0)),
                'min_records_per_file': int(row.get('min_records_per_file', 0))
            })
    
    return jsonify({
        'weekly_stats': weekly_stats_data,
        'performance_metrics': performance_metrics_data,
        'last_updated': datetime.now().isoformat()
    })

@app.route('/control/shutdown', methods=['POST'])
def control_shutdown():
    """Create shutdown file to gracefully stop the script"""
    try:
        shutdown_file = Path("../shutdown_load_january.flag")  # In parent directory
        shutdown_file.write_text(f"Shutdown requested from dashboard at {datetime.now().isoformat()}")
        flash("Shutdown signal created. The script will stop gracefully after completing the current transaction.", "success")
    except Exception as e:
        flash(f"Error creating shutdown file: {e}", "error")
    
    return redirect(url_for('dashboard'))

@app.route('/control/resume', methods=['POST'])
def control_resume():
    """Remove shutdown file to allow the script to continue"""
    try:
        shutdown_file = Path("../shutdown_load_january.flag")  # In parent directory
        if shutdown_file.exists():
            shutdown_file.unlink()
            flash("Shutdown signal removed. The script will continue normally.", "success")
        else:
            flash("No shutdown signal found.", "info")
    except Exception as e:
        flash(f"Error removing shutdown file: {e}", "error")
    
    return redirect(url_for('dashboard'))

@app.route('/api/database_stats')
def api_database_stats():
    """API endpoint for database statistics"""
    
    # Get table sizes
    table_stats_query = """
    SELECT 
        schema_name,
        table_name,
        estimated_size
    FROM information_schema.tables
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    """
    
    table_stats_df = safe_query(table_stats_query)
    
    # Get record counts
    record_counts = {}
    for schema in ['bronze', 'silver', 'gold']:
        schema_query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        """
        tables_df = safe_query(schema_query)
        
        for _, row in tables_df.iterrows():
            table_name = row['table_name']
            count_query = f"SELECT COUNT(*) as count FROM {schema}.{table_name}"
            count_df = safe_query(count_query)
            if not count_df.empty:
                record_counts[f"{schema}.{table_name}"] = count_df.iloc[0]['count']
    
    return jsonify({
        'table_stats': table_stats_df.to_dict('records') if not table_stats_df.empty else [],
        'record_counts': record_counts,
        'last_updated': datetime.now().isoformat()
    })

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    templates_dir = Path("templates")
    templates_dir.mkdir(exist_ok=True)
    
    # Create static directory for CSS/JS
    static_dir = Path("static")
    static_dir.mkdir(exist_ok=True)
    
    print("Starting January 2025 Data Loading Dashboard...")
    print("Dashboard will be available at: http://localhost:12345")
    print("Use Ctrl+C to stop the dashboard")
    
    app.run(debug=False, host='0.0.0.0', port=12345) 