-- NiFi Statistics Tables for Daily and Weekly Rolling Analysis
-- Created for DuckDB integration with NiFi 2.3.0

-- ============================================================================
-- DAILY STATISTICS TABLES
-- ============================================================================

-- Gold layer table for daily aggregated statistics
CREATE TABLE IF NOT EXISTS gold.daily_stats (
    id INTEGER PRIMARY KEY DEFAULT nextval('gold.daily_stats_seq'),
    
    -- Time dimension
    stats_date DATE NOT NULL,
    exchange VARCHAR NOT NULL,
    
    -- File processing statistics
    total_files_processed INTEGER DEFAULT 0,
    total_files_completed INTEGER DEFAULT 0,
    total_files_failed INTEGER DEFAULT 0,
    total_files_skipped INTEGER DEFAULT 0,
    completion_rate DECIMAL(5,2) DEFAULT 0,
    
    -- Record processing statistics
    total_records_processed BIGINT DEFAULT 0,
    total_records_loaded BIGINT DEFAULT 0,
    total_records_rejected BIGINT DEFAULT 0,
    data_quality_score DECIMAL(5,2) DEFAULT 0,
    rejection_rate DECIMAL(5,2) DEFAULT 0,
    
    -- Performance metrics
    avg_processing_time_seconds DECIMAL(10,2),
    min_processing_time_seconds DECIMAL(10,2),
    max_processing_time_seconds DECIMAL(10,2),
    total_processing_time_seconds DECIMAL(10,2),
    
    -- Throughput metrics
    avg_records_per_second DECIMAL(10,2),
    peak_records_per_second DECIMAL(10,2),
    total_data_volume_bytes BIGINT DEFAULT 0,
    avg_bytes_per_second DECIMAL(10,2),
    
    -- Error statistics
    total_errors INTEGER DEFAULT 0,
    error_rate DECIMAL(5,2) DEFAULT 0,
    top_error_type VARCHAR,
    
    -- Operational metrics
    total_processing_duration_hours DECIMAL(10,2),
    uptime_percentage DECIMAL(5,2),
    
    -- Metadata
    created_timestamp TIMESTAMP DEFAULT NOW(),
    updated_timestamp TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(stats_date, exchange)
);

-- Create sequence for daily stats
CREATE SEQUENCE IF NOT EXISTS gold.daily_stats_seq START 1;

-- ============================================================================
-- WEEKLY ROLLING STATISTICS TABLES
-- ============================================================================

-- Gold layer table for 7-day rolling window statistics
CREATE TABLE IF NOT EXISTS gold.weekly_rolling_stats (
    id INTEGER PRIMARY KEY DEFAULT nextval('gold.weekly_rolling_seq'),
    
    -- Time dimension
    window_end_date DATE NOT NULL,
    window_start_date DATE NOT NULL,
    exchange VARCHAR NOT NULL,
    
    -- Rolling averages (7-day window)
    avg_daily_files_processed DECIMAL(10,2) DEFAULT 0,
    avg_daily_records_processed DECIMAL(15,2) DEFAULT 0,
    avg_daily_processing_time_hours DECIMAL(10,2) DEFAULT 0,
    avg_completion_rate DECIMAL(5,2) DEFAULT 0,
    avg_data_quality_score DECIMAL(5,2) DEFAULT 0,
    
    -- Rolling totals (7-day window)
    total_files_processed INTEGER DEFAULT 0,
    total_records_processed BIGINT DEFAULT 0,
    total_processing_time_hours DECIMAL(10,2) DEFAULT 0,
    total_data_volume_gb DECIMAL(10,2) DEFAULT 0,
    
    -- Performance trends
    processing_time_trend VARCHAR, -- 'improving', 'stable', 'degrading'
    throughput_trend VARCHAR, -- 'increasing', 'stable', 'decreasing'
    quality_trend VARCHAR, -- 'improving', 'stable', 'degrading'
    
    -- Variance metrics
    processing_time_variance DECIMAL(10,2),
    throughput_variance DECIMAL(10,2),
    quality_variance DECIMAL(10,2),
    
    -- Peak performance metrics
    peak_daily_throughput DECIMAL(15,2),
    peak_daily_throughput_date DATE,
    best_daily_quality_score DECIMAL(5,2),
    best_daily_quality_date DATE,
    
    -- Efficiency metrics
    resource_efficiency_score DECIMAL(5,2),
    processing_consistency_score DECIMAL(5,2),
    
    -- Metadata
    created_timestamp TIMESTAMP DEFAULT NOW(),
    updated_timestamp TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(window_end_date, exchange)
);

-- Create sequence for weekly rolling stats
CREATE SEQUENCE IF NOT EXISTS gold.weekly_rolling_seq START 1;

-- ============================================================================
-- HOURLY STATISTICS TABLE (for real-time monitoring)
-- ============================================================================

-- Table for hourly statistics (used for real-time dashboards)
CREATE TABLE IF NOT EXISTS silver.hourly_stats (
    id INTEGER PRIMARY KEY DEFAULT nextval('silver.hourly_stats_seq'),
    
    -- Time dimension
    stats_hour TIMESTAMP NOT NULL,
    exchange VARCHAR NOT NULL,
    
    -- Hourly metrics
    files_processed_this_hour INTEGER DEFAULT 0,
    records_processed_this_hour BIGINT DEFAULT 0,
    avg_processing_time_seconds DECIMAL(10,2),
    errors_this_hour INTEGER DEFAULT 0,
    
    -- Cumulative metrics (since start of day)
    cumulative_files_today INTEGER DEFAULT 0,
    cumulative_records_today BIGINT DEFAULT 0,
    cumulative_errors_today INTEGER DEFAULT 0,
    
    -- Performance indicators
    current_throughput_per_second DECIMAL(10,2),
    current_error_rate DECIMAL(5,2),
    
    -- Metadata
    created_timestamp TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(stats_hour, exchange)
);

-- Create sequence for hourly stats
CREATE SEQUENCE IF NOT EXISTS silver.hourly_stats_seq START 1;

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Daily stats indexes
CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON gold.daily_stats(stats_date);
CREATE INDEX IF NOT EXISTS idx_daily_stats_exchange ON gold.daily_stats(exchange);
CREATE INDEX IF NOT EXISTS idx_daily_stats_date_exchange ON gold.daily_stats(stats_date, exchange);

-- Weekly rolling stats indexes
CREATE INDEX IF NOT EXISTS idx_weekly_stats_window ON gold.weekly_rolling_stats(window_end_date, window_start_date);
CREATE INDEX IF NOT EXISTS idx_weekly_stats_exchange ON gold.weekly_rolling_stats(exchange);
CREATE INDEX IF NOT EXISTS idx_weekly_stats_date_exchange ON gold.weekly_rolling_stats(window_end_date, exchange);

-- Hourly stats indexes
CREATE INDEX IF NOT EXISTS idx_hourly_stats_hour ON silver.hourly_stats(stats_hour);
CREATE INDEX IF NOT EXISTS idx_hourly_stats_exchange ON silver.hourly_stats(exchange);
CREATE INDEX IF NOT EXISTS idx_hourly_stats_hour_exchange ON silver.hourly_stats(stats_hour, exchange);

-- ============================================================================
-- MATERIALIZED VIEWS FOR DASHBOARD QUERIES
-- ============================================================================

-- Current progress summary (refreshed every 5 minutes)
CREATE OR REPLACE VIEW gold.v_current_progress_dashboard AS
SELECT 
    'All Exchanges' as exchange,
    SUM(total_files_processed) as total_files_processed,
    SUM(total_files_completed) as total_files_completed,
    SUM(total_files_failed) as total_files_failed,
    ROUND(AVG(completion_rate), 2) as avg_completion_rate,
    SUM(total_records_processed) as total_records_processed,
    ROUND(AVG(data_quality_score), 2) as avg_data_quality_score,
    ROUND(AVG(avg_records_per_second), 2) as avg_records_per_second,
    MAX(stats_date) as last_updated_date
FROM gold.daily_stats
WHERE stats_date >= CURRENT_DATE - INTERVAL '7' DAY
UNION ALL
SELECT 
    exchange,
    SUM(total_files_processed) as total_files_processed,
    SUM(total_files_completed) as total_files_completed,
    SUM(total_files_failed) as total_files_failed,
    ROUND(AVG(completion_rate), 2) as avg_completion_rate,
    SUM(total_records_processed) as total_records_processed,
    ROUND(AVG(data_quality_score), 2) as avg_data_quality_score,
    ROUND(AVG(avg_records_per_second), 2) as avg_records_per_second,
    MAX(stats_date) as last_updated_date
FROM gold.daily_stats
WHERE stats_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY exchange
ORDER BY exchange;

-- Weekly trends summary
CREATE OR REPLACE VIEW gold.v_weekly_trends_dashboard AS
SELECT 
    exchange,
    window_end_date,
    avg_daily_files_processed,
    avg_daily_records_processed,
    avg_completion_rate,
    avg_data_quality_score,
    processing_time_trend,
    throughput_trend,
    quality_trend,
    resource_efficiency_score,
    processing_consistency_score
FROM gold.weekly_rolling_stats
WHERE window_end_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY window_end_date DESC, exchange;

-- Error analysis dashboard
CREATE OR REPLACE VIEW gold.v_error_analysis_dashboard AS
SELECT 
    DATE(error_timestamp) as error_date,
    error_type,
    COUNT(*) as error_count,
    COUNT(DISTINCT file_id) as affected_files,
    STRING_AGG(DISTINCT LEFT(error_message, 100), '; ') as sample_error_messages,
    AVG(CASE WHEN resolved THEN 1.0 ELSE 0.0 END) as resolution_rate
FROM bronze.nifi_load_errors
WHERE error_timestamp >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY DATE(error_timestamp), error_type
ORDER BY error_date DESC, error_count DESC;

-- ============================================================================
-- FUNCTIONS FOR STATISTICS CALCULATION
-- ============================================================================

-- Function to calculate daily statistics (called by NiFi)
CREATE OR REPLACE FUNCTION gold.calculate_daily_stats(target_date DATE, target_exchange VARCHAR)
RETURNS TABLE (
    success BOOLEAN,
    message VARCHAR,
    records_processed BIGINT,
    files_processed INTEGER
) AS $$
BEGIN
    -- Insert or update daily statistics
    INSERT INTO gold.daily_stats (
        stats_date, exchange, total_files_processed, total_files_completed, 
        total_files_failed, total_files_skipped, completion_rate,
        total_records_processed, total_records_loaded, total_records_rejected,
        data_quality_score, rejection_rate, avg_processing_time_seconds,
        min_processing_time_seconds, max_processing_time_seconds,
        total_processing_time_seconds, total_errors, error_rate,
        updated_timestamp
    )
    SELECT 
        target_date,
        target_exchange,
        COUNT(*) as total_files_processed,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as total_files_completed,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as total_files_failed,
        SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as total_files_skipped,
        ROUND(100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 2) as completion_rate,
        SUM(records_processed) as total_records_processed,
        SUM(records_loaded) as total_records_loaded,
        SUM(records_rejected) as total_records_rejected,
        ROUND(100.0 * SUM(records_loaded) / NULLIF(SUM(records_processed), 0), 2) as data_quality_score,
        ROUND(100.0 * SUM(records_rejected) / NULLIF(SUM(records_processed), 0), 2) as rejection_rate,
        AVG(processing_duration_seconds) as avg_processing_time_seconds,
        MIN(processing_duration_seconds) as min_processing_time_seconds,
        MAX(processing_duration_seconds) as max_processing_time_seconds,
        SUM(processing_duration_seconds) as total_processing_time_seconds,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as total_errors,
        ROUND(100.0 * SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_rate,
        NOW() as updated_timestamp
    FROM bronze.nifi_load_progress
    WHERE data_date = target_date
    AND exchange = target_exchange
    ON CONFLICT (stats_date, exchange) DO UPDATE SET
        total_files_processed = EXCLUDED.total_files_processed,
        total_files_completed = EXCLUDED.total_files_completed,
        total_files_failed = EXCLUDED.total_files_failed,
        total_files_skipped = EXCLUDED.total_files_skipped,
        completion_rate = EXCLUDED.completion_rate,
        total_records_processed = EXCLUDED.total_records_processed,
        total_records_loaded = EXCLUDED.total_records_loaded,
        total_records_rejected = EXCLUDED.total_records_rejected,
        data_quality_score = EXCLUDED.data_quality_score,
        rejection_rate = EXCLUDED.rejection_rate,
        avg_processing_time_seconds = EXCLUDED.avg_processing_time_seconds,
        min_processing_time_seconds = EXCLUDED.min_processing_time_seconds,
        max_processing_time_seconds = EXCLUDED.max_processing_time_seconds,
        total_processing_time_seconds = EXCLUDED.total_processing_time_seconds,
        total_errors = EXCLUDED.total_errors,
        error_rate = EXCLUDED.error_rate,
        updated_timestamp = NOW();
    
    RETURN QUERY
    SELECT 
        TRUE as success,
        'Daily statistics calculated successfully' as message,
        COALESCE(SUM(total_records_processed), 0) as records_processed,
        COALESCE(SUM(total_files_processed), 0) as files_processed
    FROM gold.daily_stats
    WHERE stats_date = target_date AND exchange = target_exchange;
END;
$$ LANGUAGE plpgsql; 