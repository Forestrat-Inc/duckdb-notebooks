-- NiFi Progress Tracking Tables for January 2025 Data Loading
-- Created for DuckDB integration with NiFi 2.3.0

-- Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- PROGRESS TRACKING TABLES
-- ============================================================================

-- Table to track individual file processing progress
CREATE TABLE IF NOT EXISTS bronze.nifi_load_progress (
    id INTEGER PRIMARY KEY DEFAULT nextval('bronze.nifi_progress_seq'),
    file_id VARCHAR NOT NULL UNIQUE,
    exchange VARCHAR NOT NULL,
    processing_stage VARCHAR NOT NULL,
    data_date DATE NOT NULL,
    source_file_path VARCHAR NOT NULL,
    file_size_bytes BIGINT,
    
    -- Processing status
    status VARCHAR NOT NULL DEFAULT 'queued', -- queued, processing, completed, failed, skipped
    
    -- Timing information
    queued_timestamp TIMESTAMP DEFAULT NOW(),
    processing_start_timestamp TIMESTAMP,
    processing_end_timestamp TIMESTAMP,
    processing_duration_seconds DECIMAL(10,2),
    
    -- Processing results
    records_processed BIGINT DEFAULT 0,
    records_loaded BIGINT DEFAULT 0,
    records_rejected BIGINT DEFAULT 0,
    
    -- Error information
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    
    -- Metadata
    nifi_processor_id VARCHAR,
    nifi_flow_file_uuid VARCHAR,
    created_timestamp TIMESTAMP DEFAULT NOW(),
    updated_timestamp TIMESTAMP DEFAULT NOW()
);

-- Create sequence for progress tracking
CREATE SEQUENCE IF NOT EXISTS bronze.nifi_progress_seq START 1;

-- Table for real-time statistics and monitoring
CREATE TABLE IF NOT EXISTS bronze.nifi_load_stats (
    id INTEGER PRIMARY KEY DEFAULT nextval('bronze.nifi_stats_seq'),
    
    -- Time window
    stats_timestamp TIMESTAMP DEFAULT NOW(),
    stats_window_start TIMESTAMP NOT NULL,
    stats_window_end TIMESTAMP NOT NULL,
    
    -- Aggregation level
    aggregation_level VARCHAR NOT NULL, -- 'real-time', 'hourly', 'daily'
    exchange VARCHAR, -- NULL for all exchanges
    
    -- File-level statistics
    total_files_queued INTEGER DEFAULT 0,
    total_files_processing INTEGER DEFAULT 0,
    total_files_completed INTEGER DEFAULT 0,
    total_files_failed INTEGER DEFAULT 0,
    total_files_skipped INTEGER DEFAULT 0,
    
    -- Record-level statistics
    total_records_processed BIGINT DEFAULT 0,
    total_records_loaded BIGINT DEFAULT 0,
    total_records_rejected BIGINT DEFAULT 0,
    
    -- Performance metrics
    avg_processing_time_seconds DECIMAL(10,2),
    min_processing_time_seconds DECIMAL(10,2),
    max_processing_time_seconds DECIMAL(10,2),
    total_processing_time_seconds DECIMAL(10,2),
    
    -- Throughput metrics
    records_per_second DECIMAL(10,2),
    files_per_minute DECIMAL(10,2),
    bytes_per_second BIGINT,
    
    -- Progress metrics
    completion_percentage DECIMAL(5,2),
    estimated_completion_time TIMESTAMP,
    
    -- Data quality metrics
    data_quality_score DECIMAL(5,2),
    rejection_rate DECIMAL(5,2),
    
    created_timestamp TIMESTAMP DEFAULT NOW()
);

-- Create sequence for stats
CREATE SEQUENCE IF NOT EXISTS bronze.nifi_stats_seq START 1;

-- Table for error tracking and analysis
CREATE TABLE IF NOT EXISTS bronze.nifi_load_errors (
    id INTEGER PRIMARY KEY DEFAULT nextval('bronze.nifi_errors_seq'),
    
    -- Reference to progress record
    progress_id INTEGER REFERENCES bronze.nifi_load_progress(id),
    file_id VARCHAR NOT NULL,
    
    -- Error details
    error_type VARCHAR NOT NULL, -- 'validation', 'processing', 'database', 'network', 'timeout'
    error_code VARCHAR,
    error_message TEXT NOT NULL,
    error_stack_trace TEXT,
    
    -- Context information
    processor_name VARCHAR,
    processor_id VARCHAR,
    flow_file_uuid VARCHAR,
    
    -- Timing
    error_timestamp TIMESTAMP DEFAULT NOW(),
    
    -- Resolution
    resolved BOOLEAN DEFAULT FALSE,
    resolution_notes TEXT,
    resolved_timestamp TIMESTAMP,
    resolved_by VARCHAR,
    
    -- Metadata
    additional_context JSON,
    created_timestamp TIMESTAMP DEFAULT NOW()
);

-- Create sequence for errors
CREATE SEQUENCE IF NOT EXISTS bronze.nifi_errors_seq START 1;

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Progress tracking indexes
CREATE INDEX IF NOT EXISTS idx_nifi_progress_file_id ON bronze.nifi_load_progress(file_id);
CREATE INDEX IF NOT EXISTS idx_nifi_progress_status ON bronze.nifi_load_progress(status);
CREATE INDEX IF NOT EXISTS idx_nifi_progress_exchange_date ON bronze.nifi_load_progress(exchange, data_date);
CREATE INDEX IF NOT EXISTS idx_nifi_progress_timestamps ON bronze.nifi_load_progress(processing_start_timestamp, processing_end_timestamp);

-- Stats indexes
CREATE INDEX IF NOT EXISTS idx_nifi_stats_timestamp ON bronze.nifi_load_stats(stats_timestamp);
CREATE INDEX IF NOT EXISTS idx_nifi_stats_window ON bronze.nifi_load_stats(stats_window_start, stats_window_end);
CREATE INDEX IF NOT EXISTS idx_nifi_stats_aggregation ON bronze.nifi_load_stats(aggregation_level, exchange);

-- Error tracking indexes
CREATE INDEX IF NOT EXISTS idx_nifi_errors_file_id ON bronze.nifi_load_errors(file_id);
CREATE INDEX IF NOT EXISTS idx_nifi_errors_type ON bronze.nifi_load_errors(error_type);
CREATE INDEX IF NOT EXISTS idx_nifi_errors_timestamp ON bronze.nifi_load_errors(error_timestamp);
CREATE INDEX IF NOT EXISTS idx_nifi_errors_resolved ON bronze.nifi_load_errors(resolved);

-- ============================================================================
-- VIEWS FOR EASY QUERYING
-- ============================================================================

-- Real-time progress view
CREATE OR REPLACE VIEW bronze.v_nifi_progress_summary AS
SELECT 
    exchange,
    COUNT(*) as total_files,
    SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) as queued_files,
    SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing_files,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_files,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_files,
    SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped_files,
    SUM(records_loaded) as total_records_loaded,
    AVG(processing_duration_seconds) as avg_processing_time,
    ROUND(100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 2) as completion_percentage
FROM bronze.nifi_load_progress
GROUP BY exchange
ORDER BY exchange;

-- Error analysis view
CREATE OR REPLACE VIEW bronze.v_nifi_error_analysis AS
SELECT 
    error_type,
    COUNT(*) as error_count,
    COUNT(DISTINCT file_id) as affected_files,
    AVG(CASE WHEN resolved THEN 1.0 ELSE 0.0 END) as resolution_rate,
    MIN(error_timestamp) as first_occurrence,
    MAX(error_timestamp) as last_occurrence
FROM bronze.nifi_load_errors
GROUP BY error_type
ORDER BY error_count DESC;

-- Processing performance view
CREATE OR REPLACE VIEW bronze.v_nifi_performance_metrics AS
SELECT 
    DATE(processing_start_timestamp) as processing_date,
    exchange,
    COUNT(*) as files_processed,
    SUM(records_loaded) as total_records,
    AVG(processing_duration_seconds) as avg_processing_time,
    AVG(records_loaded::DECIMAL / NULLIF(processing_duration_seconds, 0)) as avg_records_per_second,
    MIN(processing_duration_seconds) as min_processing_time,
    MAX(processing_duration_seconds) as max_processing_time
FROM bronze.nifi_load_progress
WHERE status = 'completed'
AND processing_start_timestamp IS NOT NULL
AND processing_end_timestamp IS NOT NULL
GROUP BY DATE(processing_start_timestamp), exchange
ORDER BY processing_date DESC, exchange; 