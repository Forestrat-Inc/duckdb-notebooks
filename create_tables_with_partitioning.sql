-- =====================================================
-- DuckDB Tables with Partitioning for Multi-Exchange Data Lake
-- =====================================================
-- This script creates tables that map to the updated S3 structure
-- with support for LSE, CME, NYQ exchanges and multiple processing stages

-- Create enhanced schemas for multi-exchange support
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS views;

-- =====================================================
-- BRONZE LAYER TABLES (Raw Ingestion)
-- =====================================================

-- LSE Raw Market Data (Ingestion Stage)
CREATE TABLE IF NOT EXISTS bronze.lse_market_data_raw (
    -- Trading Data Fields
    date_time TIMESTAMP,
    symbol VARCHAR,
    message_type VARCHAR,
    exchange_time TIMESTAMP,
    price DECIMAL(18,6),
    quantity BIGINT,
    side VARCHAR(4), -- BUY/SELL
    trade_id VARCHAR,
    order_id VARCHAR,
    bid_price DECIMAL(18,6),
    ask_price DECIMAL(18,6),
    bid_size BIGINT,
    ask_size BIGINT,
    
    -- Market Data Fields
    high_price DECIMAL(18,6),
    low_price DECIMAL(18,6),
    open_price DECIMAL(18,6),
    close_price DECIMAL(18,6),
    volume BIGINT,
    turnover DECIMAL(18,2),
    vwap DECIMAL(18,6),
    
    -- Metadata Fields
    data_date DATE,
    exchange VARCHAR DEFAULT 'LSE',
    processing_stage VARCHAR DEFAULT 'ingestion',
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    
    -- Partitioning
    PRIMARY KEY (data_date, symbol, exchange_time)
) PARTITION BY (data_date, exchange);

-- CME Raw Market Data (Ingestion Stage)
CREATE TABLE IF NOT EXISTS bronze.cme_market_data_raw (
    -- Trading Data Fields (CME specific structure)
    date_time TIMESTAMP,
    symbol VARCHAR,
    message_type VARCHAR,
    exchange_time TIMESTAMP,
    price DECIMAL(18,6),
    quantity BIGINT,
    side VARCHAR(4),
    trade_id VARCHAR,
    order_id VARCHAR,
    bid_price DECIMAL(18,6),
    ask_price DECIMAL(18,6),
    bid_size BIGINT,
    ask_size BIGINT,
    
    -- CME Specific Fields
    contract_month VARCHAR,
    contract_year INTEGER,
    settlement_price DECIMAL(18,6),
    open_interest BIGINT,
    
    -- Market Data Fields
    high_price DECIMAL(18,6),
    low_price DECIMAL(18,6),
    open_price DECIMAL(18,6),
    close_price DECIMAL(18,6),
    volume BIGINT,
    turnover DECIMAL(18,2),
    
    -- Metadata Fields
    data_date DATE,
    exchange VARCHAR DEFAULT 'CME',
    processing_stage VARCHAR DEFAULT 'ingestion',
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    
    -- Partitioning
    PRIMARY KEY (data_date, symbol, exchange_time)
) PARTITION BY (data_date, exchange);

-- NYQ Raw Market Data (Ingestion Stage)
CREATE TABLE IF NOT EXISTS bronze.nyq_market_data_raw (
    -- Trading Data Fields
    date_time TIMESTAMP,
    symbol VARCHAR,
    message_type VARCHAR,
    exchange_time TIMESTAMP,
    price DECIMAL(18,6),
    quantity BIGINT,
    side VARCHAR(4),
    trade_id VARCHAR,
    order_id VARCHAR,
    bid_price DECIMAL(18,6),
    ask_price DECIMAL(18,6),
    bid_size BIGINT,
    ask_size BIGINT,
    
    -- Market Data Fields
    high_price DECIMAL(18,6),
    low_price DECIMAL(18,6),
    open_price DECIMAL(18,6),
    close_price DECIMAL(18,6),
    volume BIGINT,
    turnover DECIMAL(18,2),
    vwap DECIMAL(18,6),
    
    -- NYQ Specific Fields
    market_cap DECIMAL(18,2),
    sector VARCHAR,
    
    -- Metadata Fields
    data_date DATE,
    exchange VARCHAR DEFAULT 'NYQ',
    processing_stage VARCHAR DEFAULT 'ingestion',
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    
    -- Partitioning
    PRIMARY KEY (data_date, symbol, exchange_time)
) PARTITION BY (data_date, exchange);

-- Universal Market Data Reports Table
CREATE TABLE IF NOT EXISTS bronze.market_data_reports (
    report_id UUID DEFAULT gen_random_uuid(),
    exchange VARCHAR,
    data_date DATE,
    report_type VARCHAR, -- 'NORMALIZEDMP', 'SUMMARY', etc.
    
    -- Report Content
    total_trades BIGINT,
    total_volume BIGINT,
    total_turnover DECIMAL(18,2),
    unique_symbols INTEGER,
    processing_errors INTEGER,
    data_quality_score DECIMAL(5,2),
    
    -- File Information
    source_file VARCHAR,
    file_size BIGINT,
    processing_time_seconds INTEGER,
    
    -- Metadata
    processing_stage VARCHAR DEFAULT 'ingestion',
    created_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (report_id)
) PARTITION BY (data_date, exchange);

-- =====================================================
-- SILVER LAYER TABLES (Cleaned & Normalized)
-- =====================================================

-- Unified Market Data (All Exchanges)
CREATE TABLE IF NOT EXISTS silver.market_data_unified (
    trade_id UUID DEFAULT gen_random_uuid(),
    
    -- Standardized Trading Fields
    exchange VARCHAR,
    symbol VARCHAR,
    exchange_time TIMESTAMP,
    price DECIMAL(18,6),
    quantity BIGINT,
    side VARCHAR(4),
    
    -- Standardized Market Data
    bid_price DECIMAL(18,6),
    ask_price DECIMAL(18,6),
    bid_size BIGINT,
    ask_size BIGINT,
    spread DECIMAL(18,6), -- Calculated field
    
    -- OHLCV Data
    open_price DECIMAL(18,6),
    high_price DECIMAL(18,6),
    low_price DECIMAL(18,6),
    close_price DECIMAL(18,6),
    volume BIGINT,
    turnover DECIMAL(18,2),
    vwap DECIMAL(18,6),
    
    -- Data Quality Flags
    is_valid_trade BOOLEAN DEFAULT true,
    quality_score DECIMAL(5,2) DEFAULT 100.0,
    anomaly_flags VARCHAR[],
    
    -- Metadata
    data_date DATE,
    processing_timestamp TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (trade_id)
) PARTITION BY (data_date, exchange);

-- Price Time Series (For Analytics)
CREATE TABLE IF NOT EXISTS silver.price_timeseries (
    ts_id UUID DEFAULT gen_random_uuid(),
    exchange VARCHAR,
    symbol VARCHAR,
    timestamp TIMESTAMP,
    
    -- Price Data
    price DECIMAL(18,6),
    volume BIGINT,
    
    -- Technical Indicators (computed)
    sma_10 DECIMAL(18,6),  -- 10-period Simple Moving Average
    sma_50 DECIMAL(18,6),  -- 50-period Simple Moving Average
    rsi DECIMAL(5,2),      -- Relative Strength Index
    
    -- Metadata  
    data_date DATE,
    
    PRIMARY KEY (ts_id)
) PARTITION BY (data_date, exchange);

-- =====================================================
-- GOLD LAYER TABLES (Analytics Ready)
-- =====================================================

-- Daily Market Summary
CREATE TABLE IF NOT EXISTS gold.daily_market_summary (
    summary_id UUID DEFAULT gen_random_uuid(),
    exchange VARCHAR,
    symbol VARCHAR,
    data_date DATE,
    
    -- OHLCV Summary
    open_price DECIMAL(18,6),
    high_price DECIMAL(18,6),
    low_price DECIMAL(18,6),
    close_price DECIMAL(18,6),
    volume BIGINT,
    turnover DECIMAL(18,2),
    vwap DECIMAL(18,6),
    
    -- Trading Statistics
    trade_count INTEGER,
    avg_trade_size DECIMAL(18,6),
    price_volatility DECIMAL(18,6),
    
    -- Cross-Exchange Metrics
    price_difference_vs_other_exchanges DECIMAL(18,6),
    volume_rank INTEGER, -- Rank among all exchanges for this symbol
    
    PRIMARY KEY (summary_id)
) PARTITION BY (data_date, exchange);

-- Cross-Exchange Arbitrage Opportunities
CREATE TABLE IF NOT EXISTS gold.arbitrage_opportunities (
    opportunity_id UUID DEFAULT gen_random_uuid(),
    symbol VARCHAR,
    timestamp TIMESTAMP,
    
    -- Price Differences
    exchange_1 VARCHAR,
    exchange_2 VARCHAR,
    price_1 DECIMAL(18,6),
    price_2 DECIMAL(18,6),
    price_spread DECIMAL(18,6),
    spread_percentage DECIMAL(5,2),
    
    -- Volume Information
    volume_1 BIGINT,
    volume_2 BIGINT,
    potential_profit DECIMAL(18,2),
    
    -- Metadata
    data_date DATE,
    opportunity_duration_seconds INTEGER,
    
    PRIMARY KEY (opportunity_id)
) PARTITION BY (data_date);

-- =====================================================
-- AUDIT & LINEAGE TABLES
-- =====================================================

-- Enhanced Data Ingestion Log
CREATE OR REPLACE TABLE audit.data_ingestion_log (
    ingestion_id UUID DEFAULT gen_random_uuid(),
    exchange VARCHAR,
    processing_stage VARCHAR,
    source_path VARCHAR,
    file_size BIGINT,
    record_count BIGINT,
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    data_date DATE,
    processing_status VARCHAR DEFAULT 'bronze',
    error_message VARCHAR,
    processing_duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (ingestion_id)
) PARTITION BY (data_date, exchange);

-- Pipeline State Tracking (Enhanced)
CREATE OR REPLACE TABLE audit.pipeline_state (
    pipeline_name VARCHAR,
    exchange VARCHAR,
    processing_stage VARCHAR,
    last_processed_date DATE,
    last_processed_file VARCHAR,
    processing_status VARCHAR,
    records_processed BIGINT,
    last_updated TIMESTAMP DEFAULT NOW(),
    error_message VARCHAR,
    next_scheduled_run TIMESTAMP,
    
    PRIMARY KEY (pipeline_name, exchange, processing_stage)
);

-- Data Quality Metrics
CREATE OR REPLACE TABLE audit.data_quality_metrics (
    quality_id UUID DEFAULT gen_random_uuid(),
    exchange VARCHAR,
    table_name VARCHAR,
    data_date DATE,
    
    -- Quality Metrics
    total_records BIGINT,
    valid_records BIGINT,
    invalid_records BIGINT,
    null_price_count BIGINT,
    null_volume_count BIGINT,
    duplicate_count BIGINT,
    anomaly_count BIGINT,
    
    -- Quality Scores
    completeness_score DECIMAL(5,2),
    accuracy_score DECIMAL(5,2),
    consistency_score DECIMAL(5,2),
    overall_quality_score DECIMAL(5,2),
    
    -- Metadata
    check_timestamp TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (quality_id)
) PARTITION BY (data_date, exchange);

-- =====================================================
-- VIEWS FOR EASY ACCESS
-- =====================================================

-- Latest Market Data View
CREATE OR REPLACE VIEW views.latest_market_data AS
SELECT 
    exchange,
    symbol,
    price,
    volume,
    bid_price,
    ask_price,
    spread,
    exchange_time,
    data_date,
    ROW_NUMBER() OVER (PARTITION BY exchange, symbol ORDER BY exchange_time DESC) as rn
FROM silver.market_data_unified
WHERE data_date >= CURRENT_DATE - INTERVAL '7 days'
QUALIFY rn = 1;

-- Cross-Exchange Price Comparison
CREATE OR REPLACE VIEW views.cross_exchange_prices AS
SELECT 
    symbol,
    MAX(CASE WHEN exchange = 'LSE' THEN price END) as lse_price,
    MAX(CASE WHEN exchange = 'CME' THEN price END) as cme_price,
    MAX(CASE WHEN exchange = 'NYQ' THEN price END) as nyq_price,
    MAX(exchange_time) as latest_time,
    CURRENT_DATE as data_date
FROM views.latest_market_data
GROUP BY symbol;

-- Daily Volume Leaders
CREATE OR REPLACE VIEW views.daily_volume_leaders AS
SELECT 
    exchange,
    symbol,
    volume,
    turnover,
    data_date,
    RANK() OVER (PARTITION BY exchange, data_date ORDER BY volume DESC) as volume_rank
FROM gold.daily_market_summary
WHERE data_date >= CURRENT_DATE - INTERVAL '30 days';

-- Pipeline Health Dashboard
CREATE OR REPLACE VIEW views.pipeline_health AS
SELECT 
    exchange,
    processing_stage,
    processing_status,
    last_processed_date,
    CURRENT_DATE - last_processed_date as days_behind,
    records_processed,
    last_updated,
    CASE 
        WHEN CURRENT_DATE - last_processed_date <= 1 THEN 'HEALTHY'
        WHEN CURRENT_DATE - last_processed_date <= 3 THEN 'WARNING'
        ELSE 'CRITICAL'
    END as health_status
FROM audit.pipeline_state
ORDER BY days_behind DESC;

-- =====================================================
-- HELPER FUNCTIONS
-- =====================================================

-- Function to get the latest S3 path for an exchange and date
CREATE OR REPLACE FUNCTION get_s3_path(exchange_name VARCHAR, date_val DATE, processing_stage VARCHAR DEFAULT 'ingestion')
RETURNS VARCHAR AS (
    RETURN 's3://vendor-data-s3/LSEG/TRTH/' || UPPER(exchange_name) || '/' || processing_stage || '/' || date_val::VARCHAR || '/data/merged/' || UPPER(exchange_name) || '-' || date_val::VARCHAR || '-NORMALIZEDMP-Data-1-of-1.csv.gz';
);

-- Function to get report path
CREATE OR REPLACE FUNCTION get_s3_report_path(exchange_name VARCHAR, date_val DATE, processing_stage VARCHAR DEFAULT 'ingestion')
RETURNS VARCHAR AS (
    RETURN 's3://vendor-data-s3/LSEG/TRTH/' || UPPER(exchange_name) || '/' || processing_stage || '/' || date_val::VARCHAR || '/data/merged/' || UPPER(exchange_name) || '-' || date_val::VARCHAR || '-NORMALIZEDMP-Report-1-of-1.csv.gz';
);

-- Function to load data for a specific exchange and date
CREATE OR REPLACE FUNCTION load_exchange_data(exchange_name VARCHAR, date_val DATE)
RETURNS VARCHAR AS (
    -- This will be implemented as a stored procedure for data loading
    RETURN 'Data loading function for ' || exchange_name || ' on ' || date_val::VARCHAR;
);

-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Indexes on bronze tables
CREATE INDEX IF NOT EXISTS idx_lse_raw_date_symbol ON bronze.lse_market_data_raw(data_date, symbol);
CREATE INDEX IF NOT EXISTS idx_lse_raw_exchange_time ON bronze.lse_market_data_raw(exchange_time);
CREATE INDEX IF NOT EXISTS idx_cme_raw_date_symbol ON bronze.cme_market_data_raw(data_date, symbol);
CREATE INDEX IF NOT EXISTS idx_cme_raw_exchange_time ON bronze.cme_market_data_raw(exchange_time);
CREATE INDEX IF NOT EXISTS idx_nyq_raw_date_symbol ON bronze.nyq_market_data_raw(data_date, symbol);
CREATE INDEX IF NOT EXISTS idx_nyq_raw_exchange_time ON bronze.nyq_market_data_raw(exchange_time);

-- Indexes on silver tables
CREATE INDEX IF NOT EXISTS idx_unified_date_exchange_symbol ON silver.market_data_unified(data_date, exchange, symbol);
CREATE INDEX IF NOT EXISTS idx_unified_exchange_time ON silver.market_data_unified(exchange_time);
CREATE INDEX IF NOT EXISTS idx_timeseries_date_exchange_symbol ON silver.price_timeseries(data_date, exchange, symbol);

-- Indexes on gold tables
CREATE INDEX IF NOT EXISTS idx_daily_summary_date_exchange ON gold.daily_market_summary(data_date, exchange);
CREATE INDEX IF NOT EXISTS idx_arbitrage_date_symbol ON gold.arbitrage_opportunities(data_date, symbol);

-- Indexes on audit tables
CREATE INDEX IF NOT EXISTS idx_ingestion_log_date_exchange ON audit.data_ingestion_log(data_date, exchange);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_date_exchange ON audit.data_quality_metrics(data_date, exchange); 