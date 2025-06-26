-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS audit;

-- Bronze Layer Tables (Raw data from each exchange)
CREATE TABLE IF NOT EXISTS bronze.lse_market_data_raw (
    symbol VARCHAR,
    timestamp_utc TIMESTAMP,
    price DECIMAL(15,6),
    volume BIGINT,
    bid_price DECIMAL(15,6),
    ask_price DECIMAL(15,6),
    bid_size BIGINT,
    ask_size BIGINT,
    trade_type VARCHAR,
    market_mechanism VARCHAR,
    participant_id VARCHAR,
    order_book_id VARCHAR,
    tick_direction VARCHAR,
    trade_flags VARCHAR,
    currency VARCHAR,
    market_segment VARCHAR,
    instrument_type VARCHAR,
    
    -- Metadata columns
    data_date DATE,
    exchange VARCHAR,
    processing_stage VARCHAR,
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.cme_market_data_raw (
    symbol VARCHAR,
    timestamp_utc TIMESTAMP,
    price DECIMAL(15,6),
    volume BIGINT,
    bid_price DECIMAL(15,6),
    ask_price DECIMAL(15,6),
    bid_size BIGINT,
    ask_size BIGINT,
    trade_type VARCHAR,
    settlement_price DECIMAL(15,6),
    open_interest BIGINT,
    contract_month VARCHAR,
    product_group VARCHAR,
    contract_size INTEGER,
    tick_size DECIMAL(10,8),
    
    -- Metadata columns
    data_date DATE,
    exchange VARCHAR,
    processing_stage VARCHAR,
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.nyq_market_data_raw (
    symbol VARCHAR,
    timestamp_utc TIMESTAMP,
    price DECIMAL(15,6),
    volume BIGINT,
    bid_price DECIMAL(15,6),
    ask_price DECIMAL(15,6),
    bid_size BIGINT,
    ask_size BIGINT,
    trade_type VARCHAR,
    sector VARCHAR,
    market_cap BIGINT,
    listing_exchange VARCHAR,
    primary_exchange VARCHAR,
    security_type VARCHAR,
    dividend_yield DECIMAL(8,4),
    pe_ratio DECIMAL(8,2),
    
    -- Metadata columns
    data_date DATE,
    exchange VARCHAR,
    processing_stage VARCHAR,
    source_file VARCHAR,
    ingestion_timestamp TIMESTAMP
);

-- Audit Tables
CREATE TABLE IF NOT EXISTS audit.data_ingestion_log (
    log_id INTEGER PRIMARY KEY,
    exchange VARCHAR,
    processing_stage VARCHAR,
    data_date DATE,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_count BIGINT,
    file_size BIGINT,
    processing_status VARCHAR,
    error_message VARCHAR,
    source_file VARCHAR,
    processing_duration_seconds INTEGER
);

CREATE TABLE IF NOT EXISTS audit.pipeline_state (
    pipeline_name VARCHAR,
    exchange VARCHAR,
    processing_stage VARCHAR,
    last_processed_date DATE,
    processing_status VARCHAR,
    records_processed BIGINT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline_name, exchange, processing_stage)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_lse_date_symbol ON bronze.lse_market_data_raw(data_date, symbol);
CREATE INDEX IF NOT EXISTS idx_lse_timestamp ON bronze.lse_market_data_raw(timestamp_utc);

CREATE INDEX IF NOT EXISTS idx_cme_date_symbol ON bronze.cme_market_data_raw(data_date, symbol);
CREATE INDEX IF NOT EXISTS idx_cme_timestamp ON bronze.cme_market_data_raw(timestamp_utc);

CREATE INDEX IF NOT EXISTS idx_nyq_date_symbol ON bronze.nyq_market_data_raw(data_date, symbol);
CREATE INDEX IF NOT EXISTS idx_nyq_timestamp ON bronze.nyq_market_data_raw(timestamp_utc); 