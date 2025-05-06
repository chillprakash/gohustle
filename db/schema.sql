-- TimescaleDB schema for tick data
-- Run this file to create the necessary tables and indexes

-- Enable TimescaleDB extension if not already enabled
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS nifty_ticks;
DROP TABLE IF EXISTS sensex_ticks;
DROP TABLE IF EXISTS credentials;
DROP TABLE IF EXISTS index_metrics;
DROP TABLE IF EXISTS straddle_metrics;

-- Create NIFTY ticks table
CREATE TABLE nifty_ticks (
    id BIGSERIAL,
    instrument_token INTEGER NOT NULL,
    exchange_unix_timestamp TIMESTAMPTZ NOT NULL,
    last_price DOUBLE PRECISION NOT NULL,
    open_interest INTEGER NOT NULL,
    volume_traded INTEGER NOT NULL,
    average_trade_price DOUBLE PRECISION NOT NULL,
    
    -- Metadata
    tick_received_time TIMESTAMPTZ NOT NULL,
    tick_stored_in_db_time TIMESTAMPTZ NOT NULL,
    
    -- Primary key must include the partitioning column for TimescaleDB
    PRIMARY KEY (id, exchange_unix_timestamp)
);

-- Create SENSEX ticks table with identical schema
CREATE TABLE sensex_ticks (
    id BIGSERIAL,
    instrument_token INTEGER NOT NULL,
    exchange_unix_timestamp TIMESTAMPTZ NOT NULL,
    last_price DOUBLE PRECISION NOT NULL,
    open_interest INTEGER NOT NULL,
    volume_traded INTEGER NOT NULL,
    average_trade_price DOUBLE PRECISION NOT NULL,
    
    -- Metadata
    tick_received_time TIMESTAMPTZ NOT NULL,
    tick_stored_in_db_time TIMESTAMPTZ NOT NULL,
    
    -- Primary key must include the partitioning column for TimescaleDB
    PRIMARY KEY (id, exchange_unix_timestamp)
);

-- Convert tables to TimescaleDB hypertables
-- This enables time-series optimizations
SELECT create_hypertable('nifty_ticks', 'exchange_unix_timestamp');
SELECT create_hypertable('sensex_ticks', 'exchange_unix_timestamp');

-- Create indexes for faster queries
-- Index on instrument_token for filtering
CREATE INDEX idx_nifty_instrument_token ON nifty_ticks (instrument_token);
CREATE INDEX idx_sensex_instrument_token ON sensex_ticks (instrument_token);

-- Compound index for time-series queries filtered by instrument
CREATE INDEX idx_nifty_instrument_time ON nifty_ticks (instrument_token, exchange_unix_timestamp DESC);
CREATE INDEX idx_sensex_instrument_time ON sensex_ticks (instrument_token, exchange_unix_timestamp DESC);

-- Create credentials table (migrated from SQLite)
CREATE TABLE credentials (
    id BIGSERIAL PRIMARY KEY,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create unique index on key to prevent duplicates
CREATE UNIQUE INDEX idx_credentials_key ON credentials (key);

-- Create index_metrics table for derived data
CREATE TABLE index_metrics (
    id BIGSERIAL,
    index_name TEXT NOT NULL,
    spot_price DOUBLE PRECISION NOT NULL,
    fair_price DOUBLE PRECISION NOT NULL,
    straddle_price DOUBLE PRECISION NOT NULL,
    atm_strike DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    
    -- Primary key must include the partitioning column for TimescaleDB
    PRIMARY KEY (id, timestamp)
);

-- Convert index_metrics to TimescaleDB hypertable
SELECT create_hypertable('index_metrics', 'timestamp');
Sign in to enable AI completions, or disable inline completions in Settings (DBCode > AI).
-- Create indexes for derived data tables
CREATE INDEX idx_index_metrics_name ON index_metrics (index_name);
CREATE INDEX idx_index_metrics_name_time ON index_metrics (index_name, timestamp DESC);

-- Add table comments
COMMENT ON TABLE nifty_ticks IS 'Time-series data for NIFTY instruments';
COMMENT ON TABLE sensex_ticks IS 'Time-series data for SENSEX instruments';
COMMENT ON TABLE credentials IS 'API credentials and configuration values';
COMMENT ON TABLE index_metrics IS 'Derived metrics for indices (spot, fair, straddle prices)';
COMMENT ON TABLE straddle_metrics IS 'Detailed straddle metrics per strike price';

CREATE TABLE orders (
    id                  SERIAL PRIMARY KEY,
    order_id            VARCHAR(64),           -- Zerodha order ID or GTT trigger ID
    order_type          VARCHAR(16),           -- MARKET, LIMIT, SL, SL-M, GTT
    gtt_type            VARCHAR(16),           -- NULL for regular, 'single'/'oco' for GTT
    status              VARCHAR(32),           -- Status from Zerodha (e.g., 'success', 'rejected')
    message             TEXT,                  -- Any message from Zerodha
    trading_symbol      VARCHAR(32) NOT NULL,
    exchange            VARCHAR(16) NOT NULL,
    side                VARCHAR(8),            -- BUY/SELL
    quantity            INT,
    price               NUMERIC(12,2),
    trigger_price       NUMERIC(12,2),
    product             VARCHAR(8),
    validity            VARCHAR(8),
    disclosed_qty       INT,
    tag                 VARCHAR(64),
    user_id             VARCHAR(64),           -- If you support multi-user
    placed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- When the order was placed
    kite_response       JSONB,                 -- Raw Zerodha response for audit/debug
    paper_trading       BOOLEAN DEFAULT FALSE  -- Whether this is a paper trade (not sent to broker)
);

CREATE TABLE strategies (
    id                SERIAL PRIMARY KEY,
    name              VARCHAR(64) UNIQUE NOT NULL,  -- Unique strategy name
    description       TEXT,                         -- Strategy description
    type              VARCHAR(32),                  -- e.g., "options", "futures", "equity"
    parameters        JSONB,                        -- Strategy-specific parameters
    entry_rules       JSONB,                        -- Entry conditions
    exit_rules        JSONB,                        -- Exit conditions
    risk_parameters   JSONB,                        -- Risk management parameters
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    active            BOOLEAN DEFAULT TRUE,         -- Whether strategy is active
    creator_id        VARCHAR(64)                   -- User who created the strategy
);

CREATE TABLE positions (
    id                  SERIAL PRIMARY KEY,
    position_id         VARCHAR(64),           -- Unique identifier for the position
    trading_symbol      VARCHAR(32) NOT NULL,
    exchange            VARCHAR(16) NOT NULL,
    product             VARCHAR(8),            -- NRML, MIS, CNC
    quantity            INT,                   -- Current position quantity (+ for long, - for short)
    average_price       NUMERIC(12,2),         -- Average price of the position
    last_price          NUMERIC(12,2),         -- Latest market price
    pnl                 NUMERIC(12,2),         -- Current P&L
    realized_pnl        NUMERIC(12,2),         -- Realized P&L
    unrealized_pnl      NUMERIC(12,2),         -- Unrealized P&L
    multiplier          NUMERIC(10,2),         -- Contract multiplier (for derivatives)
    buy_quantity        INT,                   -- Total buy quantity
    sell_quantity       INT,                   -- Total sell quantity
    buy_price           NUMERIC(12,2),         -- Average buy price
    sell_price          NUMERIC(12,2),         -- Average sell price
    buy_value           NUMERIC(16,2),         -- Total buy value
    sell_value          NUMERIC(16,2),         -- Total sell value
    position_type       VARCHAR(8),            -- 'net', 'day'
    strategy            VARCHAR(64),           -- Strategy name (e.g., 'iron_condor', 'straddle', etc.)
    user_id             VARCHAR(64),           -- User ID
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- When the position was last updated
    paper_trading       BOOLEAN DEFAULT FALSE,  -- Whether this is a paper trade
    kite_response       JSONB,                 -- Raw Zerodha response for audit/debug
    strategy_id         INTEGER REFERENCES strategies(id), -- Reference to strategy table
    UNIQUE(position_id)
);


-- Create strategy_pnl_timeseries table for tracking strategy P&L over time
CREATE TABLE strategy_pnl_timeseries (
    id BIGSERIAL,
    strategy_id INTEGER NOT NULL,        -- Strategy ID
    strategy_name TEXT NOT NULL,         -- Strategy name for easier querying
    total_pnl DOUBLE PRECISION NOT NULL, -- Total P&L for the strategy
    paper_trading BOOLEAN NOT NULL,      -- Whether this is paper trading or real
    timestamp TIMESTAMPTZ NOT NULL,      -- Timestamp of the P&L snapshot
    
    -- Primary key must include the partitioning column for TimescaleDB
    PRIMARY KEY (id, timestamp)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable('strategy_pnl_timeseries', 'timestamp');

-- Create indexes for faster queries
CREATE INDEX idx_strategy_pnl_strategy_id ON strategy_pnl_timeseries (strategy_id, timestamp DESC);
CREATE INDEX idx_strategy_pnl_paper_trading ON strategy_pnl_timeseries (paper_trading, timestamp DESC);

-- Add compression policy (compress data older than 1 day)
SELECT add_compression_policy('strategy_pnl_timeseries', INTERVAL '1 day');

-- Add retention policy (keep data for 90 days)
SELECT add_retention_policy('strategy_pnl_timeseries', INTERVAL '90 days');

-- Create continuous aggregate for hourly data
CREATE MATERIALIZED VIEW strategy_pnl_hourly WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS bucket,
    strategy_id,
    strategy_name,
    paper_trading,
    AVG(total_pnl) AS avg_pnl,
    MIN(total_pnl) AS min_pnl,
    MAX(total_pnl) AS max_pnl,
    LAST(total_pnl, timestamp) AS last_pnl
FROM strategy_pnl_timeseries
GROUP BY bucket, strategy_id, strategy_name, paper_trading;

-- Create tick_archive_jobs table for tracking tick data archiving processes
CREATE TABLE tick_archive_jobs (
    id SERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    index_name TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL, -- 'pending', 'running', 'completed', 'failed', 'failed_permanent'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    tick_count INT,
    file_path TEXT,
    file_size_bytes BIGINT,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    next_retry_at TIMESTAMPTZ
);

-- Create indexes for faster queries
CREATE INDEX idx_tick_archive_jobs_status ON tick_archive_jobs(status);
CREATE INDEX idx_tick_archive_jobs_index_date ON tick_archive_jobs(index_name, start_time);
CREATE INDEX idx_tick_archive_jobs_next_retry ON tick_archive_jobs(next_retry_at) WHERE next_retry_at IS NOT NULL;

-- Add table comment
COMMENT ON TABLE tick_archive_jobs IS 'Tracks tick data archiving processes for export to Parquet and deletion';
