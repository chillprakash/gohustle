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

CREATE TABLE positions (
    id                  SERIAL PRIMARY KEY,
    position_id         VARCHAR(64) UNIQUE,    -- Unique identifier for the position
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
    user_id             VARCHAR(64),           -- User ID
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- When the position was last updated
    paper_trading       BOOLEAN DEFAULT FALSE,  -- Whether this is a paper trade
    kite_response       JSONB                  -- Raw Zerodha response for audit/debug
);
