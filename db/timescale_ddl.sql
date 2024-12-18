-- Create base tables for spot indices
CREATE TABLE nifty_ticks (
    id BIGSERIAL,
    instrument_token BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    is_tradable BOOLEAN NOT NULL,
    is_index BOOLEAN NOT NULL,
    mode TEXT NOT NULL,
    
    -- Price information
    last_price DOUBLE PRECISION NOT NULL,
    last_traded_quantity INTEGER NOT NULL,
    average_trade_price DOUBLE PRECISION NOT NULL,
    volume_traded INTEGER NOT NULL,
    total_buy_quantity INTEGER NOT NULL,
    total_sell_quantity INTEGER NOT NULL,
    total_buy INTEGER NOT NULL,
    total_sell INTEGER NOT NULL,
    
    -- OHLC
    ohlc_open DOUBLE PRECISION NOT NULL,
    ohlc_high DOUBLE PRECISION NOT NULL,
    ohlc_low DOUBLE PRECISION NOT NULL,
    ohlc_close DOUBLE PRECISION NOT NULL,
    
    -- Market Depth - Buy
    depth_buy_price_1 DOUBLE PRECISION,
    depth_buy_quantity_1 INTEGER,
    depth_buy_orders_1 INTEGER,
    depth_buy_price_2 DOUBLE PRECISION,
    depth_buy_quantity_2 INTEGER,
    depth_buy_orders_2 INTEGER,
    depth_buy_price_3 DOUBLE PRECISION,
    depth_buy_quantity_3 INTEGER,
    depth_buy_orders_3 INTEGER,
    depth_buy_price_4 DOUBLE PRECISION,
    depth_buy_quantity_4 INTEGER,
    depth_buy_orders_4 INTEGER,
    depth_buy_price_5 DOUBLE PRECISION,
    depth_buy_quantity_5 INTEGER,
    depth_buy_orders_5 INTEGER,
    
    -- Market Depth - Sell
    depth_sell_price_1 DOUBLE PRECISION,
    depth_sell_quantity_1 INTEGER,
    depth_sell_orders_1 INTEGER,
    depth_sell_price_2 DOUBLE PRECISION,
    depth_sell_quantity_2 INTEGER,
    depth_sell_orders_2 INTEGER,
    depth_sell_price_3 DOUBLE PRECISION,
    depth_sell_quantity_3 INTEGER,
    depth_sell_orders_3 INTEGER,
    depth_sell_price_4 DOUBLE PRECISION,
    depth_sell_quantity_4 INTEGER,
    depth_sell_orders_4 INTEGER,
    depth_sell_price_5 DOUBLE PRECISION,
    depth_sell_quantity_5 INTEGER,
    depth_sell_orders_5 INTEGER,
    
    -- Additional fields
    last_trade_time TIMESTAMPTZ NOT NULL,
    net_change DOUBLE PRECISION NOT NULL,
    
    -- Metadata
    tick_received_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tick_stored_in_db_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Primary key must include the partitioning column
    PRIMARY KEY (id, timestamp)
);

-- Create identical tables for other instruments
CREATE TABLE sensex_ticks (LIKE nifty_ticks INCLUDING ALL);
CREATE TABLE nifty_upcoming_expiry_ticks (LIKE nifty_ticks INCLUDING ALL);
CREATE TABLE sensex_upcoming_expiry_ticks (LIKE nifty_ticks INCLUDING ALL);

-- Convert tables to hypertables with 1-day chunks
SELECT create_hypertable('nifty_ticks', 'timestamp', chunk_time_interval => INTERVAL '1 day');
SELECT create_hypertable('sensex_ticks', 'timestamp', chunk_time_interval => INTERVAL '1 day');
SELECT create_hypertable('nifty_upcoming_expiry_ticks', 'timestamp', chunk_time_interval => INTERVAL '1 day');
SELECT create_hypertable('sensex_upcoming_expiry_ticks', 'timestamp', chunk_time_interval => INTERVAL '1 day');

-- Create indexes for each table
DO $$
DECLARE
    table_names text[] := ARRAY['nifty_ticks', 'sensex_ticks', 'nifty_upcoming_expiry_ticks', 'sensex_upcoming_expiry_ticks'];
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        -- Create index on instrument_token and timestamp
        EXECUTE format('
            CREATE INDEX %I_instrument_time_idx ON %I (instrument_token, timestamp DESC);
            CREATE INDEX %I_time_idx ON %I (timestamp DESC);
        ', 
        table_name, table_name,
        table_name, table_name);
    END LOOP;
END $$;

-- Enable compression on all tables with correct settings
ALTER TABLE nifty_ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'id,instrument_token',
    timescaledb.compress_orderby = 'timestamp DESC'
);

ALTER TABLE sensex_ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'id,instrument_token',
    timescaledb.compress_orderby = 'timestamp DESC'
);

ALTER TABLE nifty_upcoming_expiry_ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'id,instrument_token',
    timescaledb.compress_orderby = 'timestamp DESC'
);

ALTER TABLE sensex_upcoming_expiry_ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'id,instrument_token',
    timescaledb.compress_orderby = 'timestamp DESC'
);

-- Add compression policies (compress chunks older than 7 days)
SELECT add_compression_policy('nifty_ticks', INTERVAL '7 days');
SELECT add_compression_policy('sensex_ticks', INTERVAL '7 days');
SELECT add_compression_policy('nifty_upcoming_expiry_ticks', INTERVAL '7 days');
SELECT add_compression_policy('sensex_upcoming_expiry_ticks', INTERVAL '7 days');

-- Set retention policy (keep 1 year of data)
SELECT add_retention_policy('nifty_ticks', INTERVAL '1 year');
SELECT add_retention_policy('sensex_ticks', INTERVAL '1 year');
SELECT add_retention_policy('nifty_upcoming_expiry_ticks', INTERVAL '1 year');
SELECT add_retention_policy('sensex_upcoming_expiry_ticks', INTERVAL '1 year');

-- Create views for latest prices
CREATE OR REPLACE VIEW latest_nifty_ticks AS
    SELECT DISTINCT ON (instrument_token)
        instrument_token,
        timestamp,
        last_price,
        net_change,
        volume_traded,
        last_trade_time
    FROM nifty_ticks
    ORDER BY instrument_token, timestamp DESC;

CREATE OR REPLACE VIEW latest_sensex_ticks AS
    SELECT DISTINCT ON (instrument_token)
        instrument_token,
        timestamp,
        last_price,
        net_change,
        volume_traded,
        last_trade_time
    FROM sensex_ticks
    ORDER BY instrument_token, timestamp DESC;

-- Create derived data summary table for Nifty
CREATE TABLE derived_data_summary_nifty (
    id BIGSERIAL,
    added_time TIMESTAMPTZ NOT NULL,
    spot DOUBLE PRECISION NOT NULL,
    synthetic_future DOUBLE PRECISION NOT NULL,
    atm_strike DOUBLE PRECISION NOT NULL,
    
    -- Primary key must include the partitioning column
    PRIMARY KEY (id, added_time)
);

-- Create derived data summary table for Sensex
CREATE TABLE derived_data_summary_sensex (
    id BIGSERIAL,
    added_time TIMESTAMPTZ NOT NULL,
    spot DOUBLE PRECISION NOT NULL,
    synthetic_future DOUBLE PRECISION NOT NULL,
    atm_strike DOUBLE PRECISION NOT NULL,
    
    -- Primary key must include the partitioning column
    PRIMARY KEY (id, added_time)
);

-- Convert tables to hypertables
SELECT create_hypertable('derived_data_summary_nifty', 'added_time', chunk_time_interval => INTERVAL '1 day');
SELECT create_hypertable('derived_data_summary_sensex', 'added_time', chunk_time_interval => INTERVAL '1 day');

-- Create indexes for better query performance
CREATE INDEX ON derived_data_summary_nifty (added_time DESC);
CREATE INDEX ON derived_data_summary_sensex (added_time DESC);

-- Enable compression with TimescaleDB
ALTER TABLE derived_data_summary_nifty SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'id',
    timescaledb.compress_orderby = 'added_time DESC'
);

ALTER TABLE derived_data_summary_sensex SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'id',
    timescaledb.compress_orderby = 'added_time DESC'
);

-- Add compression policies (compress chunks older than 7 days)
SELECT add_compression_policy('derived_data_summary_nifty', INTERVAL '7 days');
SELECT add_compression_policy('derived_data_summary_sensex', INTERVAL '7 days');

-- Add retention policy (keep 1 year of data)
SELECT add_retention_policy('derived_data_summary_nifty', INTERVAL '1 year');
SELECT add_retention_policy('derived_data_summary_sensex', INTERVAL '1 year');