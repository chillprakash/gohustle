-- Update positions table to use auto-generated primary keys
-- First, drop the unique constraint on position_id
ALTER TABLE positions DROP CONSTRAINT IF EXISTS positions_position_id_key;

-- Make position_id nullable (since we'll be phasing it out)
ALTER TABLE positions ALTER COLUMN position_id DROP NOT NULL;

-- Add index on trading_symbol, exchange, product, paper_trading, user_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_positions_lookup ON positions (trading_symbol, exchange, product, paper_trading, user_id);

-- Update orders table to ensure order_id is nullable (for paper trading orders)
ALTER TABLE orders ALTER COLUMN order_id DROP NOT NULL;
