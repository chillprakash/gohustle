package db

import (
	"context"
	"fmt"
	"os"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TimescaleDB struct {
	pool   *pgxpool.Pool
	config *config.TimescaleConfig
}

// InitDB initializes the database connection
func InitDB(cfg *config.TimescaleConfig) *TimescaleDB {
	log := logger.GetLogger()
	ctx := context.Background()

	// Create connection string
	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName,
	)

	// Configure connection pool
	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Error("Failed to parse database config", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Set pool configuration from TimescaleConfig
	poolConfig.MaxConns = int32(cfg.MaxConnections)
	poolConfig.MinConns = int32(cfg.MinConnections)

	// Parse MaxConnLifetime
	maxLifetime, err := time.ParseDuration(cfg.MaxConnLifetime)
	if err != nil {
		log.Error("Invalid MaxConnLifetime", map[string]interface{}{
			"error": err.Error(),
			"value": cfg.MaxConnLifetime,
		})
		os.Exit(1)
	}
	poolConfig.MaxConnLifetime = maxLifetime

	// Parse MaxConnIdleTime
	maxIdleTime, err := time.ParseDuration(cfg.MaxConnIdleTime)
	if err != nil {
		log.Error("Invalid MaxConnIdleTime", map[string]interface{}{
			"error": err.Error(),
			"value": cfg.MaxConnIdleTime,
		})
		os.Exit(1)
	}
	poolConfig.MaxConnIdleTime = maxIdleTime

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Error("Failed to create database pool", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		log.Error("Failed to ping database", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	log.Info("Successfully connected to database", map[string]interface{}{
		"host":         cfg.Host,
		"port":         cfg.Port,
		"db":           cfg.DBName,
		"max_conns":    cfg.MaxConnections,
		"min_conns":    cfg.MinConnections,
		"max_lifetime": cfg.MaxConnLifetime,
		"max_idletime": cfg.MaxConnIdleTime,
	})

	return &TimescaleDB{
		pool:   pool,
		config: cfg,
	}
}

// GetConfig returns the database configuration
func (db *TimescaleDB) GetConfig() *config.TimescaleConfig {
	return db.config
}

func (t *TimescaleDB) GetPool() *pgxpool.Pool {
	return t.pool
}

// Close closes the database connection
func (db *TimescaleDB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

// QueryRow executes a query that returns a single row
func (db *TimescaleDB) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	return db.pool.QueryRow(ctx, query, args...)
}

// Exec executes a query that doesn't return rows
func (db *TimescaleDB) Exec(ctx context.Context, query string, args ...interface{}) (CommandTag, error) {
	return db.pool.Exec(ctx, query, args...)
}

// Row interface for query results
type Row interface {
	Scan(dest ...interface{}) error
}

// CommandTag interface for exec results
type CommandTag interface {
	RowsAffected() int64
}

// CreateTickTables creates tables for a specific index and expiry
func (db *TimescaleDB) CreateTickTables(indexName string, expiryDate time.Time) error {
	log := logger.GetLogger()
	log.Info("Creating tick tables", map[string]interface{}{
		"index":  indexName,
		"expiry": expiryDate.Format("20060102"),
	})

	var tableName, depthTableName, minuteViewName string
	err := db.pool.QueryRow(context.Background(),
		`SELECT * FROM create_tick_tables($1, $2)`,
		indexName, expiryDate).Scan(&tableName, &depthTableName, &minuteViewName)
	if err != nil {
		return fmt.Errorf("failed to create tick tables: %w", err)
	}

	// Create continuous aggregate view
	createViewSQL := fmt.Sprintf(`
        CREATE MATERIALIZED VIEW IF NOT EXISTS %s
        WITH (timescaledb.continuous) AS
        SELECT 
            time_bucket('1 minute', timestamp) AS bucket,
            instrument_token,
            first(open_price, timestamp) as open,
            max(high_price) as high,
            min(low_price) as low,
            last(close_price, timestamp) as close,
            sum(volume_traded) as volume,
            last(oi, timestamp) as oi
        FROM %s
        GROUP BY bucket, instrument_token;`,
		minuteViewName, tableName)

	_, err = db.pool.Exec(context.Background(), createViewSQL)
	if err != nil {
		log.Error("Failed to create view", map[string]interface{}{
			"error": err.Error(),
			"view":  minuteViewName,
		})
		return fmt.Errorf("failed to create continuous aggregate view: %w", err)
	}

	// Add continuous aggregate policy
	addPolicySQL := fmt.Sprintf(`
        SELECT add_continuous_aggregate_policy('%s',
            start_offset => INTERVAL '3 hours',
            end_offset => INTERVAL '1 minute',
            schedule_interval => INTERVAL '1 minute',
            if_not_exists => true);`,
		minuteViewName)

	_, err = db.pool.Exec(context.Background(), addPolicySQL)
	if err != nil {
		log.Error("Failed to add policy", map[string]interface{}{
			"error": err.Error(),
			"view":  minuteViewName,
		})
		return fmt.Errorf("failed to add continuous aggregate policy: %w", err)
	}

	log.Info("Successfully created tables and views", map[string]interface{}{
		"index":  indexName,
		"expiry": expiryDate.Format("20060102"),
		"table":  tableName,
		"depth":  depthTableName,
		"view":   minuteViewName,
	})

	return nil
}

// StoreTick stores tick data in the specified table
func (db *TimescaleDB) StoreTick(tableName string, tick *proto.TickData) error {
	log := logger.GetLogger()
	tx, err := db.pool.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	// Insert main tick data
	_, err = tx.Exec(context.Background(), fmt.Sprintf(`
        INSERT INTO %s (
            instrument_token, timestamp, is_tradable, is_index, mode,
            last_price, last_trade_time, last_traded_quantity,
            total_buy_quantity, total_sell_quantity, volume_traded,
            total_buy, total_sell, average_trade_price,
            oi, oi_day_high, oi_day_low, net_change,
            open_price, high_price, low_price, close_price
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        ON CONFLICT (instrument_token, timestamp) 
        DO UPDATE SET
            last_price = EXCLUDED.last_price,
            oi = EXCLUDED.oi`, tableName),
		tick.InstrumentToken, time.Unix(tick.Timestamp, 0), tick.IsTradable, tick.IsIndex, tick.Mode,
		tick.LastPrice, time.Unix(tick.LastTradeTime, 0), tick.LastTradedQuantity,
		tick.TotalBuyQuantity, tick.TotalSellQuantity, tick.VolumeTraded,
		tick.TotalBuy, tick.TotalSell, tick.AverageTradePrice,
		tick.Oi, tick.OiDayHigh, tick.OiDayLow, tick.NetChange,
		tick.Ohlc.Open, tick.Ohlc.High, tick.Ohlc.Low, tick.Ohlc.Close)

	if err != nil {
		return fmt.Errorf("failed to insert tick data: %w", err)
	}

	// Insert depth data if available
	if tick.Depth != nil {
		depthTableName := fmt.Sprintf("%s_depth", tableName)

		// First delete existing depth data for this tick
		_, err = tx.Exec(context.Background(), fmt.Sprintf(`
            DELETE FROM %s 
            WHERE instrument_token = $1 AND timestamp = $2`,
			depthTableName),
			tick.InstrumentToken, time.Unix(tick.Timestamp, 0))
		if err != nil {
			return fmt.Errorf("failed to delete old depth data: %w", err)
		}

		// Insert buy depth
		for i, buy := range tick.Depth.Buy {
			_, err = tx.Exec(context.Background(), fmt.Sprintf(`
                INSERT INTO %s (
                    instrument_token, timestamp, side, level,
                    price, quantity, orders
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				depthTableName),
				tick.InstrumentToken, time.Unix(tick.Timestamp, 0),
				"buy", i+1, buy.Price, buy.Quantity, buy.Orders)
			if err != nil {
				return fmt.Errorf("failed to insert buy depth data: %w", err)
			}
		}

		// Insert sell depth
		for i, sell := range tick.Depth.Sell {
			_, err = tx.Exec(context.Background(), fmt.Sprintf(`
                INSERT INTO %s (
                    instrument_token, timestamp, side, level,
                    price, quantity, orders
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				depthTableName),
				tick.InstrumentToken, time.Unix(tick.Timestamp, 0),
				"sell", i+1, sell.Price, sell.Quantity, sell.Orders)
			if err != nil {
				return fmt.Errorf("failed to insert sell depth data: %w", err)
			}
		}
	}

	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Debug("Successfully stored tick data", map[string]interface{}{
		"table":      tableName,
		"instrument": tick.InstrumentToken,
		"timestamp":  time.Unix(tick.Timestamp, 0),
	})

	return nil
}
