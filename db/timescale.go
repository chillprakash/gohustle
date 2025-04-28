package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	timescaleInstance *TimescaleDB
	timescaleOnce     sync.Once
)

type TimescaleDB struct {
	pool   *pgxpool.Pool
	config *config.TimescaleConfig
	log    *logger.Logger
}

// GetTimescaleDB returns a singleton instance of TimescaleDB
func GetTimescaleDB() *TimescaleDB {
	timescaleOnce.Do(func() {
		var err error
		timescaleInstance, err = initDB()
		if err != nil {
			panic(fmt.Sprintf("Failed to initialize TimescaleDB: %v", err))
		}
	})
	return timescaleInstance
}

// initDB initializes the database connection
func initDB() (*TimescaleDB, error) {
	log := logger.L()
	cfg := config.GetConfig().Timescale
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
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	// Set pool configuration
	poolConfig.MaxConns = int32(cfg.MaxConnections)
	poolConfig.MinConns = int32(cfg.MinConnections)

	// Parse MaxConnLifetime
	maxLifetime, err := time.ParseDuration(cfg.MaxConnLifetime)
	if err != nil {
		log.Error("Invalid MaxConnLifetime", map[string]interface{}{
			"error": err.Error(),
			"value": cfg.MaxConnLifetime,
		})
		return nil, fmt.Errorf("invalid MaxConnLifetime: %w", err)
	}
	poolConfig.MaxConnLifetime = maxLifetime

	// Parse MaxConnIdleTime
	maxIdleTime, err := time.ParseDuration(cfg.MaxConnIdleTime)
	if err != nil {
		log.Error("Invalid MaxConnIdleTime", map[string]interface{}{
			"error": err.Error(),
			"value": cfg.MaxConnIdleTime,
		})
		return nil, fmt.Errorf("invalid MaxConnIdleTime: %w", err)
	}
	poolConfig.MaxConnIdleTime = maxIdleTime

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Error("Failed to create database pool", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to create database pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		log.Error("Failed to ping database", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to ping database: %w", err)
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
		config: &cfg,
		log:    log,
	}, nil
}

// GetPool returns the connection pool
func (t *TimescaleDB) GetPool() *pgxpool.Pool {
	return t.pool
}

// Close closes the database connection
func (t *TimescaleDB) Close() {
	if t.pool != nil {
		t.pool.Close()
	}
}

// QueryRow executes a query that returns a single row
func (t *TimescaleDB) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	return t.pool.QueryRow(ctx, query, args...)
}

// Exec executes a query that doesn't return rows
func (t *TimescaleDB) Exec(ctx context.Context, query string, args ...interface{}) (CommandTag, error) {
	return t.pool.Exec(ctx, query, args...)
}

// Row interface for query results
type Row interface {
	Scan(dest ...interface{}) error
}

// CommandTag interface for exec results
type CommandTag interface {
	RowsAffected() int64
}

// TickRecord struct aligned with proto.TickData message
type TickRecord struct {
	ID                    int64            `db:"id"`
	InstrumentToken       uint32           `db:"instrument_token"`
	ExchangeUnixTimestamp pgtype.Timestamp `db:"exchange_unix_timestamp"`
	LastPrice             float64          `db:"last_price"`
	OpenInterest          uint32           `db:"open_interest"`
	VolumeTraded          uint32           `db:"volume_traded"`
	AverageTradePrice     float64          `db:"average_trade_price"`

	// Metadata for database operations
	TickReceivedTime   pgtype.Timestamp `db:"tick_received_time"`
	TickStoredInDbTime pgtype.Timestamp `db:"tick_stored_in_db_time"`
}

// Convert proto.TickData to TickRecord
func protoToTickRecord(tick *proto.TickData) *TickRecord {
	record := &TickRecord{
		InstrumentToken:       tick.InstrumentToken,
		ExchangeUnixTimestamp: pgtype.Timestamp{Time: time.Unix(tick.ExchangeUnixTimestamp, 0), Valid: true},
		LastPrice:             tick.LastPrice,
		OpenInterest:          tick.OpenInterest,
		VolumeTraded:          tick.VolumeTraded,
		AverageTradePrice:     tick.AverageTradePrice,

		// Metadata
		TickReceivedTime:   pgtype.Timestamp{Time: time.Now(), Valid: true},
		TickStoredInDbTime: pgtype.Timestamp{Time: time.Now(), Valid: true},
	}

	return record
}

// No depth helper methods needed as we're using the simplified TickData model

// GetTableName returns the appropriate table name for the tick
// WriteTicks inserts a batch of ticks efficiently using pgx.CopyFrom
func (t *TimescaleDB) WriteTicks(ctx context.Context, tableName string, ticks []*proto.TickData) error {
	if len(ticks) == 0 {
		return nil
	}

	rows := make([][]interface{}, 0, len(ticks))
	for _, tick := range ticks {
		record := protoToTickRecord(tick)
		rows = append(rows, []interface{}{
			record.InstrumentToken,
			record.ExchangeUnixTimestamp,
			record.LastPrice,
			record.OpenInterest,
			record.VolumeTraded,
			record.AverageTradePrice,
			record.TickReceivedTime,
			record.TickStoredInDbTime,
		})
	}

	colNames := []string{
		"instrument_token",
		"exchange_unix_timestamp",
		"last_price",
		"open_interest",
		"volume_traded",
		"average_trade_price",
		"tick_received_time",
		"tick_stored_in_db_time",
	}

	copyCount, err := t.pool.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		colNames,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		t.log.Error("Failed to batch write ticks", map[string]interface{}{
			"error":     err.Error(),
			"table":     tableName,
			"batch_len": len(ticks),
		})
		return fmt.Errorf("failed to batch write ticks: %w", err)
	}

	if int(copyCount) != len(ticks) {
		t.log.Error("Batch write: unexpected number of rows written", map[string]interface{}{
			"expected": len(ticks),
			"actual":   copyCount,
			"table":    tableName,
		})
	}

	return nil
}

// Complete WriteTick method
func (t *TimescaleDB) WriteTick(ctx context.Context, tableName string, tick *proto.TickData) error {
	record := protoToTickRecord(tick)

	query := fmt.Sprintf(`
		INSERT INTO %s (
			instrument_token, exchange_unix_timestamp, last_price,
			open_interest, volume_traded, average_trade_price,
			tick_received_time, tick_stored_in_db_time
		) VALUES (
			@instrument_token, @exchange_unix_timestamp, @last_price,
			@open_interest, @volume_traded, @average_trade_price,
			@tick_received_time, @tick_stored_in_db_time
		)
	`, tableName)

	args := pgx.NamedArgs{
		"instrument_token":        record.InstrumentToken,
		"exchange_unix_timestamp": record.ExchangeUnixTimestamp,
		"last_price":              record.LastPrice,
		"open_interest":           record.OpenInterest,
		"volume_traded":           record.VolumeTraded,
		"average_trade_price":     record.AverageTradePrice,
		"tick_received_time":      record.TickReceivedTime,
		"tick_stored_in_db_time":  record.TickStoredInDbTime,
	}

	result, err := t.pool.Exec(ctx, query, args)
	if err != nil {
		t.log.Error("Failed to write tick", map[string]interface{}{
			"error":            err.Error(),
			"table":            tableName,
			"instrument_token": tick.InstrumentToken,
		})
		return fmt.Errorf("failed to write tick: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected != 1 {
		t.log.Error("Unexpected number of rows affected", map[string]interface{}{
			"rows_affected":    rowsAffected,
			"table":            tableName,
			"instrument_token": tick.InstrumentToken,
		})
	}

	return nil
}
