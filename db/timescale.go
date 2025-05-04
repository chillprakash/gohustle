package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"
	"gohustle/utils"

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

// OrderRecord struct for persisting order data
// Matches the 'orders' table in schema.sql
type OrderRecord struct {
	ID            int64       `db:"id"`
	OrderID       string      `db:"order_id"`
	OrderType     string      `db:"order_type"`
	GTTType       string      `db:"gtt_type"`
	Status        string      `db:"status"`
	Message       string      `db:"message"`
	TradingSymbol string      `db:"trading_symbol"`
	Exchange      string      `db:"exchange"`
	Side          string      `db:"side"`
	Quantity      int         `db:"quantity"`
	Price         float64     `db:"price"`
	TriggerPrice  float64     `db:"trigger_price"`
	Product       string      `db:"product"`
	Validity      string      `db:"validity"`
	DisclosedQty  int         `db:"disclosed_qty"`
	Tag           string      `db:"tag"`
	UserID        string      `db:"user_id"`
	PlacedAt      time.Time   `db:"placed_at"`
	KiteResponse  interface{} `db:"kite_response"`
	PaperTrading  bool        `db:"paper_trading"`
}

// PositionRecord represents a record in the positions table
type PositionRecord struct {
	ID            int64       `db:"id"`
	PositionID    string      `db:"position_id"`
	TradingSymbol string      `db:"trading_symbol"`
	Exchange      string      `db:"exchange"`
	Product       string      `db:"product"`
	Quantity      int         `db:"quantity"`
	AveragePrice  float64     `db:"average_price"`
	LastPrice     float64     `db:"last_price"`
	PnL           float64     `db:"pnl"`
	RealizedPnL   float64     `db:"realized_pnl"`
	UnrealizedPnL float64     `db:"unrealized_pnl"`
	Multiplier    float64     `db:"multiplier"`
	BuyQuantity   int         `db:"buy_quantity"`
	SellQuantity  int         `db:"sell_quantity"`
	BuyPrice      float64     `db:"buy_price"`
	SellPrice     float64     `db:"sell_price"`
	BuyValue      float64     `db:"buy_value"`
	SellValue     float64     `db:"sell_value"`
	PositionType  string      `db:"position_type"`
	StrategyID    *int        `db:"strategy_id"`
	UserID        string      `db:"user_id"`
	UpdatedAt     time.Time   `db:"updated_at"`
	PaperTrading  bool        `db:"paper_trading"`
	KiteResponse  interface{} `db:"kite_response"`
}

// ListOrders fetches all order records from the orders table
func (t *TimescaleDB) ListOrders(ctx context.Context) ([]*OrderRecord, error) {
	query := `SELECT id, order_id, order_type, gtt_type, status, message, trading_symbol, exchange, side, quantity, price, trigger_price, product, validity, disclosed_qty, tag, user_id, placed_at, kite_response, paper_trading FROM orders ORDER BY placed_at DESC`
	rows, err := t.pool.Query(ctx, query)
	if err != nil {
		t.log.Error("Failed to list orders", map[string]interface{}{"error": err.Error()})
		return nil, err
	}
	defer rows.Close()

	var orders []*OrderRecord
	for rows.Next() {
		order := &OrderRecord{}
		var kiteRespBytes []byte
		err := rows.Scan(
			&order.ID,
			&order.OrderID,
			&order.OrderType,
			&order.GTTType,
			&order.Status,
			&order.Message,
			&order.TradingSymbol,
			&order.Exchange,
			&order.Side,
			&order.Quantity,
			&order.Price,
			&order.TriggerPrice,
			&order.Product,
			&order.Validity,
			&order.DisclosedQty,
			&order.Tag,
			&order.UserID,
			&order.PlacedAt,
			&kiteRespBytes,
			&order.PaperTrading,
		)
		if err != nil {
			t.log.Error("Failed to scan order row", map[string]interface{}{"error": err.Error()})
			continue
		}
		// Unmarshal kite_response JSON
		if len(kiteRespBytes) > 0 {
			_ = json.Unmarshal(kiteRespBytes, &order.KiteResponse)
		}
		orders = append(orders, order)
	}
	if err := rows.Err(); err != nil {
		t.log.Error("Error iterating order rows", map[string]interface{}{"error": err.Error()})
		return nil, err
	}
	return orders, nil
}

// InsertOrder inserts an order record into the orders table
func (t *TimescaleDB) InsertOrder(ctx context.Context, order *OrderRecord) error {
	query := `
		INSERT INTO orders (
			order_id, order_type, gtt_type, status, message, trading_symbol, exchange, side,
			quantity, price, trigger_price, product, validity, disclosed_qty, tag, user_id, placed_at, kite_response
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13, $14, $15, $16, $17, $18
		)
	`
	_, err := t.pool.Exec(ctx, query,
		order.OrderID, order.OrderType, order.GTTType, order.Status, order.Message, order.TradingSymbol, order.Exchange, order.Side,
		order.Quantity, order.Price, order.TriggerPrice, order.Product, order.Validity, order.DisclosedQty, order.Tag, order.UserID, order.PlacedAt, order.KiteResponse,
	)
	if err != nil {
		t.log.Error("Failed to insert order", map[string]interface{}{"error": err.Error(), "order_id": order.OrderID})
		return err
	}
	return nil
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
		TickReceivedTime:   pgtype.Timestamp{Time: utils.NowIST(), Valid: true},
		TickStoredInDbTime: pgtype.Timestamp{Time: utils.NowIST(), Valid: true},
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
