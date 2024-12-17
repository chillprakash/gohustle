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
	instance *TimescaleDB
	once     sync.Once
)

type TimescaleDB struct {
	pool   *pgxpool.Pool
	config *config.TimescaleConfig
	log    *logger.Logger
}

// GetTimescaleDB returns a singleton instance of TimescaleDB
func GetTimescaleDB() *TimescaleDB {
	once.Do(func() {
		var err error
		instance, err = initDB()
		if err != nil {
			panic(fmt.Sprintf("Failed to initialize TimescaleDB: %v", err))
		}
	})
	return instance
}

// initDB initializes the database connection
func initDB() (*TimescaleDB, error) {
	log := logger.GetLogger()
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

// GetTableName returns the appropriate table name for the tick
// You can implement your logic here
func (t *TimescaleDB) GetTableName(tick *proto.TickData) string {
	// TODO: Implement your table name resolution logic
	if tick.InstrumentToken == 256265 {
		return "nifty_ticks"
	} else if tick.InstrumentToken == 265 {
		return "sensex_ticks"
	}

	if tick.IndexName == "NIFTY" {
		return "nifty_upcoming_expiry_ticks"
	} else if tick.IndexName == "SENSEX" {
		return "sensex_upcoming_expiry_ticks"
	}

	return "nifty_ticks" // placeholder
}

// Add this struct for ORM-style mapping
type TickRecord struct {
	ID              int64            `db:"id"`
	InstrumentToken int64            `db:"instrument_token"`
	Timestamp       pgtype.Timestamp `db:"timestamp"`
	IsTradable      bool             `db:"is_tradable"`
	IsIndex         bool             `db:"is_index"`
	Mode            string           `db:"mode"`

	// Price information
	LastPrice          float64 `db:"last_price"`
	LastTradedQuantity int32   `db:"last_traded_quantity"`
	AverageTradePrice  float64 `db:"average_trade_price"`
	VolumeTraded       int32   `db:"volume_traded"`
	TotalBuyQuantity   int32   `db:"total_buy_quantity"`
	TotalSellQuantity  int32   `db:"total_sell_quantity"`
	TotalBuy           int32   `db:"total_buy"`
	TotalSell          int32   `db:"total_sell"`

	// OHLC
	OhlcOpen  float64 `db:"ohlc_open"`
	OhlcHigh  float64 `db:"ohlc_high"`
	OhlcLow   float64 `db:"ohlc_low"`
	OhlcClose float64 `db:"ohlc_close"`

	// Market Depth - Buy
	DepthBuyPrice1    pgtype.Float8 `db:"depth_buy_price_1"`
	DepthBuyQuantity1 pgtype.Int4   `db:"depth_buy_quantity_1"`
	DepthBuyOrders1   pgtype.Int4   `db:"depth_buy_orders_1"`
	DepthBuyPrice2    pgtype.Float8 `db:"depth_buy_price_2"`
	DepthBuyQuantity2 pgtype.Int4   `db:"depth_buy_quantity_2"`
	DepthBuyOrders2   pgtype.Int4   `db:"depth_buy_orders_2"`
	DepthBuyPrice3    pgtype.Float8 `db:"depth_buy_price_3"`
	DepthBuyQuantity3 pgtype.Int4   `db:"depth_buy_quantity_3"`
	DepthBuyOrders3   pgtype.Int4   `db:"depth_buy_orders_3"`
	DepthBuyPrice4    pgtype.Float8 `db:"depth_buy_price_4"`
	DepthBuyQuantity4 pgtype.Int4   `db:"depth_buy_quantity_4"`
	DepthBuyOrders4   pgtype.Int4   `db:"depth_buy_orders_4"`
	DepthBuyPrice5    pgtype.Float8 `db:"depth_buy_price_5"`
	DepthBuyQuantity5 pgtype.Int4   `db:"depth_buy_quantity_5"`
	DepthBuyOrders5   pgtype.Int4   `db:"depth_buy_orders_5"`

	// Market Depth - Sell
	DepthSellPrice1    pgtype.Float8 `db:"depth_sell_price_1"`
	DepthSellQuantity1 pgtype.Int4   `db:"depth_sell_quantity_1"`
	DepthSellOrders1   pgtype.Int4   `db:"depth_sell_orders_1"`
	DepthSellPrice2    pgtype.Float8 `db:"depth_sell_price_2"`
	DepthSellQuantity2 pgtype.Int4   `db:"depth_sell_quantity_2"`
	DepthSellOrders2   pgtype.Int4   `db:"depth_sell_orders_2"`
	DepthSellPrice3    pgtype.Float8 `db:"depth_sell_price_3"`
	DepthSellQuantity3 pgtype.Int4   `db:"depth_sell_quantity_3"`
	DepthSellOrders3   pgtype.Int4   `db:"depth_sell_orders_3"`
	DepthSellPrice4    pgtype.Float8 `db:"depth_sell_price_4"`
	DepthSellQuantity4 pgtype.Int4   `db:"depth_sell_quantity_4"`
	DepthSellOrders4   pgtype.Int4   `db:"depth_sell_orders_4"`
	DepthSellPrice5    pgtype.Float8 `db:"depth_sell_price_5"`
	DepthSellQuantity5 pgtype.Int4   `db:"depth_sell_quantity_5"`
	DepthSellOrders5   pgtype.Int4   `db:"depth_sell_orders_5"`

	// Additional fields
	LastTradeTime pgtype.Timestamp `db:"last_trade_time"`
	NetChange     float64          `db:"net_change"`

	// Metadata
	TickReceivedTime   pgtype.Timestamp `db:"tick_received_time"`
	TickStoredInDbTime pgtype.Timestamp `db:"tick_stored_in_db_time"`
}

// Convert proto.TickData to TickRecord
func protoToTickRecord(tick *proto.TickData) *TickRecord {
	record := &TickRecord{
		InstrumentToken: int64(tick.InstrumentToken),
		Timestamp:       pgtype.Timestamp{Time: time.Unix(tick.Timestamp, 0), Valid: true},
		IsTradable:      tick.IsTradable,
		IsIndex:         tick.IsIndex,
		Mode:            tick.Mode,

		// Price information
		LastPrice:          tick.LastPrice,
		LastTradedQuantity: int32(tick.LastTradedQuantity),
		AverageTradePrice:  tick.AverageTradePrice,
		VolumeTraded:       int32(tick.VolumeTraded),
		TotalBuyQuantity:   int32(tick.TotalBuyQuantity),
		TotalSellQuantity:  int32(tick.TotalSellQuantity),
		TotalBuy:           int32(tick.TotalBuy),
		TotalSell:          int32(tick.TotalSell),

		// OHLC
		OhlcOpen:  tick.Ohlc.Open,
		OhlcHigh:  tick.Ohlc.High,
		OhlcLow:   tick.Ohlc.Low,
		OhlcClose: tick.Ohlc.Close,

		// Additional fields
		LastTradeTime: pgtype.Timestamp{Time: time.Unix(tick.LastTradeTime, 0), Valid: true},
		NetChange:     tick.NetChange,

		// Metadata
		TickReceivedTime:   pgtype.Timestamp{Time: time.Now(), Valid: true},
		TickStoredInDbTime: pgtype.Timestamp{Time: time.Now(), Valid: true},
	}

	// Set Market Depth - Buy
	if len(tick.Depth.Buy) > 0 {
		record.setDepthBuy(tick.Depth)
	}

	// Set Market Depth - Sell
	if len(tick.Depth.Sell) > 0 {
		record.setDepthSell(tick.Depth.Sell)
	}

	return record
}

// Helper method to set buy depth
func (r *TickRecord) setDepthBuy(depth *proto.TickData_MarketDepth) {
	if depth != nil && len(depth.Buy) > 0 {
		// Level 1
		if len(depth.Buy) > 0 {
			r.DepthBuyPrice1 = pgtype.Float8{Float64: depth.Buy[0].Price, Valid: true}
			r.DepthBuyQuantity1 = pgtype.Int4{Int32: int32(depth.Buy[0].Quantity), Valid: true}
			r.DepthBuyOrders1 = pgtype.Int4{Int32: int32(depth.Buy[0].Orders), Valid: true}
		}
		// Level 2
		if len(depth.Buy) > 1 {
			r.DepthBuyPrice2 = pgtype.Float8{Float64: depth.Buy[1].Price, Valid: true}
			r.DepthBuyQuantity2 = pgtype.Int4{Int32: int32(depth.Buy[1].Quantity), Valid: true}
			r.DepthBuyOrders2 = pgtype.Int4{Int32: int32(depth.Buy[1].Orders), Valid: true}
		}
		// Level 3
		if len(depth.Buy) > 2 {
			r.DepthBuyPrice3 = pgtype.Float8{Float64: depth.Buy[2].Price, Valid: true}
			r.DepthBuyQuantity3 = pgtype.Int4{Int32: int32(depth.Buy[2].Quantity), Valid: true}
			r.DepthBuyOrders3 = pgtype.Int4{Int32: int32(depth.Buy[2].Orders), Valid: true}
		}
		// Level 4
		if len(depth.Buy) > 3 {
			r.DepthBuyPrice4 = pgtype.Float8{Float64: depth.Buy[3].Price, Valid: true}
			r.DepthBuyQuantity4 = pgtype.Int4{Int32: int32(depth.Buy[3].Quantity), Valid: true}
			r.DepthBuyOrders4 = pgtype.Int4{Int32: int32(depth.Buy[3].Orders), Valid: true}
		}
		// Level 5
		if len(depth.Buy) > 4 {
			r.DepthBuyPrice5 = pgtype.Float8{Float64: depth.Buy[4].Price, Valid: true}
			r.DepthBuyQuantity5 = pgtype.Int4{Int32: int32(depth.Buy[4].Quantity), Valid: true}
			r.DepthBuyOrders5 = pgtype.Int4{Int32: int32(depth.Buy[4].Orders), Valid: true}
		}
	}
}

// Helper method to set sell depth
func (r *TickRecord) setDepthSell(depth []*proto.TickData_DepthItem) {
	// Level 1
	if len(depth) > 0 {
		r.DepthSellPrice1 = pgtype.Float8{Float64: depth[0].Price, Valid: true}
		r.DepthSellQuantity1 = pgtype.Int4{Int32: int32(depth[0].Quantity), Valid: true}
		r.DepthSellOrders1 = pgtype.Int4{Int32: int32(depth[0].Orders), Valid: true}
	}
	// Level 2
	if len(depth) > 1 {
		r.DepthSellPrice2 = pgtype.Float8{Float64: depth[1].Price, Valid: true}
		r.DepthSellQuantity2 = pgtype.Int4{Int32: int32(depth[1].Quantity), Valid: true}
		r.DepthSellOrders2 = pgtype.Int4{Int32: int32(depth[1].Orders), Valid: true}
	}
	// Level 3
	if len(depth) > 2 {
		r.DepthSellPrice3 = pgtype.Float8{Float64: depth[2].Price, Valid: true}
		r.DepthSellQuantity3 = pgtype.Int4{Int32: int32(depth[2].Quantity), Valid: true}
		r.DepthSellOrders3 = pgtype.Int4{Int32: int32(depth[2].Orders), Valid: true}
	}
	// Level 4
	if len(depth) > 3 {
		r.DepthSellPrice4 = pgtype.Float8{Float64: depth[3].Price, Valid: true}
		r.DepthSellQuantity4 = pgtype.Int4{Int32: int32(depth[3].Quantity), Valid: true}
		r.DepthSellOrders4 = pgtype.Int4{Int32: int32(depth[3].Orders), Valid: true}
	}
	// Level 5
	if len(depth) > 4 {
		r.DepthSellPrice5 = pgtype.Float8{Float64: depth[4].Price, Valid: true}
		r.DepthSellQuantity5 = pgtype.Int4{Int32: int32(depth[4].Quantity), Valid: true}
		r.DepthSellOrders5 = pgtype.Int4{Int32: int32(depth[4].Orders), Valid: true}
	}
}

// Complete WriteTick method
func (t *TimescaleDB) WriteTick(tick *proto.TickData) error {
	tableName := t.GetTableName(tick)
	record := protoToTickRecord(tick)
	ctx := context.Background()

	query := fmt.Sprintf(`
		INSERT INTO %s (
			instrument_token, timestamp, is_tradable, is_index, mode,
			last_price, last_traded_quantity, average_trade_price,
			volume_traded, total_buy_quantity, total_sell_quantity,
			total_buy, total_sell,
			ohlc_open, ohlc_high, ohlc_low, ohlc_close,
			depth_buy_price_1, depth_buy_quantity_1, depth_buy_orders_1,
			depth_buy_price_2, depth_buy_quantity_2, depth_buy_orders_2,
			depth_buy_price_3, depth_buy_quantity_3, depth_buy_orders_3,
			depth_buy_price_4, depth_buy_quantity_4, depth_buy_orders_4,
			depth_buy_price_5, depth_buy_quantity_5, depth_buy_orders_5,
			depth_sell_price_1, depth_sell_quantity_1, depth_sell_orders_1,
			depth_sell_price_2, depth_sell_quantity_2, depth_sell_orders_2,
			depth_sell_price_3, depth_sell_quantity_3, depth_sell_orders_3,
			depth_sell_price_4, depth_sell_quantity_4, depth_sell_orders_4,
			depth_sell_price_5, depth_sell_quantity_5, depth_sell_orders_5,
			last_trade_time, net_change,
			tick_received_time, tick_stored_in_db_time
		) VALUES (
			@instrument_token, @timestamp, @is_tradable, @is_index, @mode,
			@last_price, @last_traded_quantity, @average_trade_price,
			@volume_traded, @total_buy_quantity, @total_sell_quantity,
			@total_buy, @total_sell,
			@ohlc_open, @ohlc_high, @ohlc_low, @ohlc_close,
			@depth_buy_price_1, @depth_buy_quantity_1, @depth_buy_orders_1,
			@depth_buy_price_2, @depth_buy_quantity_2, @depth_buy_orders_2,
			@depth_buy_price_3, @depth_buy_quantity_3, @depth_buy_orders_3,
			@depth_buy_price_4, @depth_buy_quantity_4, @depth_buy_orders_4,
			@depth_buy_price_5, @depth_buy_quantity_5, @depth_buy_orders_5,
			@depth_sell_price_1, @depth_sell_quantity_1, @depth_sell_orders_1,
			@depth_sell_price_2, @depth_sell_quantity_2, @depth_sell_orders_2,
			@depth_sell_price_3, @depth_sell_quantity_3, @depth_sell_orders_3,
			@depth_sell_price_4, @depth_sell_quantity_4, @depth_sell_orders_4,
			@depth_sell_price_5, @depth_sell_quantity_5, @depth_sell_orders_5,
			@last_trade_time, @net_change,
			@tick_received_time, @tick_stored_in_db_time
		)
	`, tableName)

	args := pgx.NamedArgs{
		"instrument_token":     record.InstrumentToken,
		"timestamp":            record.Timestamp,
		"is_tradable":          record.IsTradable,
		"is_index":             record.IsIndex,
		"mode":                 record.Mode,
		"last_price":           record.LastPrice,
		"last_traded_quantity": record.LastTradedQuantity,
		"average_trade_price":  record.AverageTradePrice,
		"volume_traded":        record.VolumeTraded,
		"total_buy_quantity":   record.TotalBuyQuantity,
		"total_sell_quantity":  record.TotalSellQuantity,
		"total_buy":            record.TotalBuy,
		"total_sell":           record.TotalSell,
		"ohlc_open":            record.OhlcOpen,
		"ohlc_high":            record.OhlcHigh,
		"ohlc_low":             record.OhlcLow,
		"ohlc_close":           record.OhlcClose,
		// Buy depth
		"depth_buy_price_1":    record.DepthBuyPrice1,
		"depth_buy_quantity_1": record.DepthBuyQuantity1,
		"depth_buy_orders_1":   record.DepthBuyOrders1,
		"depth_buy_price_2":    record.DepthBuyPrice2,
		"depth_buy_quantity_2": record.DepthBuyQuantity2,
		"depth_buy_orders_2":   record.DepthBuyOrders2,
		"depth_buy_price_3":    record.DepthBuyPrice3,
		"depth_buy_quantity_3": record.DepthBuyQuantity3,
		"depth_buy_orders_3":   record.DepthBuyOrders3,
		"depth_buy_price_4":    record.DepthBuyPrice4,
		"depth_buy_quantity_4": record.DepthBuyQuantity4,
		"depth_buy_orders_4":   record.DepthBuyOrders4,
		"depth_buy_price_5":    record.DepthBuyPrice5,
		"depth_buy_quantity_5": record.DepthBuyQuantity5,
		"depth_buy_orders_5":   record.DepthBuyOrders5,
		// Sell depth
		"depth_sell_price_1":    record.DepthSellPrice1,
		"depth_sell_quantity_1": record.DepthSellQuantity1,
		"depth_sell_orders_1":   record.DepthSellOrders1,
		"depth_sell_price_2":    record.DepthSellPrice2,
		"depth_sell_quantity_2": record.DepthSellQuantity2,
		"depth_sell_orders_2":   record.DepthSellOrders2,
		"depth_sell_price_3":    record.DepthSellPrice3,
		"depth_sell_quantity_3": record.DepthSellQuantity3,
		"depth_sell_orders_3":   record.DepthSellOrders3,
		"depth_sell_price_4":    record.DepthSellPrice4,
		"depth_sell_quantity_4": record.DepthSellQuantity4,
		"depth_sell_orders_4":   record.DepthSellOrders4,
		"depth_sell_price_5":    record.DepthSellPrice5,
		"depth_sell_quantity_5": record.DepthSellQuantity5,
		"depth_sell_orders_5":   record.DepthSellOrders5,
		// Additional fields
		"last_trade_time":        record.LastTradeTime,
		"net_change":             record.NetChange,
		"tick_received_time":     record.TickReceivedTime,
		"tick_stored_in_db_time": record.TickStoredInDbTime,
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
