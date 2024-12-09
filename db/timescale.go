package db

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TimescaleDB struct {
	pool           *pgxpool.Pool
	config         *config.TimescaleConfig
	batchProcessor *BatchProcessor
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

const (
	batchSize     = 10000 // Doubled batch size
	maxRetries    = 3
	workerCount   = 50                    // More workers
	channelSize   = 50000                 // Larger buffer
	flushTimeout  = 50 * time.Millisecond // Faster flushes
	maxConcurrent = 100                   // Max concurrent transactions
	maxQueueSize  = 1000000               // 1M ticks queue capacity
)

type BatchProcessor struct {
	db          *TimescaleDB
	batchChan   chan *BatchItem
	workerCount int
	semaphore   chan struct{} // Control concurrent transactions
	metrics     *BatchMetrics
}

type BatchMetrics struct {
	processedTicks atomic.Uint64
	droppedTicks   atomic.Uint64
	batchLatency   atomic.Int64
	queueSize      atomic.Int64
}

type BatchItem struct {
	tableName string
	tick      *proto.TickData
}

func NewBatchProcessor(db *TimescaleDB, workerCount int) *BatchProcessor {
	bp := &BatchProcessor{
		db:          db,
		batchChan:   make(chan *BatchItem, channelSize),
		workerCount: workerCount,
		semaphore:   make(chan struct{}, maxConcurrent),
		metrics:     &BatchMetrics{},
	}

	// Start workers
	bp.startWorkers()

	// Start metrics reporter
	go bp.reportMetrics()

	return bp
}

func (bp *BatchProcessor) reportMetrics() {
	ticker := time.NewTicker(5 * time.Second)
	log := logger.GetLogger()

	for range ticker.C {
		log.Info("Batch processor metrics", map[string]interface{}{
			"processed_ticks": bp.metrics.processedTicks.Load(),
			"dropped_ticks":   bp.metrics.droppedTicks.Load(),
			"avg_latency_ms":  time.Duration(bp.metrics.batchLatency.Load()).Milliseconds(),
			"queue_size":      bp.metrics.queueSize.Load(),
		})
	}
}

func (bp *BatchProcessor) startWorkers() {
	for i := 0; i < bp.workerCount; i++ {
		go bp.worker()
	}
}

func (bp *BatchProcessor) worker() {
	batch := make([]*BatchItem, 0, batchSize)
	ticker := time.NewTicker(flushTimeout)
	defer ticker.Stop()

	for {
		select {
		case item := <-bp.batchChan:
			batch = append(batch, item)
			if len(batch) >= batchSize {
				bp.processBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				bp.processBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (bp *BatchProcessor) processBatch(batch []*BatchItem) {
	if len(batch) == 0 {
		return
	}

	// Group by table name for efficient processing
	tableGroups := make(map[string][]*proto.TickData)
	for _, item := range batch {
		tableGroups[item.tableName] = append(tableGroups[item.tableName], item.tick)
	}

	// Process each table group
	for tableName, ticks := range tableGroups {
		bp.processBatchForTable(tableName, ticks)
	}
}

func (bp *BatchProcessor) processBatchForTable(tableName string, ticks []*proto.TickData) {
	ctx := context.Background()
	tx, err := bp.db.pool.Begin(ctx)
	if err != nil {
		logger.GetLogger().Error("Failed to begin transaction", map[string]interface{}{"error": err})
		return
	}
	defer tx.Rollback(ctx)

	for _, tick := range ticks {
		_, err = tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (
				instrument_token, timestamp, is_tradable, is_index, mode,
				last_price, last_trade_time, last_traded_quantity,
				total_buy_quantity, total_sell_quantity, volume_traded,
				total_buy, total_sell, average_trade_price,
				oi, oi_day_high, oi_day_low, net_change,
				open_price, high_price, low_price, close_price
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
			ON CONFLICT (instrument_token, timestamp) DO UPDATE SET
				last_price = EXCLUDED.last_price,
				oi = EXCLUDED.oi`, tableName),
			tick.InstrumentToken,
			time.Unix(tick.Timestamp, 0),
			tick.IsTradable,
			tick.IsIndex,
			tick.Mode,
			tick.LastPrice,
			time.Unix(tick.LastTradeTime, 0),
			tick.LastTradedQuantity,
			tick.TotalBuyQuantity,
			tick.TotalSellQuantity,
			tick.VolumeTraded,
			tick.TotalBuy,
			tick.TotalSell,
			tick.AverageTradePrice,
			tick.Oi,
			tick.OiDayHigh,
			tick.OiDayLow,
			tick.NetChange,
			tick.Ohlc.Open,
			tick.Ohlc.High,
			tick.Ohlc.Low,
			tick.Ohlc.Close,
		)

		if err != nil {
			logger.GetLogger().Error("Failed to insert or update data", map[string]interface{}{"error": err})
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		logger.GetLogger().Error("Failed to commit transaction", map[string]interface{}{"error": err})
		return
	}
}

// StoreTick now uses the batch processor
func (db *TimescaleDB) StoreTick(tableName string, tick *proto.TickData) error {
	if db.batchProcessor == nil {
		db.batchProcessor = NewBatchProcessor(db, workerCount)
	}

	// Send to batch channel
	select {
	case db.batchProcessor.batchChan <- &BatchItem{tableName: tableName, tick: tick}:
		return nil
	default:
		return fmt.Errorf("batch channel full, dropping tick")
	}
}
